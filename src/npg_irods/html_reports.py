# -*- coding: utf-8 -*-
#
# Copyright © 2024, 2025 Genome Research Ltd. All rights reserved.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# @author Keith James <kdj@sanger.ac.uk>

import calendar
import os.path
import tempfile
from collections import defaultdict
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path, PurePath

from partisan.icommands import iquest
from partisan.irods import AC, AVU, Collection, DataObject, Permission, RodsItem
from partisan.metadata import DublinCore
from structlog import get_logger
from yattag import Doc, SimpleDoc, indent

from npg_irods.common import infer_zone
from npg_irods.metadata import ont
from npg_irods.metadata.common import (
    PUBLIC_IRODS_GROUP,
    ensure_common_metadata,
    ensure_sqyrrl_metadata,
)
from npg_irods.ont import is_minknow_report
from npg_irods.utilities import load_resource

log = get_logger(__package__)


class Tags(StrEnum):
    """HTML tags. Use to avoid typos, add as necessary."""

    html = "html"
    head = "head"
    link = "link"
    meta = "meta"
    body = "body"
    style = "style"
    title = "title"

    h1 = "h1"
    h2 = "h2"
    h3 = "h3"
    h4 = "h4"
    h5 = "h5"
    h6 = "h6"

    div = "div"
    span = "span"

    a = "a"
    p = "p"

    code = "code"
    pre = "pre"

    ol = "ol"
    ul = "ul"
    li = "li"

    img = "img"


class Styles(StrEnum):
    """CSS classes. Use to avoid typos, add as necessary."""

    container = "container"

    main_cell = "main-cell"
    top_cell = "top-cell"
    top_left_cell = "top-left-cell"
    top_right_cell = "top-right-cell"

    url_cell = "url-cell"
    url_grid = "url-grid"

    acl_header = "acl-header"
    info_header = "info-header"
    metadata_header = "metadata-header"
    path_header = "path-header"

    acl_bag = "acl-bag"
    acl_cell = "acl-cell"
    acl_item = "acl-item"

    info_bag = "info-bag"
    info_cell = "info-cell"
    info_item = "info-item"

    metadata_bag = "metadata-bag"
    metadata_cell = "metadata-cell"
    metadata_item = "metadata-item"


def ont_runs_this_year(zone: str = None) -> list[tuple[Collection, datetime]]:
    """Query iRODS to find all ONT runs for the current year.

    Returns:
        For each run, a tuple of the annotated run-folder collection and the
        creation timestamp.
    """
    # get the current year as a datetime object
    start_of_year = datetime(datetime.now().year, 1, 1, tzinfo=timezone.utc)

    # One would hope that the following would work, but it doesn't; iRODS seems to
    # ignore the "and" clause on COLL_CREATE_TIME and returns all collections
    # with the specified metadata.
    #
    # For whatever reason, iRODS stores timestamps as varchar left-padded with '0' to
    # a width of 11 characters, so we need to compare lexically, with the argument
    # similarly padded.
    #
    # Testing with hand-crafted iquest commands shows the COLL_CREATE_TIME is ignored.
    #
    # sec_since_epoch = (start_of_year.now(timezone.utc) - datetime(1970, 1, 1)).total_seconds()
    #
    # args = [
    #     "%s %s",
    #     "-z",
    #     "seq",
    #     "select COLL_NAME, COLL_CREATE_TIME "
    #     f"where META_COLL_ATTR_NAME = '{ont.Instrument.EXPERIMENT_NAME}' "
    #     f"and COLL_CREATE_TIME >= '{sec_since_epoch:011.0f}'",
    # ]
    #
    # Instead, we need to get all collections with the specified metadata and filter.
    # The physical capacity of the lab limits this number to low hundreds per year, but
    # we will need to revisit this if the number of collections becomes too large.
    args = ["%s\t%s"]

    if zone is not None:
        args.append("-z")
        args.append(zone)

    query = (
        "select COLL_NAME, COLL_CREATE_TIME "
        f"where META_COLL_ATTR_NAME = '{ont.Instrument.EXPERIMENT_NAME}'"
    )

    log.info("Querying iRODS for ONT runs this year", year=start_of_year.year)

    colls = []
    for n, line in enumerate(iquest(*args, query).splitlines()):
        try:
            path, timestamp = line.split("\t")
            coll = Collection(path)

            created = datetime.fromtimestamp(int(timestamp), tz=timezone.utc)
            if created >= start_of_year:
                colls.append((coll, created))
        except Exception as e:
            log.error("Error processing iquest result line", n=n, line=line, error=e)
            continue

    return colls


def ont_runs_html_report_this_year(
    zone: str = None, all_avu=False, all_ac=False
) -> SimpleDoc:
    """Generate an HTML report of all ONT runs for the current year

    Args:
        zone: The zone to query. Optional, defaults to the current zone.
        all_avu: Report all AVUs, even those that are system-related and not normally
        relevant to data customers.
        all_ac: Report all access control entries, even those that are system-related
        and not normally relevant to data customers.
    Returns:
        A yattag SimpleDoc object containing the HTML report.
    """
    now = datetime.now()
    colls_by_month: defaultdict[int, list[Collection]] = defaultdict(list)
    for coll, created in ont_runs_this_year(zone=zone):
        colls_by_month[created.month].append(coll)

    def report_ac(ac: AC) -> bool:
        """Return True if the AC should be reported."""
        if all_ac:
            return True
        return ac.user not in [
            "irods",
            "irods-g1",
            "ont1",
            "rodsBoot",
            "srpipe",
        ]

    def report_avu(avu: AVU) -> bool:
        """Return True if the AVU should be reported."""
        if all_avu:
            return True
        if avu.namespace == AVU.IRODS_NAMESPACE:
            return False
        if avu.namespace == DublinCore.namespace:
            return False
        if avu.namespace == ont.Instrument.namespace and avu.without_namespace in [
            term.value
            for term in [
                ont.Instrument.DISTRIBUTION_VERSION,
                ont.Instrument.GUPPY_VERSION,
                ont.Instrument.HOSTNAME,
                ont.Instrument.PROTOCOL_GROUP_ID,
            ]
        ]:
            return False
        return True

    def do_info_cell(x: DataObject):
        """Add an info cell (data object size, creation timestamp) to the report."""
        with tag(Tags.div, klass=Styles.info_cell):
            with tag(Tags.div, klass=Styles.info_bag):
                # Use doc.asis to insert non-breaking spaces
                with tag(Tags.div, klass=Styles.info_item):
                    doc.asis(f"{x.created().strftime('%Y-%m-%d&nbsp;%H:%M:%S')}")
                with tag(Tags.div, klass=Styles.info_item):
                    doc.asis(f"{x.size()}&nbsp;B")

    def do_acl_cell(x: RodsItem):
        """Add an ACL cell to the report, if the ACL is not empty."""
        to_report = [ac for ac in x.acl() if report_ac(ac)]
        if len(to_report) == 0:
            return

        with tag(Tags.div, klass=Styles.acl_cell):
            with tag(Tags.div, klass=Styles.acl_bag):
                for ac in to_report:
                    line(Tags.div, str(ac), klass=Styles.acl_item)

    def do_metadata_cell(x: RodsItem):
        """Add a metadata cell to the report, if AVUs are present."""
        to_report = [avu for avu in x.metadata() if report_avu(avu)]
        if len(to_report) == 0:
            return

        with tag(Tags.div, klass=Styles.metadata_cell):
            with tag(Tags.div, klass=Styles.metadata_bag):
                for avu in to_report:
                    with tag(Tags.div, klass=Styles.metadata_item):
                        text(f"{avu.attribute}={avu.value}")

    def do_contents(c: Collection):
        contents = c.contents(acl=True, avu=True)
        if len(contents) == 0:
            return

        for item in contents:
            if is_minknow_report(item):
                with tag(Tags.div, klass=Styles.url_cell):
                    with tag(Tags.a, href=str(item)):
                        text(item.name)
                do_info_cell(item)
                do_acl_cell(item)
                do_metadata_cell(c)

    doc, tag, text, line = Doc().ttl()
    doc.asis("<!DOCTYPE html>")

    stylesheet = load_resource("style.css")

    with tag(Tags.html):
        with tag(Tags.head):
            with tag(Tags.title):
                text(f"ONT runs for {now.year}")

            doc.asis(f"<{Tags.style}>{stylesheet}</{Tags.style}>")

        with tag(Tags.body):
            with tag(Tags.div, klass=Styles.container):
                # Top row cells containing title and report metadata
                with tag(Tags.div, klass=Styles.top_left_cell):
                    text("")
                with tag(Tags.div, klass=Styles.top_right_cell):
                    text(f"Generated: {now.strftime('%Y-%m-%d %H:%M:%S')}")
                with tag(Tags.div, klass=Styles.top_cell):
                    line(Tags.h1, f"ONT Year Report {now.year}")

                # Main cell containing the report content
                with tag(Tags.div, klass=Styles.main_cell):
                    for month in sorted(colls_by_month.keys()):
                        colls = colls_by_month[month]
                        log.debug("Found ONT runs for month", month=month, n=len(colls))

                        with tag(Tags.h2):
                            text(f"{calendar.month_name[month]} {now.year}")

                            with tag(Tags.div, klass=Styles.url_grid):
                                with tag(Tags.div, klass=Styles.url_cell):
                                    line(
                                        Tags.h3,
                                        "iRODS Path",
                                        klass=Styles.path_header,
                                    )
                                with tag(Tags.div, klass=Styles.info_cell):
                                    line(
                                        Tags.h3,
                                        "Created/Size",
                                        klass=Styles.info_header,
                                    )
                                with tag(Tags.div, klass=Styles.acl_cell):
                                    line(
                                        Tags.h3,
                                        "Access Control List",
                                        klass=Styles.acl_header,
                                    )
                                with tag(Tags.div, klass=Styles.metadata_cell):
                                    line(
                                        Tags.h3,
                                        "Metadata",
                                        klass=Styles.metadata_header,
                                    )

                                for coll in colls:
                                    # This would report the collection itself, which will
                                    # become useful when Sqyrrl can navigate the iRODS
                                    # filesystem:
                                    #
                                    # with tag(Tags.div, klass=Styles.url_cell):
                                    #     with tag(Tags.a, href=str(coll)):
                                    #         text(coll.path.as_posix())

                                    # do_metadata_cell(coll)
                                    do_contents(coll)

    return doc


def publish_report(doc: SimpleDoc, path: PurePath, category: str = None) -> DataObject:
    """Publish an HTML report to iRODS, annotated so that Sqyrrl can find it.

    Args:
        doc: The content to publish.
        path: The absolute data object path in iRODS to which the report will be written.
        category: The metadata category to apply to the report for Sqyrrl. Optional,
            defaults to None.
    Returns:
        The published DataObject.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpfile = os.path.join(tmpdir, path.name)
        with open(tmpfile, "w") as f:
            f.write(indent(doc.getvalue(), indent_text=True))

        obj = DataObject(path).put(
            tmpfile, calculate_checksum=True, verify_checksum=True, force=True
        )
        ensure_common_metadata(obj)
        ensure_sqyrrl_metadata(obj, category=category)
        obj.add_permissions(
            AC(user=PUBLIC_IRODS_GROUP, perm=Permission.READ, zone=infer_zone(obj))
        )

        return obj
