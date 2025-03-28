# -*- coding: utf-8 -*-
#
# Copyright © 2021, 2022, 2023, 2024 Genome Research Ltd. All rights reserved.
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

"""ONT-specific business logic API."""

import re
import shlex
from dataclasses import dataclass
from datetime import datetime
from os import PathLike
from pathlib import PurePath
from typing import Iterator, Optional, Type

from partisan.exception import RodsError
from partisan.icommands import iquest
from partisan.irods import AVU, Collection, DataObject, query_metadata
from sqlalchemy import asc, distinct
from sqlalchemy.orm import Session
from structlog import get_logger

from npg_irods.common import infer_zone, update_metadata, update_permissions
from npg_irods.db.mlwh import OseqFlowcell, SQL_CHUNK_SIZE, Sample, Study
from npg_irods.metadata.common import SeqConcept
from npg_irods.metadata.lims import (
    ensure_consent_withdrawn,
    has_consent_withdrawn_metadata,
    make_public_read_acl,
    make_sample_acl,
    make_sample_metadata,
    make_study_metadata,
)
from npg_irods.metadata.ont import Instrument

log = get_logger(__package__)

# We are using the tag identifiers defined by ONT in their barcode arrangement files,
# which you can find distributed with MinKNOW and Guppy.
TAG_IDENTIFIER_GROUP = "tag_id"
TAG_IDENTIFIER_REGEX = re.compile(r"(?P<tag_id>\d+)$")

# Directories ignored when searching the run folder for directories containing deplexed
# data. Examples of sibling directories that are not ignored: fast5_fail, fast5_pass
IGNORED_DIRECTORIES = ["other_reports"]


@dataclass(order=True)
class Component:
    """A set of reads from an ONT sequencing run."""

    experiment_name: str
    """The experiment name recorded in MinKNOW."""
    instrument_slot: int
    """The 1-based instrument slot index where the flowcell was run."""

    tag_identifier: Optional[str]
    """The tag identifier, if the reads are from a multiplexed pool."""

    def __init__(
        self,
        experiment_name: str,
        instrument_slot: int,
        tag_identifier: Optional[str] = None,
    ):
        self.experiment_name = experiment_name
        self.instrument_slot = instrument_slot
        self.tag_identifier = tag_identifier


def apply_metadata(
    mlwh_session: Session,
    experiment_name=None,
    instrument_slot=None,
    since: datetime = None,
    until: datetime = None,
    zone=None,
) -> (int, int, int):
    """Apply iRODS metadata on ONT run collections whose corresponding ML warehouse
    records have been updated within a specified time range. This function detects
    runs that are multiplexed and adds relevant tag identifier and tag index primary
    metadata to the deplexed collections.

    Collections to annotate are identified by having ont:experiment_name and
    ont:instrument_slot metadata already attached to them. This is done for example,
    by the process which moves sequence data from the instrument into iRODS.

    Args:
        mlwh_session: An open SQL session.
        experiment_name: Limit updates to this experiment. Optional.
        instrument_slot: Limit updates to this instrument slot. Optional, requires
          an experiment_name to be supplied.
        since: A datetime. Limit updates to experiments changed at this time or later.
        until: A datetime. Limit updates to experiments before at this time or earlier.
        zone: The iRODS zone to search for metadata to update.

    Returns:
        A tuple of the number of paths found, the number of paths whose metadata
    were changed and the number of errors encountered.
    """
    if since is None:
        since = datetime.fromtimestamp(0)  # Everything since the Epoch
    if until is None:
        until = datetime.now()

    if experiment_name is None and instrument_slot is not None:
        raise ValueError(
            f"An instrument_slot {instrument_slot} was supplied "
            "without an experiment_name"
        )

    num_found, num_updated, num_errors = 0, 0, 0

    for i, c in enumerate(
        find_updated_components(
            mlwh_session, include_tags=False, since=since, until=until
        )
    ):
        if experiment_name is not None and c.experiment_name != experiment_name:
            continue
        if instrument_slot is not None and c.instrument_slot != instrument_slot:
            continue

        avus = [
            AVU(Instrument.EXPERIMENT_NAME, c.experiment_name),
            AVU(Instrument.INSTRUMENT_SLOT, c.instrument_slot),
        ]

        try:
            log.info("Searching", item=i, comp=c, zone=zone)
            colls = query_metadata(*avus, data_object=False, zone=zone)

            num_colls = len(colls)
            num_found += num_colls
            if num_colls:
                log.info("Found collections", item=i, comp=c, num_coll=num_colls)
            else:
                log.warn("Found no collections", item=i, comp=c)

            for coll in colls:
                try:
                    if annotate_results_collection(coll, c, mlwh_session):
                        log.info("Updated", item=i, path=coll, comp=c)
                        num_updated += 1
                    else:
                        num_errors += 1
                except RodsError as re1:
                    log.error(re1.message, item=i, code=re1.code)
                    num_errors += 1

        except RodsError as re2:
            log.error(re2.message, item=i, code=re2.code)
            num_errors += 1
        except Exception as e:
            log.error(e)
            num_errors += 1

    return num_found, num_updated, num_errors


def annotate_results_collection(
    path: PathLike | str, component: Component, mlwh_session: Session
) -> bool:
    """Add or update metadata on an existing iRODS collection containing ONT data.

    There are two main types of run; single-sample and multiplexed-sample. In the
    former, the sequence data files lie directly in the fast5_* / pod5_* / fastq_* etc.
    collections. In the latter, each of the fast5_* / pod5_* / fastq_* etc. collections
    contain further collections named barcode01, barcode02 etc. and it is within these
    that the sequence data files lie.

    This function handles both types of run by detecting the presence of the barcode
    collections. For single-sample runs metadata are added to the runfolder collection
    i.e. to the collection given by the `path` argument. In multiplexed-sample runs,
    metadata are added to each of the barcode01, barcode02 etc. collections.

    The metadata added are fetched from the ML warehouse and include information on the
    sample and the associated study, including data access permissions. This function
    also sets the appropriate permissions in iRODS.

    This function is idempotent. No harm will come from running it on an already
    up-to-date collection.

    Args:
        path: A collection path to annotate.
        component: A Component describing the portion of an instrument
        mlwh_session: An open SQL session.

    Returns:
        True on success.
    """
    c = component
    log.debug("Searching the ML warehouse", comp=c)

    flowcells = find_flowcells_by_component(mlwh_session, c)
    if not flowcells:
        log.warn("Failed to find flowcell information in the ML warehouse", comp=c)
        return False

    coll = Collection(path)
    if not coll.exists():
        log.warn("Collection does not exist", path=coll, comp=c)
        return False

    # A single fc record (for non-multiplexed data)
    if len(flowcells) == 1:
        log.info("Found non-multiplexed", comp=c)
        try:
            # Secondary metadata. Updating this here is an optimisation to reduce
            # turn-around-time. If we don't update, we just have to wait for a
            # cron job to call `ensure_secondary_metadata_updated`.
            _do_secondary_metadata_and_perms_update(coll, flowcells)
        except RodsError as e:
            log.error(e.message, code=e.code)
            return False

        return True

    log.info("Found multiplexed", comp=c)
    num_errors = 0

    # Since the run report files are outside the 'root' collection for
    # multiplexed runs, their permissions must be set explicitly
    num_errors += _set_public_read_perms(coll)

    # This expects the barcode directory naming style created by current ONT's
    # Guppy de-multiplexer which creates several subdirectories e.g. "fast5_pass",
    # "fast_fail". Each of these subdirectories contains another directory for each
    # barcode, plus miscellaneous directories such as "mixed" and "unclassified".

    for fc in flowcells:
        try:
            bcomp = Component(c.experiment_name, c.instrument_slot, fc.tag_identifier)

            for bcoll in barcode_collections(coll, fc.tag_identifier):
                log.info(
                    "Annotating",
                    path=bcoll,
                    comp=bcomp,
                    sample=fc.sample,
                    study=fc.study,
                )

                # Primary metadata. We are adding the full tag identifier to enable ML
                # warehouse lookup given just an iRODS path as a starting point. The tag
                # identifier to tag index transformation loses information (the tag
                # prefix), so the existing tag index AVU doesn't allow this.
                bcoll.supersede_metadata(
                    AVU(Instrument.TAG_IDENTIFIER, fc.tag_identifier),
                    AVU(SeqConcept.TAG_INDEX, tag_index_from_id(fc.tag_identifier)),
                    history=True,
                )

                # Secondary metadata. Updating this here is an optimisation to reduce
                # turn-around-time. If we don't update, we just have to wait for a
                # cron job to call `ensure_secondary_metadata_updated`.
                _do_secondary_metadata_and_perms_update(bcoll, [fc])

        except RodsError as e:
            log.error(e.message, code=e.code)
            num_errors += 1

    return num_errors == 0


def ensure_secondary_metadata_updated(
    item: Collection | DataObject, mlwh_session: Session
) -> bool:
    """Update secondary metadata on an iRODS path, using information from the ML
    warehouse.

    Args:
        item: iRODS path to update.
        mlwh_session: An open SQL session.

    Returns:
        True if any changes were made.
    """
    expt = item.avu(Instrument.EXPERIMENT_NAME).value
    slot = item.avu(Instrument.INSTRUMENT_SLOT).value
    tag_id = (
        item.avu(Instrument.TAG_IDENTIFIER).value
        if item.has_metadata_attrs(Instrument.TAG_IDENTIFIER)
        else None
    )

    component = Component(expt, slot, tag_id)

    return annotate_results_collection(item, component, mlwh_session=mlwh_session)


def requires_managed_access(obj: DataObject) -> bool:
    """Return True if the given data object requires managed access control.

    Data objects containing primary sequence or genotype data should be managed.
    For ONT that means fast5, pod5, fastq, bed and bam files. Sequencing summary
    files may contain sequence data, so these are also managed.

    The main cases for unmanaged access are report files, final summary files and sample
    sheets.
    """
    p = PurePath(obj)

    suffixes = [suffix.casefold() for suffix in p.suffixes]
    managed = [".bam", ".bed", ".fast5", ".fastq", ".pod5"]

    if any(suffix in managed for suffix in suffixes):
        return True

    name = p.name.casefold()
    if (
        name.startswith("report_")
        or name.startswith("final_summary_")
        or name.startswith("sample_sheet_")
    ):
        return False

    return True


def find_recent_expt(sess: Session, since: datetime) -> list[str]:
    """Find recent ONT experiments in the ML warehouse database.

    Find ONT experiments in the ML warehouse database that have been updated
    at, or since a specified date and time. If any element of the experiment (any of
    the positions in a multi-flowcell experiment, any of the multiplexed
    elements within a position) have been updated in the query window, the
    experiment name will be returned.

    Args:
        sess: An open session to the ML warehouse.
        since: A datetime.

    Returns:
        List of matching experiment name strings
    """

    rows = (
        sess.query(distinct(OseqFlowcell.experiment_name))
        .filter(OseqFlowcell.recorded_at >= since)
        .all()
    )

    return [val for val, in rows]


def find_updated_components(
    sess: Session, since: datetime, until: datetime, include_tags=True
) -> Iterator[Component]:
    """Return the components of runs whose ML warehouse metadata has been updated
    at or since the given date and time.

    Args:
        sess: An open SQL session.
        since: A datetime.
        until: A datetime.
        include_tags: Resolve the components to the granularity of individual tags,
          rather than as whole runs. Optional, defaults to True.

    Returns:
        An iterator over the matching components.
    """
    columns = [OseqFlowcell.experiment_name, OseqFlowcell.instrument_slot]

    if include_tags:
        columns.append(OseqFlowcell.tag_identifier)

    query = (
        sess.query(*columns)
        .distinct()
        .join(OseqFlowcell.sample)
        .join(OseqFlowcell.study)
        .filter(
            Sample.recorded_at.between(since, until)
            | Study.recorded_at.between(since, until)
            | OseqFlowcell.recorded_at.between(since, until)
        )
        .group_by(*columns)
    )

    order = [asc(OseqFlowcell.experiment_name), asc(OseqFlowcell.instrument_slot)]
    if include_tags:
        order.append(asc(OseqFlowcell.tag_identifier))
    query = query.order_by(*order)

    for cols in query.yield_per(SQL_CHUNK_SIZE):
        yield Component(*cols)


def find_flowcells_by_component(
    sess: Session, component: Component
) -> list[Type[OseqFlowcell]]:
    """Return the flowcells for this component.

    Args:
        sess: An open SQL session.
        component: An ONT run component.

    Returns:
        The OseqFlowcells for the component.
    """
    query = (
        sess.query(OseqFlowcell)
        .distinct()
        .filter(OseqFlowcell.experiment_name == component.experiment_name)
    )

    if component.instrument_slot is not None:
        query = query.filter(OseqFlowcell.instrument_slot == component.instrument_slot)

    if component.tag_identifier is not None:
        query = query.filter(OseqFlowcell.tag_identifier.like())

    return query.order_by(
        asc(OseqFlowcell.experiment_name),
        asc(OseqFlowcell.instrument_slot),
        asc(OseqFlowcell.tag_identifier),
        asc(OseqFlowcell.tag2_identifier),
    ).all()


def find_run_collections(
    since: datetime, until: datetime, zone: str = None
) -> list[Collection]:
    """Find ONT run collections in iRODS by their metadata and creation time.

    Args:
        since: The earliest creation date of the collections to find.
        until: The latest creation date of the collections to find.
        zone: The federated iRODS zone in which to find collections. Optional,
            defaults to "seq". Use None to search the local zone.

    Returns:
        Paths of collections created between these times.
    """

    args = []
    if zone is not None:
        args.extend(["-z", shlex.quote(zone)])
    args.append("%s")

    query = (
        "select COLL_NAME where "
        f"META_COLL_ATTR_NAME = '{Instrument.EXPERIMENT_NAME}' and "
        f"META_COLL_ATTR_NAME = '{Instrument.INSTRUMENT_SLOT}' and "
        f"COLL_CREATE_TIME >= '{int(since.timestamp()) :>011}' and "
        f"COLL_CREATE_TIME <= '{int(until.timestamp()) :>011}'"
    )

    # iquest mixes logging and data in its output
    ignore1 = f"Zone is {zone}" if zone is not None else "Zone is"
    ignore2 = "CAT_NO_ROWS_FOUND"

    paths = []
    for line in iquest(*args, query).splitlines():
        p = line.strip()
        if p.startswith(ignore1) or p.startswith(ignore2):
            continue
        paths.append(p)

    return [Collection(p) for p in paths]


def tag_index_from_id(tag_identifier: str) -> int:
    """Return the barcode tag index given a barcode tag identifier."""
    match = TAG_IDENTIFIER_REGEX.search(tag_identifier)
    if match:
        return int(match.group(TAG_IDENTIFIER_GROUP))

    raise ValueError(
        f"Invalid ONT tag identifier '{tag_identifier}'. "
        f"Expected a value matching {TAG_IDENTIFIER_REGEX}"
    )


def barcode_name_from_id(tag_identifier: str) -> str:
    """Return the barcode name given a barcode tag identifier.

    The name is used most often for directory naming in ONT experiment results."""
    match = TAG_IDENTIFIER_REGEX.search(tag_identifier)
    if match:
        return f"barcode{match.group(TAG_IDENTIFIER_GROUP) :0>2}"

    raise ValueError(
        f"Invalid ONT tag identifier '{tag_identifier}'. "
        f"Expected a value matching {TAG_IDENTIFIER_REGEX}"
    )


def barcode_collections(coll: Collection, *tag_identifier) -> list[Collection]:
    """Return the barcode-specific sub-collections that exist under the specified
    collection basecalled and deplexed on instrument or offline.

    The arrangement of these collections mirrors the directory structure created by the
    guppy/dorado basecaller. E.g. for tag identifier NB01:

        <coll>/fast5_pass/barcode01
        ...
        <coll>/fast5_fail/barcode01
        ...
        <coll>/fastq_pass/barcode01
        ...
        <coll>/fastq_fail/barcode01

    ...or for rebasecalled runs:

        <coll>/pass/barcode01
        ...
        <coll>/pass/barcode02
        ...

    ...or for old rebasecalled runs (up until June 2024):

        <coll>/barcode01
        ...
        <coll>/barcode02
        ...

    If collection paths contain duplicated barcode folder names,
    it will raise a ValueError.
        E.g. <coll>/pass/barcode01/.../barcode01

    Args:
        coll: A collection to search.
        *tag_identifier: One or more tag identifiers. Only barcode collections for
           these identifiers will be returned.

    Returns:
        A sorted list of existing collections.

    Raises:
        ValueError: Duplicated barcode folder names are found in a path
    """
    bcolls = []
    barcode_folders = [barcode_name_from_id(tag_id) for tag_id in tag_identifier]
    sub_colls = [
        item
        for item in coll.contents(recurse=True)
        if item.rods_type == Collection and item.path.name in barcode_folders
    ]

    parents = set()
    for sc in sub_colls:
        duplicated = re.findall(r"(barcode\d+)", str(sc.path))
        if len(duplicated) > 1:
            msg = (
                f"Incorrect barcode folder path {sc.path}. "
                f"Contains multiple barcode folders {duplicated}"
            )
            log.error(msg)
            raise ValueError(msg)
        parents.add(str(sc.path.parent))

    for parent in parents:
        for tag_id in tag_identifier:
            bpath = PurePath(parent, barcode_name_from_id(tag_id))
            bcoll = Collection(bpath)
            if bcoll.exists():
                bcolls.append(bcoll)
            else:
                # LIMS says there is a tag identifier, but there is no sub-collection,
                # so possibly this was not deplexed on-instrument for some reason e.g.
                # a non-standard tag set was used
                log.warn("No barcode sub-collection", path=bcoll, tag_identifier=tag_id)
    bcolls.sort()

    return bcolls


def is_minknow_report(obj: DataObject, suffix=None) -> bool:
    """Return True if the data object is a MinKNOW run report.

    Args:
        obj: iRODS path to check.
        suffix: Optional suffix to check. If supplied, will only return True if the
            object has this suffix. If omitted, will return True if the object is any
            MinKNOW report.

    Returns:
        True if the object is a MinKNOW report.
    """
    if suffix is not None:
        valid_suffixes = [".html", ".json", ".md"]
        suffix = suffix.casefold()

        if suffix not in valid_suffixes:
            raise ValueError(
                f"Invalid suffix '{suffix}'; expected one of {valid_suffixes}"
            )

    if obj.rods_type != DataObject or not obj.name.casefold().startswith("report"):
        return False

    if suffix is None:
        return True

    return suffix in PurePath(obj.name).suffixes


def _do_secondary_metadata_and_perms_update(
    item: Collection | DataObject, flowcells
) -> bool:
    """Update metadata and permissions using sample/study information obtained from
    flowcell records in the ML warehouse.

    Args:
        item: iRODS path to update.
        flowcells: ML warehouse flowcell records.

    Returns:
        True if changes were made.
    """
    zone = infer_zone(item)

    if managed_access := requires_managed_access(item):
        log.debug("Requires managed access", path=item)
    else:
        log.debug("Does not require managed access", path=item)

    metadata = []
    for fc in flowcells:
        metadata.extend(make_sample_metadata(fc.sample))
        metadata.extend(make_study_metadata(fc.study))
    meta_update = update_metadata(item, metadata)

    acl = []
    for fc in flowcells:
        if managed_access:
            acl.extend(make_sample_acl(fc.sample, fc.study, zone=zone))
        else:
            acl.extend(make_public_read_acl(zone=zone))

    recurse = item.rods_type == Collection
    cons_update = perm_update = False

    if has_consent_withdrawn_metadata(item):
        log.info("Consent withdrawn", path=item)
        cons_update = ensure_consent_withdrawn(item, recurse=recurse)
    else:
        perm_update = update_permissions(item, acl, recurse=recurse)

    return any([meta_update, cons_update, perm_update])


def _set_public_read_perms(coll: Collection) -> int:
    """Set the permissions of any unmanaged files in the collection to public:read.

    Args:
        coll: A collection containing report data objects.

    Returns:
        The number of errors encountered.
    """
    num_errors = 0

    for obj in coll.contents():
        if not requires_managed_access(obj):
            try:
                if obj.exists():
                    log.info("Updating permissions", path=obj)
                    update_permissions(obj, make_public_read_acl())
                else:
                    log.warn("File missing", path=obj)
            except RodsError as e:
                log.error(e.message, code=e.code)
                num_errors += 1
    return num_errors
