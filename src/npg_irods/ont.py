# -*- coding: utf-8 -*-
#
# Copyright © 2021, 2022, 2023 Genome Research Ltd. All rights reserved.
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
from dataclasses import dataclass
from datetime import datetime
from os import PathLike
from typing import Iterator, Optional, Type

from partisan.exception import RodsError
from partisan.irods import AVU, Collection, DataObject, query_metadata
from sqlalchemy import asc, distinct
from sqlalchemy.orm import Session
from structlog import get_logger

from npg_irods.common import update_metadata, update_permissions, infer_zone
from npg_irods.db.mlwh import OseqFlowcell, Sample, Study
from npg_irods.metadata.common import SeqConcept
from npg_irods.metadata.lims import (
    ensure_consent_withdrawn,
    has_consent_withdrawn_metadata,
    is_managed_access,
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
        self, experiment_name: str, instrument_slot: int, tag_identifier: str = None
    ):
        self.experiment_name = experiment_name
        self.instrument_slot = instrument_slot
        self.tag_identifier = tag_identifier


def apply_metadata(
    mlwh_session: Session,
    experiment_name=None,
    instrument_slot=None,
    since: datetime = None,
    zone=None,
) -> (int, int, int):
    """Apply iRODS metadata on ONT run collections whose corresponding ML warehouse
    records have been updated at, or more recently than, the specified time.

    Collections to annotate are identified by having ont:experiment_name and
    ont:instrument_slot metadata already attached to them. This is done for example,
    by the process which moves sequence data from the instrument into iRODS.

    Args:
        mlwh_session: An open SQL session.
        experiment_name: Limit updates to this experiment. Optional.
        instrument_slot: Limit updates to this instrument slot. Optional, requires
          an experiment_name to be supplied.
        since: A datetime. Limit updates to experiments changed at this time or later.
        zone: The iRODS zone to search for metadata to update.

    Returns:
        A tuple of the number of paths found, the number of paths whose metadata
    were changed and the number of errors encountered.
    """
    if since is None:
        since = datetime.fromtimestamp(0)  # Everything since the Epoch

    if experiment_name is None and instrument_slot is not None:
        raise ValueError(
            f"An instrument_slot {instrument_slot} was supplied "
            "without an experiment_name"
        )

    num_found, num_updated, num_errors = 0, 0, 0

    for i, c in enumerate(
        find_components_changed(mlwh_session, include_tags=False, since=since)
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
    num_errors += _set_minknow_reports_public(coll)

    sub_colls = [item for item in coll.contents() if item.rods_type == Collection]

    # This expects the barcode directory naming style created by current ONT's
    # Guppy de-multiplexer which creates several subdirectories e.g. "fast5_pass",
    # "fast_fail". Each of these subdirectories contains another directory for each
    # barcode, plus miscellaneous directories such as "mixed" and "unclassified".

    for sc in sub_colls:  # fast5_fail, fast5_pass etc
        # These are some known special cases that don't have barcode directories
        if sc.path.name in IGNORED_DIRECTORIES:
            log.debug("Ignoring", path=sc)
            continue

        # Multiple fc records (one per plex of multiplexed data)
        for fc in flowcells:
            try:
                bpath = sc.path / barcode_name_from_id(fc.tag_identifier)
                bcoll = Collection(bpath)
                bcomp = Component(
                    c.experiment_name, c.instrument_slot, fc.tag_identifier
                )
                if not bcoll.exists():
                    log.warn("Collection missing", path=bpath, comp=bcomp)
                    continue

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
    item: Collection | DataObject, mlwh_session
) -> bool:
    """Update secondary metadata on an iRODS path, using information from the ML
    warehouse.

    Args:
        item: iRODS path to update.
        mlwh_session: An open SQL session.

    Returns:
        True if any changes were made.
    """
    expt = item.avu(Instrument.EXPERIMENT_NAME.value)
    slot = item.avu(Instrument.INSTRUMENT_SLOT.value)
    tag_id = item.avu(Instrument.TAG_IDENTIFIER.value)

    component = Component(expt.value, slot.value, tag_id.value)
    flowcells = find_flowcells_by_component(mlwh_session, component)

    return _do_secondary_metadata_and_perms_update(item, flowcells)


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


def find_components_changed(
    sess: Session, since: datetime, include_tags=True
) -> Iterator[Component]:
    """Return the components of runs whose ML warehouse metadata has been updated
    at or since the given date and time.

    Args:
        sess: An open SQL session.
        since: A datetime.
        include_tags: Resolve the components to the granularity of individual tags,
          rather than as whole runs. Optional, defaults to True.

    Returns:
        An iterator over the matching components.
    """
    columns = [OseqFlowcell.experiment_name, OseqFlowcell.instrument_slot]

    if include_tags:
        columns.append(OseqFlowcell.tag_identifier)

    for cols in (
        sess.query(*columns)
        .distinct()
        .join(OseqFlowcell.sample)
        .join(OseqFlowcell.study)
        .filter(
            (Sample.recorded_at >= since)
            | (Study.recorded_at >= since)
            | (OseqFlowcell.recorded_at >= since)
        )
        .group_by(OseqFlowcell.experiment_name, OseqFlowcell.instrument_slot)
        .order_by(asc(OseqFlowcell.experiment_name), asc(OseqFlowcell.instrument_slot))
    ):
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


def tag_index_from_id(tag_identifier: str) -> int:
    """Return the barcode tag index given a barcode tag identifier.

    Returns: int
    """
    match = TAG_IDENTIFIER_REGEX.search(tag_identifier)
    if match:
        return int(match.group(TAG_IDENTIFIER_GROUP))

    raise ValueError(
        f"Invalid ONT tag identifier '{tag_identifier}'. "
        f"Expected a value matching {TAG_IDENTIFIER_REGEX}"
    )


def barcode_name_from_id(tag_identifier: str) -> str:
    """Return the barcode name given a barcode tag identifier. The name is used most
    often for directory naming in ONT experiment results.

    Returns: str
    """
    match = TAG_IDENTIFIER_REGEX.search(tag_identifier)
    if match:
        return f"barcode{match.group(TAG_IDENTIFIER_GROUP) :0>2}"

    raise ValueError(
        f"Invalid ONT tag identifier '{tag_identifier}'. "
        f"Expected a value matching {TAG_IDENTIFIER_REGEX}"
    )


def is_minknow_report(obj: DataObject) -> bool:
    """Return True if the data object is a MinKNOW run report.

    Args:
        obj: iRODS path to check.

    Returns:
        True if the object is a MinKNOW report.
    """
    return obj.rods_type == DataObject and "report" in obj.name


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

    metadata = []
    for fc in flowcells:
        metadata.extend(make_sample_metadata(fc.sample))
        metadata.extend(make_study_metadata(fc.study))
    meta_update = update_metadata(item, metadata)

    acl = []
    for fc in flowcells:
        acl.extend(make_sample_acl(fc.sample, fc.study, zone=zone))

    recurse = item.rods_type == Collection
    cons_update = perm_update = False

    if has_consent_withdrawn_metadata(item):
        log.info("Consent withdrawn", path=item)
        cons_update = ensure_consent_withdrawn(item, recurse=recurse)
    else:
        perm_update = update_permissions(item, acl, recurse=recurse)

    return any([meta_update, cons_update, perm_update])


def _set_minknow_reports_public(coll: Collection) -> int:
    """Set the permissions of any MinKNOW reports directly in the collection to
    public:read.

    Args:
        coll: A collection containing report data objects.

    Returns:
        The number of errors encountered.
    """
    num_errors = 0

    reports = [obj for obj in coll.contents() if is_minknow_report(obj)]

    for report in reports:
        try:
            if report.exists():
                log.info("Updating run report permissions", path=report.path)
                keep = [ac for ac in report.permissions() if not is_managed_access(ac)]
                report.supersede_permissions(*keep, *make_public_read_acl())
            else:
                log.warn("Run report missing", path=report.path)
        except RodsError as e:
            log.error(e.message, code=e.code)
            num_errors += 1
    return num_errors
