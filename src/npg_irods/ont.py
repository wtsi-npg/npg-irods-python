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
from typing import Optional, Type

from partisan.exception import RodsError
from partisan.irods import AVU, Collection, query_metadata
from sqlalchemy import asc, distinct
from sqlalchemy.orm import Session
from structlog import get_logger

from npg_irods.db.mlwh import OseqFlowcell
from npg_irods.metadata.common import SeqConcept, SeqSubset
from npg_irods.metadata.lims import (
    is_managed_access,
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
    experiment_name: str
    position: int
    tag_index: Optional[int]
    subset: Optional[SeqSubset]


class MetadataUpdate:
    """Performs updated on metadata of data objects and collections for ONT data in
    iRODS."""

    def __init__(
        self, experiment_name: str = None, instrument_slot: int = None, zone: str = None
    ):
        """Create a new metadata updater for the specified ONT run.

        Args:
            experiment_name: The ONT experiment name. Optional; provide this to limit
                the updates to only that experiment.
            instrument_slot: The ONT instrument slot number. Optional; provide this to
                limit the updates to only that slot.
            zone: The iRODS zone where the data are located. Optional; provide this to
                update on an iRODS zone other than local (i.e. on a federated zone)
        """

        self.experiment_name = experiment_name
        self.instrument_slot = instrument_slot
        self.zone = zone

    def update_secondary_metadata(
        self, mlwh_session: Session, since: datetime = None
    ) -> (int, int, int):
        """Update iRODS secondary metadata on ONT run collections whose corresponding
        ML warehouse records have been updated more recently than the specified time.

        Collections to update are identified by having ont:experiment_name and
        ont:instrument_slot metadata already attached to them. This is done for example,
        by the process which moves sequence data from the instrument into iRODS.

        Args:
            mlwh_session: An open SQL session.
            since: A datetime.

        Returns:
            A tuple of the number of paths found, the number of paths whose metadata
        was updated and the number of errors encountered.
        """
        if since is None:
            since = datetime.fromtimestamp(0)  # Everything since the Epoch

        num_found, num_updated, num_errors = 0, 0, 0

        try:
            expt_slots = find_recent_expt_slot(mlwh_session, since=since)
        except Exception as e:
            num_errors += 1
            log.error(e)
            return num_found, num_updated, num_errors

        for expt_name, slot in expt_slots:
            expt_avu = AVU(Instrument.EXPERIMENT_NAME, expt_name)
            slot_avu = AVU(Instrument.INSTRUMENT_SLOT, slot)

            try:
                if self.experiment_name is not None:
                    if self.experiment_name != expt_name:
                        log.info(
                            "Skipping on experiment name",
                            expt_name=expt_name,
                            slot=slot,
                        )
                        continue
                    if self.instrument_slot is not None:
                        if self.instrument_slot != slot:
                            log.info(
                                "Skipping on slot",
                                expt_name=expt_name,
                                slot=slot,
                            )
                            continue

                log.info("Searching", expt_name=expt_name, slot=slot, zone=self.zone)
                colls = query_metadata(
                    expt_avu,
                    slot_avu,
                    collection=True,
                    data_object=False,
                    zone=self.zone,
                )

                num_colls = len(colls)
                num_found += num_colls
                if num_colls:
                    log.info(
                        "Found collections",
                        expt_name=expt_name,
                        slot=slot,
                        num_coll=num_colls,
                    )
                else:
                    log.warn("Found no collections", expt_name=expt_name, slot=slot)

                for coll in colls:
                    try:
                        if annotate_results_collection(
                            coll, expt_name, slot, mlwh_session
                        ):
                            log.info(
                                "Updated", expt_name=expt_name, slot=slot, path=coll
                            )
                            num_updated += 1
                        else:
                            num_errors += 1
                    except RodsError as re1:
                        log.error(re1.message, code=re1.code)
                        num_errors += 1

            except RodsError as re2:
                log.error(re2.message, code=re2.code)
                num_errors += 1
            except Exception as e:
                log.error(e)
                num_errors += 1

        return num_found, num_updated, num_errors

    def __str__(self):
        return (
            f"<MetadataUpdate expt_name={self.experiment_name} "
            f"slot={self.instrument_slot}>"
        )


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


def annotate_results_collection(
    path: PathLike | str,
    experiment_name: str,
    instrument_slot: int,
    mlwh_session: Session,
) -> bool:
    """Add or update metadata on an existing iRODS collection containing ONT data.

    The metadata added are fetched from the ML warehouse and include information on the
    sample and the associated study, including data access permissions. This function
    also sets the appropriate permissions in iRODS.

    This function is idempotent. No harm will come from running it on an already
    up-to-date collection.

    Args:
        path: A collection path to annotate.
        experiment_name: The ONT experiment name.
        instrument_slot: The ONT instrument slot number.
        mlwh_session: An open SQL session.

    Returns:
        True on success.
    """
    log.debug(
        "Searching the ML warehouse", expt_name=experiment_name, slot=instrument_slot
    )

    fc_info = find_flowcell_by_expt_slot(mlwh_session, experiment_name, instrument_slot)
    if not fc_info:
        log.warn(
            "Failed to find flowcell information in the ML warehouse",
            expt_name=experiment_name,
            slot=instrument_slot,
        )
        return False

    coll = Collection(path)
    if not coll.exists():
        log.warn(
            "Collection does not exist",
            path=coll,
            expt_name=experiment_name,
            slot=instrument_slot,
        )
        return False

    avus = [
        AVU(Instrument.EXPERIMENT_NAME, experiment_name),
        AVU(Instrument.INSTRUMENT_SLOT, instrument_slot),
    ]
    coll.supersede_metadata(*avus)  # These AVUs should be present already

    # A single fc record (for non-multiplexed data)
    if len(fc_info) == 1:
        log.info(
            "Found non-multiplexed", expt_name=experiment_name, slot=instrument_slot
        )
        fc = fc_info[0]
        try:
            coll.supersede_metadata(*make_study_metadata(fc.study), history=True)
            coll.supersede_metadata(*make_sample_metadata(fc.sample), history=True)

            # Keep the access controls that we don't manage
            keep = [ac for ac in coll.permissions() if not is_managed_access(ac)]
            coll.supersede_permissions(
                *keep, *make_sample_acl(fc.sample, fc.study), recurse=True
            )
        except RodsError as e:
            log.error(e.message, code=e.code)
            return False

        return True

    log.info("Found multiplexed", expt_name=experiment_name, slot=instrument_slot)
    sub_colls = [c for c in coll.contents() if c.rods_type == Collection]

    # This expects the barcode directory naming style created by current ONT's
    # Guppy de-multiplexer which creates several subdirectories e.g. "fast5_pass",
    # "fast_fail". Each of these subdirectories contains another directory for each
    # barcode, plus miscellaneous directories such as "mixed" and "unclassified".

    num_errors = 0
    for sc in sub_colls:  # fast5_fail, fast5_pass etc
        # These are some known special cases that don't have barcode directories
        if sc.path.name in IGNORED_DIRECTORIES:
            log.debug("Ignoring", path=sc)
            continue

        # Multiple fc records (one per plex of multiplexed data)
        for fc in fc_info:
            try:
                bc_path = sc.path / barcode_name_from_id(fc.tag_identifier)
                bc_coll = Collection(bc_path)
                if not bc_coll.exists():
                    log.warn(
                        "Collection missing",
                        path=bc_path,
                        expt_name=experiment_name,
                        slot=instrument_slot,
                        tag_id=fc.tag_identifier,
                    )
                    continue

                log.info(
                    "Annotating",
                    path=bc_coll,
                    expt_name=experiment_name,
                    slot=instrument_slot,
                    tag_id=fc.tag_identifier,
                    sample=fc.sample,
                    study=fc.study,
                )

                bc_coll.supersede_metadata(
                    AVU(SeqConcept.TAG_INDEX, tag_index_from_id(fc.tag_identifier)),
                    history=True,
                )
                bc_coll.supersede_metadata(*make_study_metadata(fc.study), history=True)
                bc_coll.supersede_metadata(
                    *make_sample_metadata(fc.sample), history=True
                )

                # The ACL could be different for each plex
                # Keep the access controls that we don't manage
                keep = [ac for ac in bc_coll.permissions() if not is_managed_access(ac)]
                bc_coll.supersede_permissions(
                    *keep, *make_sample_acl(fc.sample, fc.study), recurse=True
                )
            except RodsError as e:
                log.error(e.message, code=e.code)
                num_errors += 1

    return num_errors == 0


def find_recent_expt(sess: Session, since: datetime) -> list[str]:
    """Find recent ONT experiments in the ML warehouse database.

    Find ONT experiments in the ML warehouse database that have been updated
    since a specified date and time. If any element of the experiment (any of
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
        .filter(OseqFlowcell.last_updated >= since)
        .all()
    )

    return [val for val, in rows]


def find_recent_expt_slot(sess: Session, since: datetime) -> list[tuple]:
    """Find recent ONT experiments and instrument slot positions in the ML
    warehouse database.

    Find ONT experiments and associated instrument slot positions in the ML
    warehouse database that have been updated since a specified date and time.

    Args:
        sess: An open session to the ML warehouse.
        since: A datetime.

    Returns:
        List of matching (experiment name, slot position) tuples
    """
    rows = (
        sess.query(OseqFlowcell.experiment_name, OseqFlowcell.instrument_slot)
        .filter(OseqFlowcell.last_updated >= since)
        .group_by(OseqFlowcell.experiment_name, OseqFlowcell.instrument_slot)
        .order_by(asc(OseqFlowcell.experiment_name), asc(OseqFlowcell.instrument_slot))
        .all()
    )
    return [row.tuple() for row in rows]


def find_flowcell_by_expt_slot(
    sess: Session, experiment_name: str, instrument_slot: int
) -> list[Type[OseqFlowcell]]:
    return (
        sess.query(OseqFlowcell)
        .filter(
            OseqFlowcell.experiment_name == experiment_name,
            OseqFlowcell.instrument_slot == instrument_slot,
        )
        .order_by(
            asc(OseqFlowcell.experiment_name),
            asc(OseqFlowcell.instrument_slot),
            asc(OseqFlowcell.tag_identifier),
            asc(OseqFlowcell.tag2_identifier),
        )
        .all()
    )


def find_flowcells_by_component(sess: Session, component: Component):
    return (
        sess.query(OseqFlowcell)
        .filter(
            OseqFlowcell.experiment_name == component.experiment_name,
            OseqFlowcell.instrument_slot == component.position,
        )
        .order_by(
            asc(OseqFlowcell.experiment_name),
            asc(OseqFlowcell.instrument_slot),
            asc(OseqFlowcell.tag_identifier),
            asc(OseqFlowcell.tag2_identifier),
        )
        .all()
    )
