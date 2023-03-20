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

import re
from datetime import datetime
from os import PathLike
from pathlib import PurePath
from typing import List, Tuple, Union

from partisan.exception import RodsError
from partisan.irods import AVU, Collection, query_metadata
from sqlalchemy import asc, distinct
from sqlalchemy.orm import Session
from structlog import get_logger


from npg_irods.metadata.ont import Instrument
from npg_irods.metadata.lims import (
    SeqConcept,
    make_sample_acl,
    make_sample_metadata,
    make_study_metadata,
)

from ml_warehouse.schema import OseqFlowcell

log = get_logger(__package__)

# We are using the tag identifiers defined by ONT in their barcode arrangement files,
# which you can find distributed with MinKNOW and Guppy.
TAG_IDENTIFIER_GROUP = "tag_id"
TAG_IDENTIFIER_REGEX = re.compile(r"(?P<tag_id>\d+)$")


class MetadataUpdate(object):
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

        num_found = num_updated = num_errors = 0
        expt_slots = []

        try:
            expt_slots = find_recent_expt_slot(mlwh_session, since=since)
        except Exception as e:
            num_errors += 1
            log.error(e)

        for expt_name, slot in expt_slots:
            expt_avu = AVU(
                Instrument.EXPERIMENT_NAME, expt_name, namespace=Instrument.namespace
            )
            slot_avu = AVU(
                Instrument.INSTRUMENT_SLOT, slot, namespace=Instrument.namespace
            )

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
    path: Union[str, PathLike],
    experiment_name: str,
    instrument_slot: int,
    mlwh_session: Session,
) -> bool:
    """Add or update metadata on an existing iRODS collection containing ONT data.

    The metadata added are fetched from the ML warehouse and include information on the
    sample and the associated study, including data access permissions. This function
    also sets the appropriate permissions in iRODS.

    This function is idempotent. No harm will come from running it an an already
    up-to-date collection.

    Args:
        path: A collection path to annotate.
        experiment_name: The ONT experiment name.
        instrument_slot: The ONT instrument slot number.
        mlwh_session:

    Returns:
        True on success.
    """
    log.debug(
        "Searching the ML warehouse", expt_name=experiment_name, slot=instrument_slot
    )
    fc_info = find_flowcell_info(mlwh_session, experiment_name, instrument_slot)

    avus = [
        avu.with_namespace(Instrument.namespace)
        for avu in [
            AVU(Instrument.EXPERIMENT_NAME, experiment_name),
            AVU(Instrument.INSTRUMENT_SLOT, instrument_slot),
        ]
    ]

    coll = Collection(path)
    if not coll.exists():
        log.warn(
            "The data collection does not exist",
            expt_name=experiment_name,
            slot=instrument_slot,
        )
        return False

    coll.add_metadata(*avus)  # These AVUs should be present already

    # There will be either a single fc record (for non-multiplexed data) or
    # multiple (one per plex of multiplexed data)
    for fc in fc_info:
        log.debug(
            "Found experiment/slot/tag index",
            expt_name=experiment_name,
            slot=instrument_slot,
            tag_id=fc.tag_identifier,
        )

        if fc.tag_identifier:
            # This is the barcode directory naming style created by current ONT's
            # Guppy de-multiplexer. We add information to the barcode sub-collection.
            # Guppy creates several subfolders e.g. "fast5_pass", "fast_fail", which we
            # need to traverse.

            subcolls = [
                c for c in Collection(path).contents() if c.rods_type == Collection
            ]

            num_errors = 0
            for sc in subcolls:
                try:
                    bc_path = sc.path / barcode_name_from_id(fc.tag_identifier)
                    log.debug("Annotating", path=bc_path, tag_id=fc.tag_identifier)
                    log.debug(
                        "Annotating", path=bc_path, sample=fc.sample, study=fc.study
                    )

                    bc_coll = Collection(bc_path)
                    if not bc_coll.exists():
                        log.warn(
                            "The barcoded data collection does not exist",
                            path=bc_path,
                            expt_name=experiment_name,
                            slot=instrument_slot,
                            tag_id=fc.tag_identifier,
                        )
                        continue

                    bc_coll.add_metadata(
                        AVU(SeqConcept.TAG_INDEX, tag_index_from_id(fc.tag_identifier))
                    )
                    bc_coll.add_metadata(*make_study_metadata(fc.study))
                    bc_coll.add_metadata(*make_sample_metadata(fc.sample))

                    # The ACL could be different for each plex
                    bc_coll.add_permissions(
                        *make_sample_acl(fc.sample, fc.study), recurse=True
                    )
                except RodsError as e:
                    log.error(e.message, code=e.code)
                    num_errors += 1

            if num_errors:
                return False
        else:
            # There is no tag index, meaning that this is not a
            # multiplexed run, so we add information to the containing
            # collection.
            coll.add_metadata(*make_study_metadata(fc.study))
            coll.add_metadata(*make_sample_metadata(fc.sample))

            coll.add_permissions(*make_sample_acl(fc.sample, fc.study), recurse=True)

    return True


def find_recent_expt(session: Session, since: datetime) -> List[str]:
    """Find recent ONT experiments in the ML warehouse database.

    Find ONT experiments in the ML warehouse database that have been updated
    since a specified date and time. If any element of the experiment (any of
    the positions in a multi-flowcell experiment, any of the multiplexed
    elements within a position) have been updated in the query window, the
    experiment name will be returned.

    Args:
        session: An open SQL session.
        since: A datetime.

    Returns:
        List of matching experiment name strings
    """

    result = (
        session.query(distinct(OseqFlowcell.experiment_name))
        .filter(OseqFlowcell.last_updated >= since)
        .all()
    )

    # The default behaviour of SQLAlchemy is that the result here is a list
    # of tuples, each of which must be unpacked. The official way to do
    # that for all cases is to extend sqlalchemy.orm.query.Query to do the
    # unpacking. However, that's too fancy for MVP, so we just unpack
    # manually.
    return [value for value, in result]


def find_recent_expt_slot(session: Session, since: datetime) -> List[Tuple]:
    """Find recent ONT experiments and instrument slot positions in the ML
    warehouse database.

    Find ONT experiments and associated instrument slot positions in the ML
    warehouse database that have been updated since a specified date and time.

    Args:
        session: An open SQL session.
        since: A datetime.

    Returns:
        List of matching (experiment name, slot position) tuples
    """
    return (
        session.query(OseqFlowcell.experiment_name, OseqFlowcell.instrument_slot)
        .filter(OseqFlowcell.last_updated >= since)
        .group_by(OseqFlowcell.experiment_name, OseqFlowcell.instrument_slot)
        .order_by(asc(OseqFlowcell.experiment_name), asc(OseqFlowcell.instrument_slot))
        .all()
    )


def find_flowcell_info(
    session: Session, experiment_name: str, instrument_slot: int
) -> List[OseqFlowcell]:
    flowcells = (
        session.query(OseqFlowcell)
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

    return flowcells
