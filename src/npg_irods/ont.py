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
    def __init__(self, experiment_name: str = None, instrument_slot: int = None):
        self.experiment_name = experiment_name
        self.instrument_slot = instrument_slot

    def update_secondary_metadata(
        self, mlwh_session: Session, since: datetime = None
    ) -> List[Collection]:
        """Update iRODS secondary metadata on ONT run collections whose corresponding
        ML warehouse records have been updated more recently than the specified time.

        Args:
            mlwh_session: An open SQL session.
            since: A datetime.

        Returns:
            A list of collections whose metadata were updated.
        """
        if since is None:
            since = datetime.fromtimestamp(0)  # Everything since the Epoch
        updated = []

        for expt_name, slot in find_recent_expt_slot(mlwh_session, since=since):
            if self.experiment_name is not None:
                if self.experiment_name != expt_name:
                    log.info(
                        "Skipping on experiment name",
                        experiment_name=expt_name,
                        slot=slot,
                    )
                    continue
                if self.instrument_slot is not None:
                    if self.instrument_slot != slot:
                        log.info(
                            "Skipping on slot",
                            experiment_name=expt_name,
                            slot=slot,
                        )
                        continue

            log.info("Searching for", experiment_name=expt_name, slot=slot)
            colls = query_metadata(
                AVU(
                    Instrument.EXPERIMENT_NAME,
                    expt_name,
                    namespace=Instrument.namespace,
                ),
                AVU(Instrument.INSTRUMENT_SLOT, slot, namespace=Instrument.namespace),
                collection=True,
                data_object=False,
            )
            log.info("Found collections", collections=colls)
            for coll in colls:
                if annotate_results_collection(coll, expt_name, slot, mlwh_session):
                    updated.append(coll)

        return updated


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
    log.debug(
        "Searching the warehouse for plex information",
        experiment=experiment_name,
        slot=instrument_slot,
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
            experiment=experiment_name,
            slot=instrument_slot,
        )
        return False

    coll.add_metadata(*avus)  # These AVUs should be present already

    # There will be either a single fc record (for non-multiplexed data) or
    # multiple (one per plex of multiplexed data)
    for fc in fc_info:
        log.debug(
            "Found experiment/slot/tag index",
            experiment=experiment_name,
            slot=instrument_slot,
            tag_identifier=fc.tag_identifier,
        )

        if fc.tag_identifier:
            # This is the barcode directory naming style created by ONT's
            # Guppy and qcat de-multiplexers. We add information to the
            # barcode sub-collection.
            bc_path = PurePath(path) / barcode_name_from_id(fc.tag_identifier)
            log.debug("Annotating", path=bc_path, tag_identifier=fc.tag_identifier)
            log.debug("Annotating", path=bc_path, sample=fc.sample, study=fc.study)

            bc_coll = Collection(bc_path)
            if not bc_coll.exists():
                log.warn(
                    "The barcoded data collection does not exist",
                    path=bc_path,
                    experiment=experiment_name,
                    slot=instrument_slot,
                    tag_identifier=fc.tag_identifier,
                )
                continue

            bc_coll.add_metadata(
                AVU(SeqConcept.TAG_INDEX, tag_index_from_id(fc.tag_identifier))
            )
            bc_coll.add_metadata(*make_study_metadata(fc.study))
            bc_coll.add_metadata(*make_sample_metadata(fc.sample))

            # The ACL could be different for each plex
            bc_coll.add_permissions(*make_sample_acl(fc.sample, fc.study), recurse=True)
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
