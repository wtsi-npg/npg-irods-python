# -*- coding: utf-8 -*-
#
# Copyright © 2023 Genome Research Ltd. All rights reserved.
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

"""PacBio-specific business logic API."""

from dataclasses import dataclass
from typing import Optional

from npg_irods.metadata.pacbio import Instrument
from partisan.irods import AVU, Collection, DataObject
from sqlalchemy.orm import Session
from structlog import get_logger

from npg_irods.exception import DataObjectNotFound
from npg_irods.metadata.common import SeqConcept, SeqSubset

log = get_logger(__package__)


@dataclass(order=True)
class Component:
    run_name: str
    well_label: str
    tag_identifier: str
    plate_number: Optional[int]
    subset: Optional[SeqSubset]

    def __init__(
        self,
        run_name: str,
        well_label: str,
        tag_identifier: str,
        plate_number: int = None,
        subset: str = None,
    ):
        self.run_name = run_name
        self.well_label = well_label
        self.tag_identifier = tag_identifier
        self.plate_number = plate_number
        self.subset = SeqSubset.from_string(subset)

    @staticmethod
    def from_avus(cls, *avus):
        avu_dict = AVU.collate(*avus)

        args = []
        for key in [
            Instrument.RUN_NAME.value,
            Instrument.WELL_LABEL.value,
            Instrument.TAG_IDENTIFIER.value,
        ]:
            if key not in avu_dict:
                raise ValueError(f"Missing required AVU key: {key}")
            args.append(avu_dict[key])

        return Component(
            *args,
            plate_number=avu_dict.get(Instrument.PLATE_NUMBER.value),
            subset=avu_dict.get(SeqConcept.SUBSET.value),
        )


def ensure_secondary_metadata_updated(
    item: Collection | DataObject, mlwh_session: Session
):
    pass


def find_associated_components(item: Collection | DataObject) -> list[Component]:
    errmsg = "Failed to find an associated data object bearing component metadata"

    if item.rods_type == Collection:
        raise DataObjectNotFound(
            f"{errmsg}. Illumina component metadata is only associated with data "
            f"objects, while {item} is a collection"
        )

    # Only PacBio BAM files have any component metadata
    if item.path.suffix != ".bam":
        return []

    return [Component.from_avus(item.metadata())]
