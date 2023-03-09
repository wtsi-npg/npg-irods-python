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
# @author Michael Kubiak <mk35@sanger.ac.uk>

import re
from enum import unique

from npg_id_generation.pac_bio import PacBioEntity
from structlog import get_logger

from npg_irods.metadata.common import parse_object_type, has_target_metadata
from npg_irods.metadata.lims import has_id_product_metadata, SeqConcept
from partisan.irods import AVU, DataObject
from partisan.metadata import AsValueEnum

log = get_logger(__name__)


@unique
class Instrument(AsValueEnum):
    """Pac Bio platform metadata"""
    RUN_NAME = "run"
    WELL_LABEL = "well"
    TAG_SEQUENCE = "tag_sequence"


def remove_well_padding(well_label: str) -> str:
    """
    Remove padding from well label string.

    Args:
        well_label: The padded well label as it is in iRODS metadata

    Returns: The unpadded well label string
    """
    match = re.search(r"^([A-Z])(\d+)$", well_label)
    return match.group(1) + match.group(2).lstrip("0")


def requires_id_product_metadata(obj: DataObject) -> bool:
    """Return true if the object should have these metadata.

    Args:
        obj: The data object to check

    Returns: True if the metadata are required, False if not.

    """
    if parse_object_type(obj) == "bam":  # Currently, the only Pac Bio sequence files in iRODS are bams
        return True
    return False


def ensure_id_product(obj: DataObject, overwrite: bool = False) -> bool:
    """Ensure that a data object has id_product metadata if it should need it.
    Else do nothing.

    Args:
        obj: The data object.
        overwrite: A flag to overwrite metadata that is already present.
                   Defaults to False.

    Returns: bool

    """

    if not overwrite and has_id_product_metadata(obj):
        log.debug(f"{obj} already has id_product metadata")
        return True

    if not requires_id_product_metadata(obj):
        log.debug(f"{obj} does not require id_product metadata")
        return False

    log.debug(f"Making id_product metadata for {obj}")
    metadata = {}
    id_product_old = ""
    for avu in obj.metadata():
        if avu.attribute() == SeqConcept.ID_PRODUCT:
            id_product_old = avu.value()
        elif avu.attribute() in [meta.value for meta in Instrument]:  # until python 3.12 is released, there is no easy way to test against a value being present in an Enum
            metadata[avu.attribute()] = avu.value()

    metadata[Instrument.WELL_LABEL] = \
        remove_well_padding(metadata[Instrument.WELL_LABEL])

    if not has_target_metadata(obj):
        log.debug(f"{obj} is not a target data file, adding well id")
        del metadata[Instrument.TAG_SEQUENCE]

    id_product = PacBioEntity(**metadata).hash_product_id()

    if id_product_old:
        log.debug(f"Removing old id_product {id_product_old} from {obj}")
        obj.remove_metadata(AVU(SeqConcept.ID_PRODUCT, id_product_old))

    log.debug(f"Adding id_product = {id_product} to {obj}")
    obj.add_metadata(AVU(SeqConcept.ID_PRODUCT, id_product))
    return True
