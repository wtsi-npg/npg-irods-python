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
from multiprocessing.pool import ThreadPool
from threading import Lock
from typing import List

from npg_id_generation.pac_bio import PacBioEntity
from structlog import get_logger

from npg_irods.metadata.common import parse_object_type, has_target_metadata
from npg_irods.metadata.lims import has_id_product_metadata, SeqConcept
from npg_irods.mlwh_locations.pacbio import write_mlwh_json
from partisan.irods import (
    AVU,
    Collection,
    DataObject,
    RodsError,
    client_pool,
    rods_path_type,
)
from partisan.metadata import AsValueEnum

log = get_logger(__name__)
lock = Lock()
products = {}


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
    if (
        parse_object_type(obj) == "bam"
    ):  # Currently, the only Pac Bio sequence files in iRODS are bams
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

    global products

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
        if avu.attribute == SeqConcept.ID_PRODUCT.value:
            id_product_old = avu.value
        elif avu.attribute in [
            meta.value for meta in Instrument
        ]:  # until python 3.12 is released, there is no easy way to test against a value being present in an Enum
            metadata[avu.attribute] = avu.value

    id_args = {
        "run_name": metadata[Instrument.RUN_NAME.value],
        "well_label": remove_well_padding(metadata[Instrument.WELL_LABEL.value]),
    }

    if has_target_metadata(obj):
        try:
            id_args["tags"] = metadata[Instrument.TAG_SEQUENCE.value]
            log.debug(
                f"{obj} is a target data file, adding tag sequence, if "
                "present, to product id generation"
            )
        except KeyError:
            pass  # key error means that no tag metadata is present,
            # so the well level id_product will be used

    id_product = PacBioEntity(**id_args).hash_product_id()

    if id_product_old:
        log.debug(f"Removing old id_product {id_product_old} from {obj}")
        obj.remove_metadata(AVU(SeqConcept.ID_PRODUCT.value, id_product_old))

    log.debug(f"Adding id_product = {id_product} to {obj}")
    obj.add_metadata(AVU(SeqConcept.ID_PRODUCT.value, id_product))

    if has_target_metadata(obj):
        lock.acquire()
        products[obj.path] = id_product
        lock.release()

    return True


def backfill_id_products(
    paths: List[str],
    out_path: str,
    overwrite: bool = False,
    num_threads: int = 1,
    num_clients: int = 1,
) -> bool:
    """Read iRODS paths from reader, and recursively add id_product metadata
    where it is required.

    Args:
        paths: The paths of iRODS item paths in which objects should have
               id_product metadata added.
        out_path: The path to the file for loading the mlwh locations table.
        overwrite: A flag to overwrite metadata that is already present.
                   Defaults to False.
        num_threads: The number of Python threads to use. Defaults to 1.
        num_clients: The number of baton clients to use. Defaults to 1.

    Returns: bool

    """
    rv = False
    with client_pool(num_clients) as bp, ThreadPool(num_threads) as tp:
        results = []
        for path in paths:
            if rods_path_type(path) == Collection:
                objs = Collection(path, pool=bp).iter_contents(recurse=True)
            else:
                objs = [DataObject(path, pool=bp)]

            results.extend(
                [
                    tp.apply_async(ensure_id_product, (obj, overwrite))
                    for obj in objs
                    if isinstance(obj, DataObject)
                ]
            )

        for result in results:
            try:
                result.get()
                rv = True
            except RodsError as e:
                log.error(e.message, code=e.code)

    write_mlwh_json(products, out_path)
    return rv
