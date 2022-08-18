#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright © 2022 Genome Research Ltd. All rights reserved.
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

import json

import structlog
from typing import List, Dict
from multiprocessing import Pool, pool
from partisan.irods import DataObject, Collection, client_pool, AVU
from npg_irods.metadata.lims import SeqConcept

log = structlog.get_logger(__file__)

JSON_FILE_VERSION = "1.0"
ILLUMINA = "illumina"
NPG_PROD = "npg-prod"


def has_expected_extension(name: str, ext: str) -> bool:
    """
    Returns True if an object name has the expected extension.

    Args:
         name: Filename to check
         ext: Expected extension

    Returns: bool
    """
    return name.split(".")[-1] == ext


def is_10x(path: str) -> bool:
    """
    Returns True if an object path contains "ranger", i.e. the object belongs
    to a 10x collection.

    Args:
         path: Object path to check

    Returns: bool
    """
    return "ranger" in path


def has_zero_tag_index(obj: DataObject) -> bool:
    """
    Returns True if a data object's metadata shows that it contains
    multiplex tag "0" assigned reads, i.e. reads that could not be assigned
    a real tag during de-multiplexing.

    Args:
        obj: The data object to check

    Returns: bool
    """
    return AVU(SeqConcept.TAG_INDEX, "0") in obj.metadata()


def has_phix_reference(obj: DataObject) -> bool:
    """
    Returns True if a data object's metadata shows that it uses PhiX as a
    reference, i.e. that it is most likely to be a control.

    Args:
        obj: The data object to check

    Returns: bool
    """
    for meta in obj.metadata():
        if meta.attribute == SeqConcept.REFERENCE and "PhiX" in meta.value:
            return True

    return False


def has_subset(obj: DataObject) -> bool:
    """
    Returns True if a data object's metadata shows that it is part of a subset,
    this is usually "phix" or "human" but can have other values, and should
    always be excluded.

    Args:
        obj: The data object to check

    Returns: bool
    """
    for meta in obj.metadata():
        # subset is not present alone, but is part of the component metadata
        if meta.attribute == SeqConcept.COMPONENT and "subset" in meta.value:
            return True

    return False


def create_product_dict(obj_path: str, ext: str) -> Dict:
    """
    Gathers information about a data object that is required to load
    it into the seq_product_irods_locations table.

    Args:
        obj_path: iRODS path to the data object
        ext: Expected extension of data objects that should be in
             seq_product_irods_locations - either "cram" or "bam" for
             illumina data

    Returns: Dict
    """
    # rebuild un-pickleable objects inside subprocess
    with client_pool(1) as baton_pool:
        obj = DataObject(obj_path, baton_pool)
        if has_expected_extension(obj.name, ext) and not is_10x(str(obj.path)):
            product = {
                "seq_platform_name": ILLUMINA,
                "pipeline_name": NPG_PROD,
                "irods_root_collection": str(obj.path),
                "irods_data_relative_path": str(obj.name),
            }

            # Check for unwanted files
            if has_zero_tag_index(obj) or has_phix_reference(obj) or has_subset(obj):
                raise ExcludedObjectException(f"{obj} is in an excluded object class")

            for meta in obj.metadata():
                if meta.attribute == SeqConcept.ID_PRODUCT:
                    product["id_product"] = meta.value
                if meta.attribute == SeqConcept.ALT_PROCESS:
                    product["pipeline_name"] = f"alt_{meta.value}"

            if "id_product" in product.keys():
                return product
            else:
                # The error is only raised when the ApplyResult object
                # has its .get method run, so can be handled (logged)
                # in the main process
                raise MissingMetadataError(f"id_product metadata not found for {obj}")
        else:
            raise ExcludedObjectException(f"{obj} is in an excluded class")


def extract_products(
    results: List[pool.ApplyResult], timeout: int = None
) -> List[Dict]:
    """
    Extracts products from result list and handles errors raised.

    Args:
        results: A list of ApplyResult objects created by running
                 create_product_dict in subprocesses
        timeout: Timeout for ApplyResult.get()

    Returns: List[Dict]
    """
    products = []
    for result in results:
        try:
            product = result.get(timeout=timeout)
            if product is not None:
                products.append(product)
        except MissingMetadataError as error:
            log.warn(error)
        except ExcludedObjectException as error:
            log.debug(error)
    return products


def find_products(coll: Collection, processes: int) -> List[Dict]:
    """
    Recursively finds all (non-human, non-phix) cram data objects in
    a collection.
    Runs a pool of processes to create a list of dictionaries containing
    information to load them into the seq_product_irods_locations table.

    Args:
        coll: Collection to find required data objects inside
        processes: Number of subprocesses to run - also sets number of baton
                   processes

    Returns: List[Dict]
    """

    with Pool(processes) as p:
        cram_results = [
            p.apply_async(create_product_dict, (str(obj), "cram"))
            for obj in coll.iter_contents()
            if isinstance(obj, DataObject)
        ]
        products = extract_products(cram_results)

        if not products:
            log.warn(f"No cram files found in {coll}, searching for bam files")
            bam_results = [
                p.apply_async(create_product_dict, (str(obj), "bam"))
                for obj in coll.iter_contents()
                if not isinstance(obj, Collection)
            ]
            products = extract_products(bam_results)

    return products


def generate_files(colls: List[str], processes: int, out_file: str):
    """
    Writes a json file containing information to be loaded into
    seq_product_irods_locations table for each product data object in a set of
    collections.

    Args:
        colls: List of collection paths to find product data objects inside
        processes: Number of subprocesses/ baton processes to spawn
        out_file: File name to write the json file to

    Return: None
    """

    log.info(
        f"Creating product rows for products in {colls} to output into {out_file} this is more test"
    )
    products = []
    with client_pool(1) as baton_pool:
        for coll_path in colls:
            coll = Collection(coll_path, baton_pool)
            if coll.exists():
                # find all contained products and get metadata
                coll_products = find_products(coll, processes)
                products.extend(coll_products)
                log.info(f"Found {len(coll_products)} products in {coll}")
            else:
                log.warn(f"collection {coll} not found")
    mlwh_json = {"version": JSON_FILE_VERSION, "products": products}
    with open(out_file, "w") as out:
        json.dump(mlwh_json, out)


class MissingMetadataError(Exception):
    """Raise when expected metadata is not present on an object."""

    pass


class ExcludedObjectException(Exception):
    """
    Raise when an object is one of the excluded set:

    - Has tag 0
    - Reference is PhiX (mostly controls)
    - Is a subset (such as 'phix' or 'human')

    """

    pass
