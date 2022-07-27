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
import argparse
import logging
import structlog
from typing import List
from multiprocessing import Pool

parser = argparse.ArgumentParser(
    description="A script to create json files that can be used to backfill the irods location mlwh table"
)

parser.add_argument(
    "colls",
    type=str,
    nargs="+",
    help="A list of collections in which to find products",
)

parser.add_argument(
    "-o",
    "--out",
    type=str,
    default="out.json",
    help="The output filename, defaults to out.json",
)

parser.add_argument(
    "-p",
    "--processes",
    type=int,
    default=1,
    help="The number of baton processes to run",
)

parser.add_argument(
    "-v", "--verbose", action="store_true", help="Enable INFO level logging"
)

args = parser.parse_args()
logging_level = logging.WARN
if args.verbose:
    logging_level = logging.INFO

logging.basicConfig(level=logging_level, encoding="utf-8")

structlog.configure(logger_factory=structlog.stdlib.LoggerFactory())

log = structlog.get_logger(__file__)

from partisan.irods import DataObject, Collection, client_pool

JSON_FILE_VERSION = "1.0"
ILLUMINA = "illumina"
ALT_PROCESS = "npg-prod-alt-process"
NPG_PROD = "npg-prod"


def create_product_dict(obj_path: str, ext: str):
    """
    Gathers information about a data object that is required to load
    it into the seq_product_irods_locations table.
    """
    # rebuild un-pickleable objects inside subprocess
    with client_pool(1) as baton_pool:
        obj = DataObject(obj_path, baton_pool)
        if (
            obj.name.split(".")[-1] == ext
            and "phix" not in obj.name
            and "human" not in obj.name
        ):
            product = {
                "seq_platform_name": ILLUMINA,
                "pipeline_name": NPG_PROD,
                "irods_root_collection": str(obj.path),
                "irods_data_relative_path": str(obj.name),
            }

            for meta in obj.metadata():

                if meta.attribute == "id_product":
                    product["id_product"] = meta.value
                elif meta.attribute == "alt_process":
                    product["pipeline_name"] = ALT_PROCESS

            if "id_product" in product.keys():
                return product
            else:
                raise MissingMetadataError(
                    f"id_product metadata not found for {obj.path}/{obj.name}"
                )


def find_products(coll: Collection) -> List[dict]:
    """
    Recursively finds all (non-human, non-phix) cram data objects in
    a collection.
    Runs a pool of processes to create a list of dictionaries containing
    information to load them into the seq_product_irods_locations table.
    """
    products = []

    with Pool(args.processes - 1) as p:
        cram_results = [
            p.apply_async(create_product_dict, (str(obj.path / obj.name), "cram"))
            for obj in coll.iter_contents()
            if not isinstance(obj, Collection)
        ]
        products = [
            product.get() for product in cram_results if product.get() is not None
        ]
        if not products:
            log.warn(f"No cram files found in {coll.path}, searching for bam files")
            bam_results = [
                p.apply_async(create_product_dict, (str(obj.path / obj.name), "bam"))
                for obj in coll.iter_contents()
                if not isinstance(obj, Collection)
            ]
            products = [
                product.get() for product in bam_results if product.get() is not None
            ]

    return products


def main():

    log.info(
        f"Creating product rows for products in {args.colls} to output into {args.out} this is more test"
    )
    products = []
    with client_pool(1) as baton_pool:
        for coll_path in args.colls:
            coll = Collection(coll_path, baton_pool)
            if coll.exists():
                # find all contained products and get metadata
                coll_products = find_products(coll)
                products.extend(coll_products)
                log.info(f"Found {len(coll_products)} products in {coll.path}")
            else:
                log.warn(f"collection {coll} not found")
    mlwh_json = {"version": JSON_FILE_VERSION, "products": products}
    with open(args.out, "w") as out:
        json.dump(mlwh_json, out)


class MissingMetadataError(Exception):
    """Raise when expected metadata is not present on an object."""

    pass


if __name__ == "__main__":
    main()
