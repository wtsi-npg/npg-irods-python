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
from pathlib import PurePath

from structlog import get_logger

from typing import Dict
import json

log = get_logger(__name__)
JSON_FILE_VERSION = "1.0"
PACBIO = "pacbio"
NPG_PROD = "npg-prod"


def write_mlwh_json(rows: Dict, path: str) -> bool:
    """Write the ml warehouse locations file for the provided set of iRODS
    objects

    Args:
        rows: A dictionary with iRODS paths as keys and product ids as
                  values
        path: The path to which the locations file should be written

    Returns: bool

    """

    if not rows:
        return False
    json_out = {"version": JSON_FILE_VERSION, "products": []}
    for obj in rows.keys():
        obj_path = PurePath(obj)
        coll_path = str(obj_path.parent) + "/"
        id_product = rows[obj]
        new = True
        for product in json_out["products"]:
            if (
                product["irods_root_collection"] == coll_path
                and product["id_product"] == id_product
            ):
                log.warn(
                    f"Second target object found for irods_root_collection "
                    f"{obj_path.parent} and id_product {id_product}.  Adding "
                    f"{obj_path.name} as secondary data file"
                )
                product["irods_secondary_data_relative_path"] = str(obj_path.name)
                new = False
                break
        if new:
            json_out["products"].append(
                {
                    "id_product": rows[obj],
                    "pipeline_name": NPG_PROD,
                    "platform_name": PACBIO,
                    "irods_root_collection": coll_path,
                    "irods_data_relative_path": str(obj_path.name),
                }
            )
    with open(path, "w") as out:
        json.dump(json_out, out)

    return True
