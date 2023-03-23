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
import json
from pathlib import PurePath

from structlog import get_logger

from partisan.irods import DataObject

log = get_logger(__name__)
JSON_FILE_VERSION = "1.0"
PACBIO = "pacbio"
NPG_PROD = "npg-prod"


class LocationWriter:
    """Stores information used to load the ml warehouse
    seq_product_irods_locations table, and provides methods to write that
    information to a json file.
    """

    def __init__(
        self, platform: str, pipeline: str = NPG_PROD, path: str = "mlwh.json"
    ):
        self.platform = platform
        self.pipeline = pipeline
        self.path = path
        self.products = {}

    def add_product(self, obj: DataObject, id_product: str):
        self.products[str(obj)] = id_product

    def write(self) -> bool:
        """Write the ml warehouse locations file.

        Returns: bool

        """

        if not self.products:
            return False
        json_out = {"version": JSON_FILE_VERSION, "products": []}
        for obj in self.products.keys():
            obj_path = PurePath(obj)
            coll_path = str(obj_path.parent) + "/"
            id_product = self.products[obj]
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
                        "id_product": self.products[obj],
                        "pipeline_name": NPG_PROD,
                        "platform_name": PACBIO,
                        "irods_root_collection": coll_path,
                        "irods_data_relative_path": str(obj_path.name),
                    }
                )
        with open(self.path, "w") as out:
            json.dump(json_out, out)

        return True
