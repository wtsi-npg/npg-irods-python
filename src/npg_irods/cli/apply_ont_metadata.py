# -*- coding: utf-8 -*-
#
# Copyright © 2023, 2024 Genome Research Ltd. All rights reserved.
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
# @author Marco M. Mosca <mm51@sanger.ac.uk>

import argparse
import sys

import sqlalchemy
import structlog
from npg.cli import (
    add_date_range_arguments,
    add_db_config_arguments,
    add_logging_arguments,
)

from npg.conf import IniData
from npg.log import configure_structlog
from sqlalchemy.orm import Session

from npg_irods import db, version
from npg_irods.ont import apply_metadata

description = """
Applies metadata and data access permissions on ONT run collections in iRODS, to reflect
information in the Multi-LIMS warehouse.

This script differs from `update-secondary-metadata` in that it is designed to be run
on newly created ONT data where it detects multiplexed runs and adds extra primary
metadata to results that have been deplexed on the instrument. It also updates
secondary metadata (like `update-secondary-metadata` does), but that is purely as an
optimisation to make the data available without waiting for an
`update-secondary-metadata` to be scheduled. 

Only runs whose ML warehouse records have been updated within the specified date range.
The default window for detecting changes is the 14 days prior to the time when the
script is run. This can be changed using the --begin-date and --end-date CLI options.
"""

parser = argparse.ArgumentParser(
    description=description, formatter_class=argparse.RawDescriptionHelpFormatter
)
add_logging_arguments(parser)
add_date_range_arguments(parser, begin_delta=14)
add_db_config_arguments(parser)
parser.add_argument(
    "--zone",
    help="Specify a federated iRODS zone in which to find "
    "collections to update. This is not required if the target "
    "collections are in the local zone.",
    type=str,
)
parser.add_argument(
    "--version", help="Print the version and exit.", action="version", version=version()
)

args = parser.parse_args()
configure_structlog(
    config_file=args.log_config,
    debug=args.debug,
    verbose=args.verbose,
    colour=args.colour,
    json=args.json,
)
log = structlog.get_logger("main")


def main():
    dbconfig = IniData(db.Config).from_file(args.db_config.name, "mlwh_ro")
    engine = sqlalchemy.create_engine(
        dbconfig.url, pool_pre_ping=True, pool_recycle=3600
    )

    with Session(engine) as session:
        num_processed, num_updated, num_errors = apply_metadata(
            session, since=args.begin_date, zone=args.zone
        )

        if num_errors:
            log.error(
                "Update failed",
                num_processed=num_processed,
                num_updated=num_updated,
                num_errors=num_errors,
            )
            sys.exit(1)

        msg = (
            "All updates were successful" if num_updated else "No updates were required"
        )
        log.info(
            msg,
            num_processed=num_processed,
            num_updated=num_updated,
            num_errors=num_errors,
        )
