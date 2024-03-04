# -*- coding: utf-8 -*-
#
# Copyright © 2024 Genome Research Ltd. All rights reserved.
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
# @author Zaynab Butt <zb3@sanger.ac.uk>

import argparse
import sys
import structlog
import sqlalchemy

from sqlalchemy.orm import Session
from npg_irods.cli.util import add_logging_arguments, configure_logging
from npg_irods.db import DBConfig
from npg_irods.utilities import general_metadata_update
from npg_irods.version import version

description = """
Takes a StudyID and iRods collection or data object and applies the study metadata from MLWH to it.
"""

parser = argparse.ArgumentParser(
    description=description, formatter_class=argparse.RawDescriptionHelpFormatter
)
add_logging_arguments(parser)

parser.add_argument(
    "-i",
    "--input",
    help="Input filename.",
    type=argparse.FileType("r"),
    default=sys.stdin,
)
parser.add_argument(
    "-o",
    "--output",
    help="Output filename.",
    type=argparse.FileType("w"),
    default=sys.stdout,
)
parser.add_argument(
    "--database-config",
    "--database_config",
    "--db-config",
    "--db_config",
    help="Configuration file for database connection.",
    type=argparse.FileType("r"),
    required=True,
)
parser.add_argument(
    "--zone",
    help="Specify a federated iRODS zone in which to find data objects and/or "
    "collections to update. This is not required if the target paths "
    "are on the local zone.",
    type=str,
)
parser.add_argument(
    "--print-pass",
    help="Print to output those paths that pass the check. Defaults to True.",
    action="store_true",
)
parser.add_argument(
    "--print-fail",
    help="Print to output those paths that require repair, where the repair failed. "
    "Defaults to False.",
    action="store_true",
)
parser.add_argument(
    "--version", help="Print the version and exit.", action="store_true"
)

args = parser.parse_args()
configure_logging(
    config_file=args.log_config,
    debug=args.debug,
    verbose=args.verbose,
    colour=args.colour,
    json=args.json,
)
log = structlog.get_logger("main")


def main():
    if args.version:
        print(version())
        exit(0)

    dbconfig = DBConfig.from_file(args.database_config.name, "mlwh_ro")

    engine = sqlalchemy.create_engine(dbconfig.url)
    with Session(engine) as session:
        num_processed, num_updated, num_errors = general_metadata_update(
            args.input,
            args.output,
            session,
            print_update=args.print_update,
            print_fail=args.print_fail,
        )

    if num_errors:
        log.error(
            "Some checks did not pass",
            num_processed=num_processed,
            num_passed=num_updated,
            num_errors=num_errors,
        )
        exit(1)

    log.info(
        "All checks passed",
        num_processed=num_processed,
        num_passed=num_updated,
        num_errors=num_errors,
    )
