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
# @author Keith James <kdj@sanger.ac.uk>

import argparse
import sys

import sqlalchemy
import structlog
from npg.cli import add_db_config_arguments, add_io_arguments, add_logging_arguments
from npg.conf import IniData
from npg.log import configure_structlog

from npg_irods import add_appinfo_structlog_processor, db, version
from npg_irods.utilities import update_secondary_metadata

description = """
Reads iRODS data object and/or collection paths from a file or STDIN, one per line and
updates any standard sample and/or study metadata and access permissions to reflect
the current state in the ML warehouse.

To generate a list of paths to be updated, see `locate-data-objects` in this package.

Currently this script supports:

 - Illumina sequencing data objects for data that have not been through the
   library-merge process.

 - PacBio sequencing data objects.

 - Oxford Nanopore sequencing data collections.
 
 - Other platforms whose data objects have study_id and sample_id metadata. 

If any of the paths could not be updated, the exit code will be non-zero and an
error message summarising the results will be sent to STDERR.
"""


log = structlog.get_logger("main")


def main():
    parser = argparse.ArgumentParser(
        description=description, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    add_logging_arguments(parser)
    add_io_arguments(parser)
    add_db_config_arguments(parser)
    parser.add_argument(
        "--print-update",
        help="Print to output those paths that were updated.",
        action="store_true",
    )
    parser.add_argument(
        "--print-fail",
        help="Print to output those paths that require updating, where the update "
        "failed. Defaults to False.",
        action="store_true",
    )
    parser.add_argument(
        "-c",
        "--clients",
        help="Number of baton clients to use. Defaults to 4.",
        type=int,
        default=4,
    )
    parser.add_argument(
        "-t",
        "--threads",
        help="Number of threads to use. Defaults to 4.",
        type=int,
        default=4,
    )
    parser.add_argument(
        "--version",
        help="Print the version and exit.",
        action="version",
        version=version(),
    )
    parser.add_argument(
        "--zone",
        help="Specify a federated iRODS zone in which to find data objects and/or "
        "collections to update. This is not required if the target paths "
        "are on the local zone.",
        type=str,
    )

    args = parser.parse_args()
    configure_structlog(
        config_file=args.log_config,
        debug=args.debug,
        verbose=args.verbose,
        colour=args.colour,
        json=args.log_json,
    )
    add_appinfo_structlog_processor()

    dbconfig = IniData(db.Config).from_file(args.db_config.name, "mlwh_ro")
    engine = sqlalchemy.create_engine(
        dbconfig.url, pool_pre_ping=True, pool_recycle=3600
    )

    num_processed, num_updated, num_errors = update_secondary_metadata(
        args.input,
        args.output,
        engine,
        print_update=args.print_update,
        print_fail=args.print_fail,
    )

    if num_errors:
        log.error(
            "Update failed",
            num_processed=num_processed,
            num_updated=num_updated,
            num_errors=num_errors,
        )
        sys.exit(1)

    msg = "All updates were successful" if num_updated else "No updates were required"
    log.info(
        msg,
        num_processed=num_processed,
        num_updated=num_updated,
        num_errors=num_errors,
    )
