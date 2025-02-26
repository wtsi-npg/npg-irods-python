# -*- coding: utf-8 -*-
#
# Copyright © 2022, 2023, 2024 Genome Research Ltd. All rights reserved.
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

import structlog
from npg.cli import add_io_arguments, add_logging_arguments
from npg.log import configure_structlog

from npg_irods import add_appinfo_structlog_processor, version
from npg_irods.utilities import check_checksums

description = """
Reads iRODS data object paths from a file or STDIN, one per line and performs
consistency checks on their iRODS checksums and checksum metadata.

The conditions for checksums and checksum metadata of a data object to be correct
are:

- The data object must have a checksum set for each valid replica.
- The checksums for all replicas must have the same value.
- The data object must have one, and only one, checksum AVU in its metadata.
- The checksum AVU must have the same value as the replica checksums.

This script will never change any values. To repair checksums, use the --print-fail
option to print failed paths to STDOUT and pipe them to the desired repair script
e.g. `repair-checksums`.

If any of the paths fail their checksum check, the exit code will be non-zero and an 
error message summarising the results will be sent to STDERR.
"""

parser = argparse.ArgumentParser(
    description=description, formatter_class=argparse.RawDescriptionHelpFormatter
)
add_io_arguments(parser)
add_logging_arguments(parser)
parser.add_argument(
    "--print-pass",
    help="Print to output those paths that pass the check.",
    action="store_true",
)
parser.add_argument(
    "--print-fail",
    help="Print to output those paths that fail the check.",
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
    "--version", help="Print the version and exit.", action="version", version=version()
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
log = structlog.get_logger("main")


def main():
    num_processed, num_passed, num_errors = check_checksums(
        args.input,
        args.output,
        print_pass=args.print_pass,
        print_fail=args.print_fail,
        num_clients=args.clients,
        num_threads=args.threads,
    )

    if num_errors:
        log.error(
            "Some checks did not pass",
            num_processed=num_processed,
            num_passed=num_passed,
            num_errors=num_errors,
        )
        sys.exit(1)

    log.info(
        "All checks passed",
        num_processed=num_processed,
        num_passed=num_passed,
        num_errors=num_errors,
    )
