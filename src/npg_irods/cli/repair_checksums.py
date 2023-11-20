# -*- coding: utf-8 -*-
#
# Copyright © 2022, 2023 Genome Research Ltd. All rights reserved.
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

from npg_irods.utilities import repair_checksums
from npg_irods.cli.util import add_logging_arguments, configure_logging
from npg_irods.version import version

description = """
Reads iRODS data object paths from a file or STDIN, one per line and repairs
their checksums and checksum metadata, if necessary.

The possible repairs are:

 - Data object checksums: valid replicas that have no checksum are updated to have
   their current checksum according to their state on disk, using the iRODS API.

 - Data object metadata: if all valid replicas have the same checksum and there is
   no checksum metadata AVU, then one is added.

 - Data object metadata: if all valid replicas have the same checksum and current
   checksum metadata are incorrect, a new AVU is added and any previous metadata
   moved to history.

The following states are not repaired automatically because they require an
assessment on which, if any, replicas are correct:

 - The checksums across all valid replicas are not identical.

If any of the paths could not be repaired, the exit code will be non-zero and an
error message summarising the results will be sent to STDERR.
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
    "--print-repair",
    help="Print to output those paths that were repaired. Defaults to True.",
    action="store_true",
)
parser.add_argument(
    "--print-fail",
    help="Print to output those paths that require repair, where the repair failed. "
    "Defaults to False.",
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

    num_processed, num_repaired, num_errors = repair_checksums(
        args.input,
        args.output,
        num_threads=args.threads,
        num_clients=args.clients,
        print_repair=args.print_repair,
        print_fail=args.print_fail,
    )

    if num_errors:
        log.error(
            "Some repairs failed",
            num_processed=num_processed,
            num_repaired=num_repaired,
            num_errors=num_errors,
        )
        exit(1)

    msg = "All repairs were successful" if num_repaired else "No paths required repair"
    log.info(
        msg,
        num_processed=num_processed,
        num_repaired=num_repaired,
        num_errors=num_errors,
    )
