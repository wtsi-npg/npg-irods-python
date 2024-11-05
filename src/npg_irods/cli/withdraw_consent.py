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

import structlog
from npg.cli import add_logging_arguments
from npg.log import configure_structlog

from npg_irods import add_appinfo_structlog_processor, version
from npg_irods.utilities import (
    withdraw_consent,
)

description = """
Reads iRODS data object paths from a file or STDIN, one per line and ensures that each
one is in a state consistent with sample consent being withdrawn.

The conditions for data objects to be in the correct state for having had consent
withdrawn are: 

 - The data object has the correct metadata. Either:
     - sample_consent = 0 (data managed by the GAPI codebase)
     - sample_consent_withdrawn = 1 (data managed by the NPG codebase)

 - Read permission for any SequenceScape study iRODS groups (named ss_<Study ID>)
   absent.

If any of the paths could not have consent withdrawn, the exit code will be non-zero
and an error message summarising the results will be sent to STDERR.
"""

parser = argparse.ArgumentParser(
    description=description, formatter_class=argparse.RawDescriptionHelpFormatter
)
add_logging_arguments(parser)
parser.add_argument(
    "-i",
    "--input",
    help="Input filename.",
    type=argparse.FileType("r", encoding="UTF-8"),
    default=sys.stdin,
)
parser.add_argument(
    "-o",
    "--output",
    help="Output filename.",
    type=argparse.FileType("w", encoding="UTF-8"),
    default=sys.stdout,
)
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
add_appinfo_structlog_processor()
log = structlog.get_logger("main")


def main():
    num_processed, num_withdrawn, num_errors = withdraw_consent(
        args.input,
        args.output,
        print_withdrawn=args.print_pass,
        print_fail=args.print_fail,
    )

    if num_errors:
        log.error(
            "Some updates were not successful",
            num_processed=num_processed,
            num_withdrawn=num_withdrawn,
            num_errors=num_errors,
        )
        sys.exit(1)

    msg = "All updates were successful" if num_withdrawn else "No updates were required"
    log.info(
        msg,
        num_processed=num_processed,
        num_withdrawn=num_withdrawn,
        num_errors=num_errors,
    )
