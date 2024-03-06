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

from npg_irods.utilities import (
    check_consent_withdrawn,
)
from npg_irods.cli.util import add_logging_arguments, configure_logging
from npg_irods.version import version

description = """
Reads iRODS data object paths from a file or STDIN, one per line and checks that each
one is in a state consistent with sample consent being withdrawn.

The conditions for data objects to be in the correct state for having had consent
withdrawn are: 

 - The data object has the correct metadata. Either:
     - sample_consent = 0 (data managed by the GAPI codebase)
     - sample_consent_withdrawn = 1 (data managed by the NPG codebase)

 - Read permission for any SequenceScape study iRODS groups (named ss_<Study ID>)
   absent.

N.B. having this consent withdrawn state is only the first step in handling consent
removal. Subsequent steps, including redaction of data within data objects are not
handled.

This script will never change any values. To withdraw consent, use the --print-fail
option to print failed paths to STDOUT and pipe them to the `withdraw-consent` script
included in this package.

If any of the paths fail their check, the exit code will be non-zero and an error
message summarising the results will be sent to STDERR.
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
    type=argparse.FileType("w", encoding="UTF-8"),
    default=sys.stdout,
)
parser.add_argument(
    "--print-pass",
    help="Print to output those paths that pass the check. Defaults to True.",
    action="store_true",
)
parser.add_argument(
    "--print-fail",
    help="Print to output those paths that fail the check. Defaults to False.",
    action="store_true",
)
parser.add_argument("--version", help="Print the version and exit", action="store_true")


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
        sys.exit(0)

    num_processed, num_passed, num_errors = check_consent_withdrawn(
        args.input,
        args.output,
        print_pass=args.print_pass,
        print_fail=args.print_fail,
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
