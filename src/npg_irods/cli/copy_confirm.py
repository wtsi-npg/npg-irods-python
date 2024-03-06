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
from partisan.exception import RodsError

from npg_irods.exception import ChecksumError
from npg_irods.utilities import copy
from npg_irods.cli.util import add_logging_arguments, configure_logging, rods_path
from npg_irods.version import version

description = """
Copies iRODS collections and data objects from one path to another, optionally
including metadata and permissions.

This script provides a means for safe, idempotent bulk copying of collections, data 
objects, metadata and permissions. It will not forcibly overwrite existing collections 
or data objects, so for an idempotent mode, use the --skip-existing option which will 
compare source and destination data objects by checksum and skip copying those that 
already exist at the destination.

The checksum comparison used for data objects requires both the source and destination 
data objects (if there is one present already) to have consistent checksums on all 
their valid replicas. If this is not the case, the copy operation raise an error and
abort further copying until the problem is resolved. 
"""

parser = argparse.ArgumentParser(
    description=description,
    formatter_class=argparse.RawDescriptionHelpFormatter,
)
add_logging_arguments(parser)
parser.add_argument(
    "source",
    help="Collection or data object path to copy from. Must be an absolute path.",
    nargs="?",
    type=rods_path,
)
parser.add_argument(
    "destination",
    help="Collection or data object path to copy to. Must be an absolute path.",
    nargs="?",
)
parser.add_argument(
    "--copy-metadata",
    help="Copy the AVUs (metadata) of each data object and collection.",
    action="store_true",
)
parser.add_argument(
    "--copy-permissions",
    help="Copy the ACL (access control list, permissions) of each data object "
    "and collection.",
    action="store_true",
)
parser.add_argument(
    "--recurse",
    help="Recurse into collections when copying.",
    action="store_true",
)
parser.add_argument(
    "--skip-existing",
    help="Skip existing data objects and collections when copying.",
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
        sys.exit(0)

    try:
        num_processed, num_copied = copy(
            args.source,
            args.destination,
            avu=args.copy_metadata,
            acl=args.copy_permissions,
            recurse=args.recurse,
            exist_ok=args.skip_existing,
        )

        log.info(
            f"Copied {args.source} to {args.destination}",
            num_processed=num_processed,
            num_copied=num_copied,
        )

    except ChecksumError as ce:
        log.error(ce.message, path=ce.path, expected=ce.expected, observed=ce.observed)
        sys.exit(1)
    except RodsError as re:
        log.error(re.message, code=re.code)
        sys.exit(1)
    except Exception as e:
        log.error(e)
        sys.exit(1)
