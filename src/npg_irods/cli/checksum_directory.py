# -*- coding: utf-8 -*-
#
# Copyright © 2026 Genome Research Ltd. All rights reserved.
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
# @author Calum Eadie <ce10@sanger.ac.uk>

import argparse
from pathlib import Path

import structlog
from npg.cli import add_logging_arguments
from npg.log import configure_structlog

from npg_irods import add_appinfo_structlog_processor, version
from npg_irods.checksum import checksum_directory
from npg_irods.utilities import get_md5sums_path

description = """
A utility to calculate MD5 checksums for all files in a directory.

The output follows GNU coreutils md5sum format. Checksum files (*.md5) are
ignored. Files with existing checksums are skipped.

Supports two modes of specifying checksum locations:

- Explicitly specifying a file to write checksums to (`--md5sums-path`)
- Specifying a directory (`--checksums-directory`). Checksums are written to a
  file within the directory that can be uniquely retrieved by other
  npg-irods-python utilities.
"""


def logger():
    return structlog.get_logger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description=description, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    add_logging_arguments(parser)

    parser.add_argument(
        "--directory",
        help="The directory to checksum.",
        type=str,
    )

    checksums_file_group = parser.add_mutually_exclusive_group(required=True)

    checksums_file_group.add_argument(
        "--md5sums-path",
        help="The file to write checksums to.",
        type=str,
    )

    checksums_file_group.add_argument(
        "--checksums-directory",
        help="The directory to write checksums to."
        "The path of the file to write checksums to is derived from the path of"
        "the directory to checksum.",
    )

    parser.add_argument(
        "--version",
        help="Print the version and exit.",
        action="version",
        version=version(),
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

    path = Path(args.directory)

    if args.md5sums_path:
        md5sums_path = Path(args.md5sums_path)
    elif args.checksums_directory:
        md5sums_path = get_md5sums_path(Path(args.checksums_directory), path)
    else:
        raise ValueError(
            "Expected an md5sums path or a checksums directory to be specified."
        )

    logger().info("Checksumming directory", directory=path, md5sums=md5sums_path)

    num_files, num_checksummed = checksum_directory(
        path,
        md5sums_path,
    )

    logger().info(
        "Checksummed directory successfully",
        num_files=num_files,
        num_checksummed=num_checksummed,
    )


if __name__ == "__main__":
    main()
