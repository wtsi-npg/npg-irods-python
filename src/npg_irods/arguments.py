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
from argparse import ArgumentParser
from pathlib import Path
from typing import Callable

from structlog import get_logger

from npg_irods.utilities import read_md5_file, make_get_checksum

log = get_logger(__package__)


def add_checksum_arguments(parser: ArgumentParser) -> ArgumentParser:
    """Adds arguments for using checksums.

    Args:
        parser: An argument parser to modify.

    Returns:
        The parser
    """

    checksums_group = parser.add_mutually_exclusive_group(required=False)
    checksums_group.add_argument(
        "--use-checksum-files",
        help="Expect checksum files to be present alongside the data files with "
        "the same name as the data file but with an additional '.md5' extension"
        "e.g. 'data.txt' and 'data.txt.md5'. Each checksum file should contain only "
        "the single MD5 checksum of the corresponding data file. This avoids having "
        "to calculate the checksums during the publish process. If this option is "
        "enabled and a checksum file cannot be read, an error will be raised for "
        "that file. Optional, defaults to false.",
        action="store_true",
    )
    checksums_group.add_argument(
        "--use-checksums-file",
        help="Expect checksums to be present in a checksums file at path specified "
        "following GNU coreutils md5sum format. This avoids having to calculate the "
        "checksums during the publish process. If this option is enabled and a "
        "checksum is missing or stale, an error will be raised for that file. "
        "Optional, defaults to none.",
        type=str,
        default=None,
    )

    return parser


def make_checksum_fn(args) -> Callable[[Path | str], str] | None:
    """Makes checksum function using args from add_checksum_arguments."""
    if args.use_checksum_files:
        return read_md5_file

    if args.use_checksums_file:
        try:
            return make_get_checksum(Path(args.use_checksums_file))
        except Exception as e:
            log.error(
                "Failed to read checksums file",
                path=args.use_checksums_file,
                error=str(e),
            )
            raise e

    return None
