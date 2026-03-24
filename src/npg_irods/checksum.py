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

from hashlib import file_digest
from pathlib import Path

from npg_irods.utilities import read_md5sums_file, log


def checksum_directory(path: Path, md5sums_path: Path):
    """Calculate MD5 checksums for all files in a directory and write to file.

    The output follows GNU coreutils md5sum format. Checksum files (*.md5) are
    ignored. Files with existing checksums are skipped.

    Args:
        path (Path): The directory to checksum.
        md5sums_path (Path): The file to write checksums to.

    Returns:
        A tuple containing the number of files considered and the number of files
        checksummed (not skipped).
    """

    num_files = 0
    num_checksummed = 0

    path = path.resolve()

    md5sums = read_md5sums_file(md5sums_path) if md5sums_path.exists() else {}

    # Use line buffering so checksums we've already calculated are persisted even
    # if something goes wrong.
    with md5sums_path.open("a", buffering=1) as md5sums_file:
        for path in sorted(path.rglob("*")):
            if path.is_file() and path.suffix.lower() != ".md5":
                num_files += 1

                if path in md5sums:
                    log.debug(
                        "Match found in md5sums file. Skipping.",
                        path=path,
                        md5sums_path=md5sums_path,
                    )
                    continue

                with open(path, "rb") as f:
                    digest = file_digest(f, "md5")

                md5sum = digest.hexdigest()
                md5sums_file.write(f"{md5sum}  {path}\n")

                log.debug("Calculated checksum.", path=path, md5sum=md5sum)

                num_checksummed += 1

    return num_files, num_checksummed
