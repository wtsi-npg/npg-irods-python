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
from pathlib import Path

from pytest import mark as m

from npg_irods.checksum import checksum_directory


@m.describe("Checksum")
class TestChecksum:

    @m.context("When checksumming a directory without existing checksum file")
    @m.it("Create checksum file")
    def test_checksum_directory_no_existing(self, tmp_path):
        # Arrange
        path = Path("./tests/data/simple/collection").absolute()
        md5sums_path = tmp_path / "collection.md5"

        # Act
        num_files, num_checksummed = checksum_directory(path, md5sums_path)

        # Assert
        assert num_files == 2
        assert num_checksummed == 2
        assert (
            md5sums_path.read_text()
            == f"""cac862166e910d51dc16aa0eab7a7a7c  {path}/a.txt
92f14d525211301f5ccb1ab6a8884fb3  {path}/sub/b.txt
"""
        )

    @m.context("When checksumming a directory with existing checksum file")
    @m.it("Create checksum file")
    def test_checksum_directory_no_existing(self, tmp_path):
        # Arrange
        path = Path("./tests/data/simple/collection").absolute()
        md5sums_path = tmp_path / "collection.md5"
        md5sums_path.write_text(f"""cac862166e910d51dc16aa0eab7a7a7c  {path}/a.txt
""")

        # Act
        num_files, num_checksummed = checksum_directory(path, md5sums_path)

        # Assert
        assert num_files == 2
        assert num_checksummed == 1
        assert (
            md5sums_path.read_text()
            == f"""cac862166e910d51dc16aa0eab7a7a7c  {path}/a.txt
92f14d525211301f5ccb1ab6a8884fb3  {path}/sub/b.txt
"""
        )
