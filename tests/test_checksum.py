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
from pathlib import Path, PosixPath
from unittest.mock import patch, MagicMock

from pytest import mark as m
from pytest import LogCaptureFixture

from npg_irods.checksum import checksum_directory
from npg_irods.cli import checksum_directory as checksum_directory_script


@m.describe("Checksum Script")
class TestChecksumScript:

    @m.context("When run with default parameters only")
    @m.it("Checksums the directory and outputs the status")
    @patch("npg_irods.cli.checksum_directory.checksum_directory", autospec=True)
    def test_main_normal_case(
        self, mock_checksum_directory: MagicMock, caplog: LogCaptureFixture
    ):
        # Arrange
        mock_checksum_directory.return_value = (2, 1)

        # Act
        with caplog.at_level("DEBUG", "main"):
            self._main(["--directory", "directory", "--md5sums-path", "md5sums_path"])

        # Assert
        mock_checksum_directory.assert_called_once_with(
            PosixPath("directory"),
            PosixPath("md5sums_path"),
        )
        assert "Checksummed path successfully" in caplog.text
        assert "num_files=2" in caplog.text
        assert "num_checksummed=1" in caplog.text

    @staticmethod
    def _main(args: list[str]):
        with patch("sys.argv", ["checksum-directory"] + args):
            checksum_directory_script.main()


@m.describe("Checksum")
class TestChecksum:

    @m.context("When checksumming a directory without an existing checksum file")
    @m.it("Creates a checksum file")
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

    @m.context("When checksumming a directory with an existing checksum file")
    @m.it("Creates a checksum file")
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
