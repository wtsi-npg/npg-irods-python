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
from pathlib import Path
from unittest.mock import patch

import pytest
from pytest import LogCaptureFixture, CaptureFixture
from pytest import mark as m

from npg_irods.cli import get_xenium_output_directories


@m.describe("get-xenium-output-directories")
class TestGetXeniumOutputDirectoriesScript:

    @m.context("When getting xenium output directories")
    @m.it("Should output experiment directories")
    def test_main_normal_case(
        self,
        caplog: LogCaptureFixture,
        capsys: CaptureFixture,
        xenium_staging_dir: Path,
    ) -> None:
        # Arrange
        root = str(xenium_staging_dir)

        # Act
        with caplog.at_level("DEBUG"):
            self._main([root])

        # Assert
        stdout_lines = [line for line in capsys.readouterr().out.split("\n") if line]
        expected = [
            root + "/level_1",
            root + "/sub/level_2",
        ]
        assert stdout_lines == expected

        assert "Search completed" in caplog.text
        assert "num_dirs=5" in caplog.text
        assert "num_experiments=2" in caplog.text
        assert "num_errors=0" in caplog.text

    @m.context("When some parts of tree cannot be searched")
    @m.it("Should output experiment directories that can be")
    def test_main_error_case(
        self,
        caplog: LogCaptureFixture,
        capsys: CaptureFixture,
        xenium_staging_dir_with_error: Path,
    ):
        # Arrange
        root = str(xenium_staging_dir_with_error)

        # Act
        with caplog.at_level("DEBUG"):
            with pytest.raises(SystemExit) as system_exit:
                self._main([root])

        # Assert
        assert system_exit.value.code == 1

        stdout_lines = [line for line in capsys.readouterr().out.split("\n") if line]
        expected = [
            root + "/level_1",
            root + "/sub/level_2",
        ]
        assert stdout_lines == expected

        assert "Some parts of tree could not be searched" in caplog.text
        assert "num_dirs=5" in caplog.text
        assert "num_experiments=2" in caplog.text
        assert "num_errors=1" in caplog.text

    @staticmethod
    def _main(args: list[str]):
        with patch("sys.argv", ["get-xenium-output-directories"] + args):
            get_xenium_output_directories.main()
