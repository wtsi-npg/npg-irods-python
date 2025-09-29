# -*- coding: utf-8 -*-
#
# Copyright © 2025 Genome Research Ltd. All rights reserved.
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

from pathlib import Path
from unittest.mock import MagicMock, patch

from pytest import mark as m

from npg_irods.functions import make_path_filter


@m.describe("make_path_filter")
class TestMakePathFilter:

    @m.context("When run with --exclude")
    @m.it("Exclude items that match filter anywhere in path")
    @patch("npg_irods.cli.publish_directory.publish_directory", autospec=True)
    def test_exclude(self, mock_publish_directory: MagicMock):
        # Arrange
        paths = [Path("a1b"), Path("a2b"), Path("a3b")]
        mock_publish_directory.return_value = (2, 1, 0)

        # Act
        filter_fn = make_path_filter(exclude_patterns=["1", "2"])
        filtered = [x for x in paths if filter_fn is None or not filter_fn(x)]

        # Assert
        assert filtered == [Path("a3b")]

    @m.context("With include pattern")
    @m.it("Include items that match filter anywhere in path")
    @patch("npg_irods.cli.publish_directory.publish_directory", autospec=True)
    def test_include(self, mock_publish_directory: MagicMock):
        # Arrange
        paths = [Path("a1b"), Path("a2b"), Path("a3b")]
        mock_publish_directory.return_value = (2, 1, 0)

        # Act
        filter_fn = make_path_filter(include_patterns=["1", "2"])
        filtered = [x for x in paths if filter_fn is None or not filter_fn(x)]

        # Assert
        assert filtered == [Path("a1b"), Path("a2b")]

    @m.context("When include and exclude patterns provided")
    @m.it("Should compose filters")
    @patch("npg_irods.cli.publish_directory.publish_directory", autospec=True)
    def test_include_exclude(self, mock_publish_directory: MagicMock):
        # Arrange
        paths = [
            Path("a1Xb"),
            Path("a1Yb"),
            Path("a1Zb"),
            Path("a2Xb"),
            Path("a2Yb"),
            Path("a2Zb"),
            Path("a3Xb"),
            Path("a3Yb"),
            Path("a3Zb"),
        ]
        mock_publish_directory.return_value = (2, 1, 0)

        # Act
        filter_fn = make_path_filter(
            include_patterns=["1", "2"], exclude_patterns=["X", "Y"]
        )
        filtered = [x for x in paths if filter_fn is None or not filter_fn(x)]

        # Assert
        assert filtered == [Path("a1Zb"), Path("a2Zb")]
