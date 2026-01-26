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
from unittest.mock import patch

from pytest import mark as m
from pytest import MonkeyPatch

from npg_irods.functions import make_path_filter


@m.describe("make_path_filter")
class TestMakePathFilter:

    @m.context("With exclude pattern")
    @m.it("Exclude items that match filter anywhere in path")
    def test_exclude(self):
        # Arrange
        paths = [Path("a1b"), Path("a2b"), Path("a3b")]

        # Act
        filter_fn = make_path_filter(exclude_patterns=["1", "2"])
        filtered = set(x for x in paths if filter_fn is None or not filter_fn(x))

        # Assert
        assert filtered == {Path("a3b")}

    @m.context("With include pattern")
    @m.it("Include items that match filter anywhere in path")
    def test_include(self):
        # Arrange
        paths = [Path("a1b"), Path("a2b"), Path("a3b")]

        # Act
        filter_fn = make_path_filter(include_patterns=["1", "2"])
        filtered = set(x for x in paths if filter_fn is None or not filter_fn(x))

        # Assert
        assert filtered == {Path("a1b"), Path("a2b")}

    @m.context("When include and exclude patterns provided")
    @m.it("Should compose filters")
    def test_include_exclude(self):
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

        # Act
        filter_fn = make_path_filter(
            include_patterns=["1", "2"], exclude_patterns=["X", "Y"]
        )
        filtered = set(x for x in paths if filter_fn is None or not filter_fn(x))

        # Assert
        assert filtered == {Path("a1Zb"), Path("a2Zb")}

    @m.context("With include top level files option")
    @m.it("Includes top level files")
    def test_include_top_level_files(self, monkeypatch: MonkeyPatch):
        # Arrange
        monkeypatch.chdir(Path("./tests/data/ultima/minimal"))
        paths = list(Path(".").rglob("*"))

        # Act
        filter_fn = make_path_filter(include_top_level_files=True, src=Path("."))
        filtered = set(x for x in paths if filter_fn is None or not filter_fn(x))

        # Assert
        assert filtered == {
            Path("000001_a.txt"),
            Path("000001_a.txt.md5"),
            Path("b.txt"),
            Path("b.txt.md5"),
        }

    @m.context("With exclude MD5 option")
    @m.it("Excludes md5 files")
    def test_exclude_md5(self, monkeypatch: MonkeyPatch):
        # Arrange
        monkeypatch.chdir(Path("./tests/data/ultima/minimal"))
        paths = list(Path(".").rglob("*"))

        # Act
        filter_fn = make_path_filter(exclude_md5=True)
        filtered = set(x for x in paths if filter_fn is None or not filter_fn(x))

        # Assert
        assert filtered == {
            Path("000001_a.txt"),
            Path("b.txt"),
            Path("000001-a"),
            Path("000001-a/000002-c.txt"),
            Path("000001-d"),
            Path("000001-d/000001-d.txt"),
        }
        filtered = [x for x in paths if filter_fn is None or not filter_fn(x)]
