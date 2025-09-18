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
from pathlib import Path, PurePath
from unittest.mock import MagicMock, patch

from npg_irods.cli import publish_directory
from pytest import LogCaptureFixture, MonkeyPatch
from pytest import mark as m

from partisan.irods import Collection, DataObject

@m.describe("Publish directory utility")
class TestPublishDirectory:

    @m.context("When run with default parameters only")
    @m.it("Publishes directory and outputs status")
    @patch("npg_irods.cli.publish_directory.publish_directory", autospec=True)
    def test_main_normal_case(
        self, mock_publish_directory: MagicMock, caplog: LogCaptureFixture
    ):
        # Arrange
        mock_publish_directory.return_value = (2, 1, 0)

        # Act
        with caplog.at_level("DEBUG", "main"):
            self._main(["directory", "/collection"])

        # Assert
        mock_publish_directory.assert_called_once_with(
            "directory",
            "/collection",
            avus=[],
            acl=[],
            filter_fn=None,
            local_checksum=None,
            fill=False,
            force=False,
            handle_exceptions=True,
            num_clients=4,
        )
        assert "Processed all items successfully" in caplog.text
        assert "num_items=2" in caplog.text
        assert "num_processed=1" in caplog.text
        assert "num_errors=0" in caplog.text

    @patch("npg_irods.cli.publish_directory.publish_directory", autospec=True)
    def test_exclude(self, mock_publish_directory: MagicMock):
        # Arrange
        paths = [Path("a1b"), Path("a2b"), Path("a3b")]
        mock_publish_directory.return_value = (2, 1, 0)

        # Act
        # TODO: Just test make_path_filter?
        self._main(["directory", "/collection", "--exclude", "1", "--exclude", "2"])
        mock_publish_directory.assert_called_once()
        filter_fn = mock_publish_directory.call_args.kwargs["filter_fn"]
        filtered = [x for x in paths if filter_fn is None or not filter_fn(x)]

        # Assert
        assert filtered == [Path("a3b")]

    @patch("npg_irods.cli.publish_directory.publish_directory", autospec=True)
    def test_include(self, mock_publish_directory: MagicMock):
        # Arrange
        paths = [Path("a1b"), Path("a2b"), Path("a3b")]
        mock_publish_directory.return_value = (2, 1, 0)

        # Act
        # TODO: Just test make_path_filter?
        self._main(["directory", "/collection", "--include", "1", "--include", "2"])
        mock_publish_directory.assert_called_once()
        filter_fn = mock_publish_directory.call_args.kwargs["filter_fn"]
        filtered = [x for x in paths if filter_fn is None or not filter_fn(x)]

        # Assert
        assert filtered == [Path("a1b"), Path("a2b")]

    @patch("npg_irods.cli.publish_directory.publish_directory", autospec=True)
    def test_include_exclude(self, mock_publish_directory: MagicMock):
        # Arrange
        paths = [Path("a1Xb"), Path("a1Yb"), Path("a1Zb"), Path("a2Xb"), Path("a2Yb"), Path("a2Zb"), Path("a3Xb"), Path("a3Yb"), Path("a3Zb")]
        mock_publish_directory.return_value = (2, 1, 0)

        # Act
        # TODO: Just test make_path_filter?
        self._main(["directory", "/collection", "--include", "1", "--include", "2", "--exclude", "X", "--exclude", "Y"])
        mock_publish_directory.assert_called_once()
        filter_fn = mock_publish_directory.call_args.kwargs["filter_fn"]
        filtered = [x for x in paths if filter_fn is None or not filter_fn(x)]

        # Assert
        assert filtered == [Path("a1Zb"), Path("a2Zb")]

    # Test compatibility with npg_publish_tree.pl
    # Use Ultima as test case, follow rnd_platforms SOP
    def test_npg_publish_tree_compatibility_ultima(self, tmpdir, empty_collection: PurePath, monkeypatch: MonkeyPatch):
        # Arrange
        src = Path("./tests/data/ultima").absolute()
        # empty_collection stands in for $ZONE/ultimagen/runs
        # SOP: Destination collection doesn't exist
        dest = empty_collection / "run_id_prefix" / "run_id"
        # SOP: Perform wr jobs from /tmp
        monkeypatch.chdir(tmpdir)

        # Act
        # TODO: Metadata
        self._main([str(src), str(dest), "--group", "public", "--exclude", f"'{src}/000001-'", "--exclude", "'.md5'"])

        # Assert
        assert Collection(dest).contents(recurse=True) == [
            DataObject(dest / "000001_a.txt"),
            DataObject(dest / "b.txt"),
        ]

    def _main(self, args: list[str]):
        with patch("sys.argv", ["publish-directory"] + args):
            publish_directory.main()