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
import json
from pathlib import Path, PurePath
from unittest.mock import MagicMock, patch

from npg_irods.cli import publish_directory
from pytest import LogCaptureFixture, MonkeyPatch
from pytest import mark as m

from partisan.irods import Collection, DataObject, AC, Permission, AVU


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

    @m.context(
        'When run in place of npg_publish_tree.pl in "Uploading Ultima run to iRODS" SOP from rnd_platforms'
    )
    @m.it("Should be compatible with npg_publish_tree.pl")
    def test_npg_publish_tree_compatibility_ultima(
        self, tmp_path, empty_collection: PurePath, monkeypatch: MonkeyPatch
    ):
        # Arrange
        src = Path("./tests/data/ultima/minimal").absolute()
        # empty_collection stands in for $ZONE/ultimagen/runs
        # SOP: Destination collection doesn't exist
        dest = empty_collection / "run_id_prefix" / "run_id"
        # SOP: Perform wr jobs from /tmp
        monkeypatch.chdir(tmp_path)

        # Act
        root_metadata = tmp_path / "root_metadata.json"
        root_metadata.write_text(json.dumps([{"attribute": "a1", "value": "v1"}]))
        self._main(
            [
                str(src),
                str(dest),
                "--group",
                "public",
                "--exclude",
                f"{src}/000001-",
                "--exclude",
                ".md5",
                "--metadata-file",
                str(root_metadata),
            ]
        )

        # Assert
        assert Collection(dest).contents(recurse=True) == [
            DataObject(dest / "000001_a.txt"),
            DataObject(dest / "b.txt"),
        ]

        # Act
        sample_metadata = tmp_path / "sample_metadata.json"
        sample_metadata.write_text(json.dumps([{"attribute": "a2", "value": "v2"}]))
        self._main(
            [
                str(src / "000001-a"),
                str(dest / "000001-a"),
                "--group",
                "ss_1000",
                "--exclude",
                ".md5",
                "--metadata-file",
                str(sample_metadata),
            ]
        )

        # Assert
        assert Collection(dest).contents(recurse=True) == [
            Collection(dest / "000001-a"),
            DataObject(dest / "000001_a.txt"),
            DataObject(dest / "b.txt"),
            DataObject(dest / "000001-a" / "000002-c.txt"),
        ]
        irods_own = AC("irods", Permission.OWN, "testZone")
        public_read = AC("public", Permission.READ, "testZone")
        ss_1000_read = AC("ss_1000", Permission.READ, "testZone")
        assert Collection(empty_collection / "run_id_prefix").acl() == [irods_own]
        assert Collection(empty_collection / "run_id_prefix").metadata() == []
        assert Collection(dest).acl() == [irods_own, public_read]
        assert Collection(dest).metadata() == [AVU("a1", "v1")]
        assert Collection(dest / "000001-a").acl() == [irods_own, ss_1000_read]
        assert Collection(dest / "000001-a").metadata() == [AVU("a2", "v2")]

    @staticmethod
    def _main(args: list[str]):
        with patch("sys.argv", ["publish-directory"] + args):
            publish_directory.main()
