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
from pathlib import PurePath
from unittest.mock import MagicMock, patch

import pytest

from helpers import is_inheritance_enabled, ADMIN_AC, PUBLIC_AC, STUDY_AC, UNMANAGED_AC
from npg_irods.cli import publish_directory
from pytest import LogCaptureFixture, MonkeyPatch
from pytest import mark as m

from partisan.irods import Collection, DataObject, AVU


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
        self,
        tmp_path,
        ultima_run,
        public_unmanaged_inheritance_enabled_collection: PurePath,
        monkeypatch: MonkeyPatch,
    ):
        # Arrange
        run_dir, checksums_path = ultima_run
        # SOP: Destination collection doesn't exist
        dest = (
            public_unmanaged_inheritance_enabled_collection / "run_id_prefix" / "run_id"
        )
        # SOP: Perform wr jobs from /tmp
        monkeypatch.chdir(tmp_path)

        # Act

        # Public
        root_metadata = tmp_path / "root_metadata.json"
        root_metadata.write_text(json.dumps([{"attribute": "a1", "value": "v1"}]))
        root_args = [
            str(run_dir),
            str(dest),
            "--fill",
            "--use-checksums-file",
            str(checksums_path),
            "--group",
            "public",
            "--exclude",
            f"{run_dir}/000001-",
            "--exclude",
            ".md5",
            "--metadata-file",
            str(root_metadata),
        ]
        self._main(root_args)

        # Study
        sample_metadata = tmp_path / "sample_metadata.json"
        sample_metadata.write_text(json.dumps([{"attribute": "a2", "value": "v2"}]))
        self._main(
            [
                str(run_dir / "000001-a"),
                str(dest / "000001-a"),
                "--fill",
                "--use-checksums-file",
                str(checksums_path),
                "--group",
                "ss_1000#testZone",
                "--exclude",
                ".md5",
                "--metadata-file",
                str(sample_metadata),
            ]
        )

        # Private
        self._main(
            [
                str(run_dir / "000001-d"),
                str(dest / "000001-d"),
                "--fill",
                "--use-checksums-file",
                str(checksums_path),
            ]
        )

        # Repeat
        self._main(root_args)

        # Assert
        assert is_inheritance_enabled(
            public_unmanaged_inheritance_enabled_collection / "run_id_prefix"
        )
        assert Collection(
            public_unmanaged_inheritance_enabled_collection / "run_id_prefix"
        ).acl() == [ADMIN_AC, PUBLIC_AC, UNMANAGED_AC]
        assert (
            Collection(
                public_unmanaged_inheritance_enabled_collection / "run_id_prefix"
            ).metadata()
            == []
        )

        assert is_inheritance_enabled(dest)
        assert Collection(dest).acl() == [ADMIN_AC, PUBLIC_AC, UNMANAGED_AC]
        assert Collection(dest).metadata() == [AVU("a1", "v1")]

        assert Collection(dest).contents(recurse=True) == [
            Collection(dest / "000001-a"),
            Collection(dest / "000001-d"),
            DataObject(dest / "000001_a.txt"),
            DataObject(dest / "b.txt"),
            DataObject(dest / "000001-a" / "000002-c.txt"),
            DataObject(dest / "000001-d" / "000001-d.txt"),
        ]

        assert DataObject(dest / "000001_a.txt").acl() == [
            ADMIN_AC,
            PUBLIC_AC,
            UNMANAGED_AC,
        ]
        assert DataObject(dest / "b.txt").acl() == [
            ADMIN_AC,
            PUBLIC_AC,
            UNMANAGED_AC,
        ]

        assert is_inheritance_enabled(dest / "000001-a")
        assert Collection(dest / "000001-a").acl() == [ADMIN_AC, STUDY_AC, UNMANAGED_AC]
        assert Collection(dest / "000001-a").metadata() == [AVU("a2", "v2")]
        assert DataObject(dest / "000001-a" / "000002-c.txt").acl() == [
            ADMIN_AC,
            STUDY_AC,
            UNMANAGED_AC,
        ]

        assert is_inheritance_enabled(dest / "000001-d")
        assert Collection(dest / "000001-d").acl() == [
            ADMIN_AC,
            UNMANAGED_AC,
        ]
        assert DataObject(dest / "000001-d" / "000001-d.txt").acl() == [
            ADMIN_AC,
            UNMANAGED_AC,
        ]

    @m.context("When checksum missing")
    @m.it("Skips that file, outputs errors and publishes other files")
    def test_missing_checksum(
        self, ultima_run, empty_collection_path: PurePath, caplog: LogCaptureFixture
    ):
        # Arrange
        run_dir, checksums_path = ultima_run
        checksums_path.write_text(
            "".join(checksums_path.read_text().splitlines(keepends=True)[:-1])
        )
        dest = empty_collection_path

        # Act
        with pytest.raises(SystemExit):
            self._main(
                [
                    str(run_dir),
                    str(dest),
                    "--fill",
                    "--use-checksums-file",
                    str(checksums_path),
                    "--exclude-md5",
                ]
            )

        # Assert
        assert Collection(dest).contents(recurse=True) == [
            Collection(dest / "000001-a"),
            Collection(dest / "000001-d"),
            DataObject(dest / "000001_a.txt"),
            # No b.txt
            DataObject(dest / "000001-a" / "000002-c.txt"),
            DataObject(dest / "000001-d" / "000001-d.txt"),
        ]

        assert "Processed some items with errors" in caplog.text
        assert f"No checksum found for {run_dir / "b.txt"}" in caplog.text
        assert "num_items=7" in caplog.text
        assert "num_processed=6" in caplog.text
        assert "num_errors=1" in caplog.text

    @m.context("When checksum stale")
    @m.it("Skips that file, outputs errors and publishes other files")
    def test_stale_checksum(
        self, ultima_run, empty_collection_path: PurePath, caplog: LogCaptureFixture
    ):
        # Arrange
        run_dir, checksums_path = ultima_run
        (run_dir / "b.txt").touch()
        dest = empty_collection_path

        # Act
        with pytest.raises(SystemExit):
            self._main(
                [
                    str(run_dir),
                    str(dest),
                    "--fill",
                    "--use-checksums-file",
                    str(checksums_path),
                    "--exclude-md5",
                ]
            )

        # Assert
        assert Collection(dest).contents(recurse=True) == [
            Collection(dest / "000001-a"),
            Collection(dest / "000001-d"),
            DataObject(dest / "000001_a.txt"),
            # No b.txt
            DataObject(dest / "000001-a" / "000002-c.txt"),
            DataObject(dest / "000001-d" / "000001-d.txt"),
        ]

        assert "Processed some items with errors" in caplog.text
        assert f"Checksum for {run_dir / "b.txt"} may be out of date" in caplog.text
        assert "num_items=7" in caplog.text
        assert "num_processed=6" in caplog.text
        assert "num_errors=1" in caplog.text

    @staticmethod
    def _main(args: list[str]):
        with patch("sys.argv", ["publish-directory"] + args):
            publish_directory.main()
