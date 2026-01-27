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
import shutil
from pathlib import PurePath
from unittest.mock import MagicMock, patch

from helpers import (
    is_inheritance_enabled,
    ADMIN_AC,
    PUBLIC_AC,
    UNMANAGED_AC,
    STUDY2_AC,
    history_in_meta,
)
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
        public_unmanaged_inheritance_enabled_collection: PurePath,
        monkeypatch: MonkeyPatch,
    ):
        # Arrange
        src = tmp_path / "minimal"
        shutil.copytree("./tests/data/ultima/minimal", src)
        # empty_collection stands in for $ZONE/ultimagen/runs
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
            str(src),
            str(dest),
            "--force",
            "--group",
            "public",
            "--exclude",
            f"{src}/000001-",
            "--exclude",
            ".md5",
            "--metadata-file",
            str(root_metadata),
        ]
        self._main(root_args)

        # Study
        sample_metadata = tmp_path / "sample_metadata.json"
        sample_metadata.write_text(
            json.dumps(
                [{"attribute": "a2", "value": "v2"}, {"attribute": "a3", "value": "v3"}]
            )
        )
        sample_dir_args = [
            str(src / "000001-a"),
            str(dest / "000001-a"),
            "--force",
            "--group",
            "ss_1000#testZone",
            "--exclude",
            ".md5",
            "--metadata-file",
            str(sample_metadata),
        ]
        self._main(sample_dir_args)

        # Private
        private_dir_args = [str(src / "000001-d"), str(dest / "000001-d"), "--force"]
        self._main(private_dir_args)

        # Repeated publish: No changes
        self._main(root_args)
        self._main(sample_dir_args)
        self._main(private_dir_args)

        # Repeated publish: New file
        (src / "c.txt").write_text("new")
        self._main(root_args)
        created_values = [
            x
            for x in DataObject(dest / "c.txt").metadata()
            if x.attribute == "dcterms:created"
        ]
        assert len(created_values) == 1
        c_created = created_values[0]

        # Repeated publish: Modified file
        (src / "c.txt").write_text("modified")
        self._main(root_args)
        created_values = [
            x
            for x in DataObject(dest / "c.txt").metadata()
            if x.attribute == "dcterms:created"
        ]
        assert len(created_values) == 1
        c_created_updated = created_values[0]

        # Repeated publish: Different group, different metadata, no file changes
        sample_metadata_updated = tmp_path / "sample_metadata_updated.json"
        # a2: Deleted
        # a3: Updated
        # a4: Added
        sample_metadata_updated.write_text(
            json.dumps(
                [
                    {"attribute": "a3", "value": "v3_updated"},
                    {"attribute": "a4", "value": "v4"},
                ]
            )
        )
        sample_dir_updated_args = sample_dir_args.copy()
        sample_dir_updated_args[sample_dir_updated_args.index("ss_1000#testZone")] = (
            "ss_2000#testZone"
        )
        sample_dir_updated_args[sample_dir_updated_args.index(str(sample_metadata))] = (
            str(sample_metadata_updated)
        )
        self._main(sample_dir_updated_args)

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
            DataObject(dest / "c.txt"),
            DataObject(dest / "000001-a" / "000002-c.txt"),
            DataObject(dest / "000001-d" / "000001-d.txt"),
        ]
        assert [x.attribute for x in DataObject(dest / "b.txt").metadata()] == [
            "dcterms:created",
            "dcterms:creator",
            "md5",
            "type",
        ]
        assert [x.attribute for x in DataObject(dest / "c.txt").metadata()] == [
            "dcterms:created",
            "dcterms:creator",
            "md5",
            "md5",
            "type",
        ]
        assert c_created_updated == c_created

        assert DataObject(dest / "c.txt").read() == "modified"

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
        assert DataObject(dest / "c.txt").acl() == [
            ADMIN_AC,
            PUBLIC_AC,
            UNMANAGED_AC,
        ]

        assert is_inheritance_enabled(dest / "000001-a")
        assert Collection(dest / "000001-a").acl() == [
            ADMIN_AC,
            STUDY2_AC,
            UNMANAGED_AC,
        ]
        assert [
            x
            for x in Collection(dest / "000001-a").metadata()
            if x.attribute != "a3_history"
        ] == [
            AVU("a2", "v2"),  # Earlier metadata preserved
            AVU("a3", "v3_updated"),
            AVU("a4", "v4"),
        ]
        assert history_in_meta(
            AVU.history(AVU("a3", "v3")), Collection(dest / "000001-a").metadata()
        )
        assert len(Collection(dest / "000001-a").metadata()) == 4

        assert DataObject(dest / "000001-a" / "000002-c.txt").acl() == [
            ADMIN_AC,
            STUDY2_AC,
            UNMANAGED_AC,
        ]
        assert [
            x.attribute
            for x in DataObject(dest / "000001-a" / "000002-c.txt").metadata()
        ] == [
            "dcterms:created",
            "dcterms:creator",
            "md5",
            "type",
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

    @staticmethod
    def _main(args: list[str]):
        with patch("sys.argv", ["publish-directory"] + args):
            publish_directory.main()
