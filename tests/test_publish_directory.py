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

from unittest.mock import MagicMock, patch
from npg_irods.cli import publish_directory
from pytest import LogCaptureFixture
from pytest import mark as m


@m.describe("Publish directory utility")
class TestPublishDirectory:

    @m.context("When run with default parameters only")
    @m.it("Publishes directory and outputs status")
    @patch("npg_irods.cli.publish_directory.publish_directory", autospec=True)
    def test_main_normal_case(
        self, mock_publish_directory: MagicMock, caplog: LogCaptureFixture, capsys
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

    def _main(self, args: list[str]):
        with patch("sys.argv", ["publish-directory"] + args):
            publish_directory.main()
