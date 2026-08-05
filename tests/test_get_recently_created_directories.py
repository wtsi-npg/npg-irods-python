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

from datetime import datetime, UTC, timedelta
from io import StringIO
from os import PathLike

from pathlib import Path
from typing import AnyStr

from unittest.mock import patch, Mock

from pytest import LogCaptureFixture, CaptureFixture
from pytest import mark as m

from npg_irods.cli import get_recently_created_directories

FIRST_MONDAY_3AM = datetime(2024, 1, 1, 3, 0, 0, tzinfo=UTC)
FIRST_SUNDAY_3AM = datetime(2024, 1, 7, 3, 0, 0, tzinfo=UTC)
SECOND_MONDAY_3AM = datetime(2024, 1, 8, 3, 0, 0, tzinfo=UTC)
SECOND_SUNDAY_2AM = datetime(2024, 1, 14, 2, 0, 0, tzinfo=UTC)
SECOND_SUNDAY_2_30AM = datetime(2024, 1, 14, 2, 30, 0, tzinfo=UTC)
SECOND_SUNDAY_3AM = datetime(2024, 1, 14, 3, 0, 0, tzinfo=UTC)


class FakeFilesystem:
    """Provides ability to mock ctime."""

    def __init__(self, root: Path, mock_get_ctime):
        self.root = root
        mock_get_ctime.side_effect = lambda path: self._get_ctime(path)
        self.ctimes = {}

    def create_directory(
        self,
        path: AnyStr | PathLike,
        ctime: datetime | None = None,
        file_ctimes: list[datetime] = None,
    ) -> Path:
        file_ctimes = file_ctimes or []

        directory_path = self.root / path
        directory_path.mkdir(parents=False, exist_ok=False)

        if ctime:
            self.ctimes[directory_path] = ctime.timestamp()

        for i, file_ctime in enumerate(file_ctimes):
            self.create_file(directory_path / f"{i}.txt", ctime=file_ctime)

        return directory_path

    def create_file(
        self, path: AnyStr | PathLike, ctime: datetime | None = None
    ) -> Path:
        file_path = self.root / path
        file_path.touch(exist_ok=False)

        if ctime:
            self.ctimes[file_path] = ctime.timestamp()

        return file_path

    def _get_ctime(self, path):
        ctime = self.ctimes.get(path)
        assert ctime is not None, f"Expected ctime to have been setup on path {path}"
        return ctime


@m.describe("get-recently-created-directories (script)")
class TestGetRecentlyCreatedDirectoriesScript:

    @m.context("When called with default arguments")
    @m.it(
        "Recent creation period should default to the previous 7 days excluding last 1 hour"
    )
    @m.it(
        "Max creation period should default to 6 hours from earliest to latest creation"
    )
    @patch(
        "npg_irods.cli.get_recently_created_directories.get_recently_created_directories",
        autospec=True,
    )
    @patch("npg_irods.cli.get_recently_created_directories.get_now_utc")
    def test_main_normal_case_defaults(
        self,
        mock_get_now_utc: Mock,
        mock_get_recently_created_directories: Mock,
        tmp_path: Path,
        caplog: LogCaptureFixture,
    ):
        # Arrange
        mock_get_now_utc.return_value = SECOND_SUNDAY_3AM

        input_path = tmp_path / "input.txt"
        input_path.write_text("a\nb")

        mock_get_recently_created_directories.return_value = (2, 1, 0)

        # Act
        with caplog.at_level("DEBUG"):
            self._main(["--input", str(input_path)])

        # Assert
        mock_get_recently_created_directories.assert_called_once()
        args = mock_get_recently_created_directories.call_args.kwargs
        assert args["begin"] == FIRST_SUNDAY_3AM
        assert args["end"] == SECOND_SUNDAY_2AM
        assert args["max_creation_period"] == timedelta(hours=6)

        assert "Got recently created directories" in caplog.text
        assert "num_dirs=2" in caplog.text
        assert "num_recent=1" in caplog.text
        assert "num_errors=0" in caplog.text

    @m.it("--{begin,end}-date should accept ISO8601 UTC dates times")
    @m.it("and --max-creation-period should accept period in hours")
    @patch(
        "npg_irods.cli.get_recently_created_directories.get_recently_created_directories",
        autospec=True,
    )
    @patch("npg_irods.cli.get_recently_created_directories.get_now_utc")
    def test_main_normal_case_args(
        self,
        mock_get_now_utc: Mock,
        mock_get_recently_created_directories: Mock,
        tmp_path: Path,
        caplog: LogCaptureFixture,
    ):
        # Arrange
        mock_get_now_utc.return_value = SECOND_SUNDAY_3AM

        input_path = tmp_path / "input.txt"
        input_path.write_text("a\nb")

        mock_get_recently_created_directories.return_value = (2, 1, 0)

        # Act
        with caplog.at_level("DEBUG"):
            self._main(
                [
                    "--input",
                    str(input_path),
                    "--begin-date",
                    "2024-01-07T03:00:01Z",
                    "--end-date",
                    "2024-01-14T02:00:01Z",
                    "--max-creation-period",
                    "7",
                ]
            )

        # Assert
        mock_get_recently_created_directories.assert_called_once()
        args = mock_get_recently_created_directories.call_args.kwargs
        assert args["begin"] == FIRST_SUNDAY_3AM + timedelta(seconds=1)
        assert args["end"] == SECOND_SUNDAY_2AM + timedelta(seconds=1)
        assert args["max_creation_period"] == timedelta(hours=6 + 1)

    @staticmethod
    def _main(args: list[str]):
        with patch("sys.argv", ["get-recently-created-directories"] + args):
            get_recently_created_directories.main()


@m.describe("get_recently_created_directories (function)")
class TestGetRecentlyCreatedDirectories:

    @m.context("When all files in a directory were created recently")
    @m.it("Should include")
    @patch("npg_irods.cli.get_recently_created_directories.get_ctime")
    @patch("npg_irods.cli.get_recently_created_directories.get_now_utc")
    def test_normal(
        self,
        mock_get_now_utc: Mock,
        mock_get_ctime: Mock,
        tmp_path: Path,
        caplog: LogCaptureFixture,
    ):
        # Arrange
        fs = FakeFilesystem(tmp_path, mock_get_ctime)
        mock_get_now_utc.return_value = SECOND_SUNDAY_3AM

        recent = fs.create_directory("recent", file_ctimes=[SECOND_MONDAY_3AM])
        not_recent = fs.create_directory("not_recent", file_ctimes=[FIRST_MONDAY_3AM])
        directories = [str(recent), str(not_recent)]

        # Act
        with caplog.at_level("DEBUG"):
            with StringIO("\n".join(directories)) as reader:
                with StringIO() as writer:
                    num_dirs, num_recent, num_errors = (
                        get_recently_created_directories.get_recently_created_directories(
                            reader,
                            writer,
                            begin=FIRST_SUNDAY_3AM,
                            end=SECOND_SUNDAY_3AM - timedelta(hours=1),
                            max_creation_period=timedelta(hours=6),
                        )
                    )
                    recent_paths = writer.getvalue().split()

        # Assert
        assert recent_paths == [str(recent)]

        assert num_dirs == 2
        assert num_recent == 1
        assert num_errors == 0

    @m.context("When a directory is being created")
    @m.it("Should not include and should log a warning")
    @patch("npg_irods.cli.get_recently_created_directories.get_ctime")
    @patch("npg_irods.cli.get_recently_created_directories.get_now_utc")
    def test_being_created(
        self,
        mock_get_now_utc: Mock,
        mock_get_ctime: Mock,
        tmp_path: Path,
        caplog: LogCaptureFixture,
    ):
        # Arrange
        fs = FakeFilesystem(tmp_path, mock_get_ctime)
        mock_get_now_utc.return_value = SECOND_SUNDAY_3AM

        recent = fs.create_directory("recent", file_ctimes=[SECOND_MONDAY_3AM])
        being_created = fs.create_directory(
            "being_created", file_ctimes=[SECOND_SUNDAY_2_30AM]
        )
        directories = [str(recent), str(being_created)]

        # Act
        with caplog.at_level("DEBUG"):
            with StringIO("\n".join(directories)) as reader:
                with StringIO() as writer:
                    num_dirs, num_recent, num_errors = (
                        get_recently_created_directories.get_recently_created_directories(
                            reader,
                            writer,
                            begin=FIRST_SUNDAY_3AM,
                            end=SECOND_SUNDAY_3AM - timedelta(hours=1),
                            max_creation_period=timedelta(hours=6),
                        )
                    )
                    recent_paths = writer.getvalue().split()

        # Assert
        assert recent_paths == [str(recent)]

        assert num_dirs == 2
        assert num_recent == 1
        assert num_errors == 0

        assert "too new" in caplog.text

    @m.context("When a directory contains a later change")
    @m.it("Should not include and should log a warning")
    @patch("npg_irods.cli.get_recently_created_directories.get_ctime")
    @patch("npg_irods.cli.get_recently_created_directories.get_now_utc")
    def test_later_change(
        self,
        mock_get_now_utc: Mock,
        mock_get_ctime: Mock,
        tmp_path: Path,
        caplog: LogCaptureFixture,
    ):
        # Arrange
        fs = FakeFilesystem(tmp_path, mock_get_ctime)

        mock_get_now_utc.return_value = SECOND_SUNDAY_3AM

        recent = fs.create_directory("recent", file_ctimes=[SECOND_MONDAY_3AM])
        later_change = fs.create_directory(
            "later_change", file_ctimes=[FIRST_MONDAY_3AM, SECOND_MONDAY_3AM]
        )
        directories = [str(recent), str(later_change)]

        # Act
        with caplog.at_level("DEBUG"):
            with StringIO("\n".join(directories)) as reader:
                with StringIO() as writer:
                    num_dirs, num_recent, num_errors = (
                        get_recently_created_directories.get_recently_created_directories(
                            reader,
                            writer,
                            begin=FIRST_SUNDAY_3AM,
                            end=SECOND_SUNDAY_3AM - timedelta(hours=1),
                            max_creation_period=timedelta(hours=6),
                        )
                    )
                    recent_paths = writer.getvalue().split()

        # Assert
        assert recent_paths == [str(recent)]

        assert num_dirs == 2
        assert num_recent == 1
        assert num_errors == 1

        assert "Unexpected later change to file" in caplog.text

    @m.context("When a directory is empty")
    @m.it("Should not include and should log a warning")
    @patch("npg_irods.cli.get_recently_created_directories.get_ctime")
    @patch("npg_irods.cli.get_recently_created_directories.get_now_utc")
    def test_empty(
        self,
        mock_get_now_utc: Mock,
        mock_get_ctime: Mock,
        tmp_path: Path,
        caplog: LogCaptureFixture,
    ):
        # Arrange
        fs = FakeFilesystem(tmp_path, mock_get_ctime)

        mock_get_now_utc.return_value = SECOND_SUNDAY_3AM

        recent = fs.create_directory("recent", file_ctimes=[SECOND_MONDAY_3AM])
        empty = fs.create_directory("empty")
        directories = [str(recent), str(empty)]

        # Act
        with caplog.at_level("DEBUG"):
            with StringIO("\n".join(directories)) as reader:
                with StringIO() as writer:
                    num_dirs, num_recent, num_errors = (
                        get_recently_created_directories.get_recently_created_directories(
                            reader,
                            writer,
                            begin=FIRST_SUNDAY_3AM,
                            end=SECOND_SUNDAY_3AM - timedelta(hours=1),
                            max_creation_period=timedelta(hours=6),
                        )
                    )
                    recent_paths = writer.getvalue().split()

        # Assert
        assert recent_paths == [str(recent)]

        assert num_dirs == 2
        assert num_recent == 1
        assert num_errors == 1

        assert "Unexpected empty directory" in caplog.text

    @m.it(
        "Should excludes checksums (.md5) and macOS Finder metadata (.DS_Store) files"
    )
    @patch("npg_irods.cli.get_recently_created_directories.get_ctime")
    @patch("npg_irods.cli.get_recently_created_directories.get_now_utc")
    def test_ignore(
        self,
        mock_get_now_utc: Mock,
        mock_get_ctime: Mock,
        tmp_path: Path,
        caplog: LogCaptureFixture,
    ):
        # Arrange
        fs = FakeFilesystem(tmp_path, mock_get_ctime)

        mock_get_now_utc.return_value = SECOND_SUNDAY_3AM

        recent = fs.create_directory("recent", file_ctimes=[SECOND_MONDAY_3AM])
        # Should be ignored and folder not be categorised as being created
        fs.create_file("recent/checksum.md5", SECOND_SUNDAY_3AM)
        fs.create_file("recent/.DS_Store", SECOND_SUNDAY_3AM)
        directories = [str(recent)]

        # Act
        with caplog.at_level("DEBUG"):
            with StringIO("\n".join(directories)) as reader:
                with StringIO() as writer:
                    num_dirs, num_recent, num_errors = (
                        get_recently_created_directories.get_recently_created_directories(
                            reader,
                            writer,
                            begin=FIRST_SUNDAY_3AM,
                            end=SECOND_SUNDAY_3AM - timedelta(hours=1),
                            max_creation_period=timedelta(hours=6),
                        )
                    )
                    recent_paths = writer.getvalue().split()

        # Assert
        assert recent_paths == [str(recent)]

        assert num_dirs == 1
        assert num_recent == 1
        assert num_errors == 0
