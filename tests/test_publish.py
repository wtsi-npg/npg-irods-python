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
#
from pathlib import Path
from os import getcwd
from os.path import relpath

import pytest
from partisan.irods import AC, AVU, Collection, Permission
from pytest import mark as m, MonkeyPatch

from npg_irods.exception import PublishingError
from npg_irods.publish import publish_directory


@m.describe("Publish")
class TestPublish:
    @m.context(
        "Publishing a local directory and "
        "the local directory does not exist and error handling is enabled"
    )
    @m.it("Returns the expected error count")
    def test_publish_non_existent_dir_yield(self, tmpdir, empty_collection):
        num_items, num_processed, num_errors = publish_directory(
            tmpdir / "no_such_dir", empty_collection
        )
        assert num_items == 1  # The exception
        assert num_processed == 0
        assert num_errors == 1

    @m.context(
        "When the local directory does not exist and error handling is not enabled"
    )
    @m.it("Raises a PublishingError")
    def test_publish_non_existent_dir_err(self, tmpdir, empty_collection):
        with pytest.raises(PublishingError, match="Error while publishing") as exc_info:
            publish_directory(
                tmpdir / "no_such_dir", empty_collection, handle_exceptions=False
            )

        assert exc_info.value.src == tmpdir / "no_such_dir"
        assert exc_info.value.dest == empty_collection
        assert exc_info.value.num_processed == 0
        assert exc_info.value.num_errors == 1

    @m.context("When the directory is empty")
    @m.it("Creates an empty collection in iRODS and returns the expected item counts")
    def test_publish_empty_dir(self, tmpdir, empty_collection):
        num_items, num_processed, num_errors = publish_directory(
            tmpdir, empty_collection
        )
        assert num_items == 1  # The collection itself
        assert num_processed == 1
        assert num_errors == 0

    @m.context("When the local directory is not empty")
    @m.it("Creates a collection in iRODS and returns the expected item counts")
    def test_publish_non_empty_dir(self, empty_collection):
        src = Path("./tests/data/simple/collection")
        dest = empty_collection

        num_items, num_processed, num_errors = publish_directory(src, dest)

        # The root collection plus one sub-collection containing two data objects == 4
        assert num_items == 4
        assert num_processed == 4
        assert num_errors == 0

    @m.context("When an ACL is provided")
    @m.it("Adds it to the published collections and data objects")
    def test_publish_with_acl(self, empty_collection):
        zone = "testZone"
        src = Path("./tests/data/simple/collection")
        dest = empty_collection
        acl = [AC("ss_1000", Permission.READ, zone=zone)]

        num_items, num_processed, num_errors = publish_directory(src, dest, acl=acl)
        assert num_items == 4
        assert num_processed == 4
        assert num_errors == 0

        for item in Collection(dest).iter_contents():
            perms = item.acl()
            for ac in acl:
                assert ac in perms

    @m.context("When metadata are provided")
    @m.it("Adds it to the published root collection, but not to its contents")
    def test_publish_with_metadata(self, empty_collection):
        zone = "testZone"
        src = Path("./tests/data/simple/collection")
        dest = empty_collection / "sub"
        avus = [AVU("attr1", "val1"), AVU("attr2", "val2")]

        num_items, num_processed, num_errors = publish_directory(src, dest, avus=avus)
        assert num_items == 4
        assert num_processed == 4
        assert num_errors == 0

        mc = Collection(dest).metadata()
        for avu in avus:
            assert avu in mc

        for item in Collection(dest).iter_contents():
            mi = item.metadata()
            for avu in avus:
                assert avu not in mi

    @m.context("When not handling errors and an error occurs")
    @m.it("Raises a PublishingError")
    def test_publish_with_error(self, empty_collection):
        src = Path("./tests/data/simple/collection")
        dest = empty_collection / "sub"
        avus = [AVU("attr1", "")]  # Empty value to trigger an error

        with pytest.raises(PublishingError, match="Error while publishing") as exc_info:
            publish_directory(src, dest, avus=avus, handle_exceptions=False)

        assert exc_info.value.src == src
        assert exc_info.value.dest == dest
        assert exc_info.value.num_processed == 4
        assert exc_info.value.num_errors == 1  # One error from the root collection

    @m.context("When src inside working directory and specified with relative path")
    @m.it("Passes relative paths to filter_fn")
    def test_publish_inside_working_directory_relative_src(self, empty_collection):
        src = Path("./tests/data/simple/collection")
        dest = empty_collection

        paths = []
        publish_directory(src, dest, filter_fn=lambda x: paths.append(x))

        children = ["sub", "a.txt", "sub/b.txt"]
        parent = Path("tests/data/simple/collection")
        expected_paths = [ parent / x for x in children ]
        assert paths == expected_paths, "Expected relative paths"

    @m.context("When src inside working directory and specified with absolute path")
    @m.it("Passes absolute paths to filter_fn")
    def test_publish_inside_working_directory_absolute_src(self, empty_collection):
        src = Path("./tests/data/simple/collection").absolute()
        dest = empty_collection

        paths = []
        publish_directory(src, dest, filter_fn=lambda x: paths.append(x))

        children = ["sub", "a.txt", "sub/b.txt"]
        parent = src
        expected_paths = [ parent / x for x in children ]
        assert paths == expected_paths, f"Expected absolute paths"

    @m.context("When src outside working directory and specified with relative path")
    @m.it("Passes relative paths to filter_fn")
    def test_publish_outside_working_directory(self, tmpdir, empty_collection, monkeypatch: MonkeyPatch):
        absolute_path = Path("./tests/data/simple/collection").absolute()
        monkeypatch.chdir(tmpdir)
        relative_path = Path(relpath(absolute_path, getcwd()))
        src = relative_path
        dest = empty_collection

        paths = []
        publish_directory(src, dest, filter_fn=lambda x: paths.append(x))

        children = ["sub", "a.txt", "sub/b.txt"]
        parent = relative_path
        expected_paths = [parent / x for x in children]
        breakpoint()
        assert paths == expected_paths, "Expected relative paths"

    @m.context("When src outside working directory and specified with absolute path")
    @m.it("Passes absolute paths to filter_fn")
    def test_publish_outside_working_directory_absolute_src(self, tmpdir, empty_collection, monkeypatch: MonkeyPatch):
        absolute_path = Path("./tests/data/simple/collection").absolute()
        monkeypatch.chdir(tmpdir)
        src = absolute_path
        dest = empty_collection

        paths = []
        publish_directory(src, dest, filter_fn=lambda x: paths.append(x))

        children = ["sub", "a.txt", "sub/b.txt"]
        parent = absolute_path
        expected_paths = [parent / x for x in children]
        assert paths == expected_paths, "Expected absolute paths"