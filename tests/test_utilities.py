# -*- coding: utf-8 -*-
#
# Copyright © 2022 Genome Research Ltd. All rights reserved.
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
# @author Keith James <kdj@sanger.ac.uk>

import re
import subprocess
from io import StringIO
from pathlib import Path, PurePath

import partisan.irods
import pytest
from partisan.exception import RodsError
from partisan.irods import AC, AVU, Collection, DataObject, Permission
from pytest import mark as m

from npg_irods.metadata.common import ensure_common_metadata
from npg_irods.utilities import (
    check_checksums,
    copy,
    repair_checksums,
    write_safe_remove_commands,
    write_safe_remove_script,
)


@m.describe("Utilities")
class TestUtilities:
    @m.context("When data object checksums are checked")
    @m.context("When all of the data objects have checksum metadata")
    @m.it("Counts correct checksums correctly")
    def test_checked_checksums_passes(self, annotated_tree):
        obj_paths = []
        for p in Collection(annotated_tree).contents(recurse=True):
            if p.rods_type == partisan.irods.DataObject:
                ensure_common_metadata(p)
                obj_paths.append(str(p))

        with StringIO("\n".join(obj_paths)) as reader:
            with StringIO() as writer:
                num_processed, num_passed, num_errors = check_checksums(
                    reader, writer, print_pass=True
                )
                assert num_processed == len(obj_paths)
                assert num_passed == len(obj_paths)
                assert num_errors == 0

                passed_paths = writer.getvalue().split()
                assert passed_paths == obj_paths

    @m.context("When data object checksums are checked")
    @m.context("When none of the data objects have checksum metadata")
    @m.it("Counts failures correctly")
    def test_check_checksums_failures(self, annotated_tree):
        obj_paths = [
            str(p)
            for p in Collection(annotated_tree).contents(recurse=True)
            if p.rods_type == partisan.irods.DataObject
        ]

        with StringIO("\n".join(obj_paths)) as reader:
            with StringIO() as writer:
                num_processed, num_passed, num_errors = check_checksums(
                    reader, writer, print_fail=True
                )
                assert num_processed == len(obj_paths)
                assert num_passed == 0
                assert num_errors == len(obj_paths)

                failed_paths = writer.getvalue().split()
                assert failed_paths == obj_paths

    @m.context("When data object checksums are repaired")
    @m.context("When all of the data objects have checksum metadata")
    @m.it("Counts repairs correctly")
    def test_checked_checksums_all(self, annotated_tree):
        obj_paths = []
        for p in Collection(annotated_tree).contents(recurse=True):
            if p.rods_type == partisan.irods.DataObject:
                ensure_common_metadata(p)
                obj_paths.append(str(p))

        with StringIO("\n".join(obj_paths)) as reader:
            with StringIO() as writer:
                num_processed, num_repaired, num_errors = repair_checksums(
                    reader, writer, print_repair=True
                )
                assert num_processed == len(obj_paths)
                assert num_repaired == 0
                assert num_errors == 0

                repaired_paths = writer.getvalue().split()
                assert repaired_paths == []

    @m.context("When data object checksums are repaired")
    @m.context("When none of the data objects have checksum metadata")
    @m.it("Counts repairs correctly")
    def test_repair_checksums_none(self, annotated_tree):
        obj_paths = [
            str(p)
            for p in Collection(annotated_tree).contents(recurse=True)
            if p.rods_type == partisan.irods.DataObject
        ]

        with StringIO("\n".join(obj_paths)) as reader:
            with StringIO() as writer:
                num_processed, num_repaired, num_errors = repair_checksums(
                    reader, writer, print_repair=True
                )
                assert num_processed == len(obj_paths)
                assert num_repaired == len(obj_paths)
                assert num_errors == 0

                repaired_paths = writer.getvalue().split()
                assert repaired_paths == obj_paths

    @m.context("When a collection is copied")
    @m.context("When a there is no collection with that name at the destination")
    @m.it("Creates a copy within the destination collection")
    def test_copy_collection(self, simple_collection):
        x = Collection(PurePath(simple_collection, "x"))
        x.create()
        y = Collection(PurePath(simple_collection, "y"))
        y.create()

        copy(x, y)
        assert Collection(PurePath(simple_collection, "y", "x")).exists()

    @m.context("When a collection is copied")
    @m.context("When a there is already a collection with that name at the destination")
    @m.it("Raises an exception, unless exists_ok is True")
    def test_copy_collection_error(self, simple_collection):
        x = Collection(PurePath(simple_collection, "x"))
        x.create()
        y = Collection(PurePath(simple_collection, "y"))
        y.create()
        copy(x, y)

        with pytest.raises(RodsError, match="CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME"):
            copy(x, y)

        num_processed, num_copied = copy(x, y, exist_ok=True)
        assert num_processed == 1
        assert num_copied == 0

    @m.context("When a data object is copied")
    @m.context("When the destination path is an existing collection")
    @m.context("When there is no data object with that name at the destination")
    @m.it("Creates a copy within the destination collection")
    def test_copy_data_object(self, simple_collection, simple_data_object):
        x = Collection(PurePath(simple_collection, "x"))
        x.create()

        d = DataObject(simple_data_object)
        num_processed, num_copied = copy(d, x)

        assert DataObject(PurePath(x.path, simple_data_object.name)).exists()
        assert num_processed == 1
        assert num_copied == 1

    @m.context("When a data object is copied")
    @m.context("When the destination path is an existing collection")
    @m.context("When there is already a data object with that name at the destination")
    @m.it("Raises an exception, unless exists_ok is True")
    def test_copy_data_object_error(self, simple_collection, simple_data_object):
        x = Collection(PurePath(simple_collection, "x"))
        x.create()
        d = DataObject(simple_data_object)
        copy(d, x)

        with pytest.raises(RodsError, match="OVERWRITE_WITHOUT_FORCE_FLAG"):
            copy(d, x)

        num_processed, num_copied = copy(d, x, exist_ok=True)
        assert num_processed == 1
        assert num_copied == 0

    @m.context("When a tree is copied")
    @m.it("Copies all collections and objects")
    def test_copy_recurse(self, annotated_tree, simple_collection):
        src = Collection(annotated_tree)
        dest = Collection(simple_collection)

        num_processed, num_copied = copy(src, dest, recurse=True)

        expected = [
            PurePath(dest.path, "tree", p).as_posix()
            for p in [
                "./",
                "./a",
                "./a/m",
                "./a/n",
                "./a/o",
                "./b",
                "./b/p",
                "./b/q",
                "./b/r",
                "./c",
                "./c/s",
                "./c/t",
                "./c/u",
                "./x.txt",
                "./y.txt",
                "./z.txt",
                "./a/h.txt",
                "./a/i.txt",
                "./a/j.txt",
                "./a/m/w.txt",
                "./a/n/x.txt",
                "./a/o/y.txt",
                "./b/p/w.txt",
                "./b/q/x.txt",
                "./b/r/z.txt",
            ]
        ]

        observed = [str(x) for x in dest.contents(recurse=True)]
        assert observed == expected
        assert num_processed == len(expected)
        assert num_copied == len(expected)

    @m.context("When a tree with annotation is copied")
    @m.it("Copies annotation on collections and data objects")
    def test_copy_annotation_recurse(self, annotated_tree, simple_collection):
        src = Collection(annotated_tree)
        dest = Collection(simple_collection)

        copy(src, dest, avu=True, recurse=True)

        for item in dest.contents(avu=True, recurse=True):
            # The path of the source item must appear in the annotation of the dest
            # item for the test to pass
            src_path = re.sub(r"simple_collection", "annotated_tree", str(item))
            assert item.metadata() == [AVU("path", src_path)]

    @m.context("When a tree with permissions is copied")
    @m.it("Copies permissions on collections and data objects")
    def test_copy_permissions_recurse(self, annotated_tree, simple_collection):
        src = Collection(annotated_tree)
        dest = Collection(simple_collection)

        copy(src, dest, acl=True, recurse=True)

        for item in dest.contents(acl=True, recurse=True):
            assert (
                AC("ss_study_01", Permission.READ, zone="testZone")
                in item.permissions()
            )

    @m.context("When passed a hierarchy of collections and data objects")
    @m.it("Writes the expected commands")
    def test_write_safe_remove_commands(self, annotated_tree):
        with StringIO() as writer:
            write_safe_remove_commands(annotated_tree, writer)

            expected = [
                ("irm", "w.txt"),
                ("irm", "x.txt"),
                ("irm", "y.txt"),
                ("irm", "h.txt"),
                ("irm", "i.txt"),
                ("irm", "j.txt"),
                ("irm", "w.txt"),
                ("irm", "x.txt"),
                ("irm", "z.txt"),
                ("irm", "x.txt"),
                ("irm", "y.txt"),
                ("irm", "z.txt"),
                ("irmdir", "u"),
                ("irmdir", "t"),
                ("irmdir", "s"),
                ("irmdir", "c"),  # c contains t, u & s
                ("irmdir", "r"),
                ("irmdir", "q"),
                ("irmdir", "p"),
                ("irmdir", "b"),  # b contains p, q & r
                ("irmdir", "o"),
                ("irmdir", "n"),
                ("irmdir", "m"),
                ("irmdir", "a"),  # a contains m, n & o
                ("irmdir", "tree"),  # The root
            ]
            observed = []
            for line in writer.getvalue().splitlines():
                cmd, path = line.split(maxsplit=1)
                observed.append((cmd, PurePath(path).name))
            assert observed == expected

    @m.context("When a generated safe remove script is run")
    @m.it("Removes the expected collections and data objects")
    def test_write_safe_remove_script(self, tmp_path, annotated_tree):
        script = Path(tmp_path, "safe_rm.sh")
        write_safe_remove_script(script, annotated_tree)
        subprocess.run([script.as_posix()], check=True)

        assert not Collection(annotated_tree).exists()

    @m.context("When passed a hierarchy of collections and data objects")
    @m.context("When paths contain spaces and/or quotes")
    @m.it("Writes the expected commands")
    def test_write_safe_remove_commands_special(self, special_paths):
        with StringIO() as writer:
            write_safe_remove_commands(special_paths, writer)

            expected = [
                ("irm", "x.txt"),
                ("irm", "y y.txt"),
                ("irm", 'z".txt'),
                ("irm", "x.txt"),
                ("irm", "y y.txt"),
                ("irm", 'z".txt'),
                ("irm", "x.txt"),
                ("irm", "y y.txt"),
                ("irm", 'z".txt'),
                ("irmdir", 'b"b'),
                ("irmdir", "a a"),
                ("irmdir", "special"),
            ]
            observed = []
            for line in writer.getvalue().splitlines():
                cmd, path = line.split(maxsplit=1)
                # Remove the outer single quotes added by shlex.quote
                path = path.lstrip("'")
                path = path.rstrip("'")
                observed.append((cmd, PurePath(path).name))
            assert observed == expected

    @m.context("When a generated safe remove script is run")
    @m.context("When paths contain spaces and/or quotes")
    @m.it("Removes the expected collections and data objects")
    def test_write_safe_remove_script_special(self, tmp_path, special_paths):
        script = Path(tmp_path, "safe_rm.sh")
        write_safe_remove_script(script, special_paths)
        subprocess.run([script.as_posix()], check=True)

        assert not Collection(special_paths).exists()
