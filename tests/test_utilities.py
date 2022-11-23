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
from pathlib import PurePath

import pytest
from partisan.exception import RodsError
from partisan.irods import AC, AVU, Collection, DataObject, Permission
from pytest import mark as m

from npg_irods.utilities import copy


@m.describe("Utilities")
class TestUtilities:
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
