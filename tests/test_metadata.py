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

import datetime

from partisan.irods import AVU, DataObject
from pytest import mark as m

from conftest import icommands_have_admin
from npg_irods.metadata.common import (
    CompressSuffix,
    DataFile,
    RECOGNISED_FILE_SUFFIXES,
    ensure_checksum_metadata,
    ensure_creation_metadata,
    ensure_type_metadata,
    has_checksum_metadata,
    has_creation_metadata,
    has_type_metadata,
    make_creation_metadata,
    make_type_metadata,
    parse_object_type,
    requires_type_metadata,
)


@m.describe("Type metadata")
class TestTypeMetadata:
    @m.context("When a data object requires type metadata")
    @m.it("Returns True")
    def test_requires_type_metadata(self):
        for suffix in RECOGNISED_FILE_SUFFIXES:
            assert requires_type_metadata(DataObject(f"/dummy/path.{suffix}"))

    @m.context("When a data object doesn't require type metadata")
    @m.it("Returns False")
    def test_not_requires_type_metadata(self):
        suffix = "unrecognised"
        assert not requires_type_metadata(DataObject(f"/dummy/path/name.{suffix}"))

    @m.context("When type metadata are created")
    @m.it("Has the correct form")
    def test_make_type_metadata(self):
        for suffix in RECOGNISED_FILE_SUFFIXES:
            assert make_type_metadata(DataObject(f"/dummy/path.{suffix}")) == [
                AVU(DataFile.TYPE, suffix)
            ]

    @m.context("When a data object path is parsed")
    @m.it("Finds the correct suffix")
    def test_parse_object_type(self):
        for suffix in RECOGNISED_FILE_SUFFIXES:
            assert parse_object_type(DataObject(f"/dummy/path.{suffix}")) == suffix
            assert (
                parse_object_type(DataObject(f"/dummy/path.{suffix}.other")) == "other"
            )
            for cs in CompressSuffix:
                assert (
                    parse_object_type(DataObject(f"/dummy/path.other.{cs}")) == "other"
                )


@m.describe("Creation metadata")
class TestCreationMetadata:
    @m.context("When creation metadata are created")
    @m.it("Has the correct form")
    def test_make_creation_metadata(self):
        now = datetime.datetime.utcnow()
        name = "dummy"
        assert make_creation_metadata(name, now) == [
            AVU("dcterms:creator", name),
            AVU("dcterms:created", now.isoformat(timespec="seconds")),
        ]


@m.describe("Common metadata")
class TestCommonMetadata:
    @icommands_have_admin
    @m.context("When common metadata are present")
    @m.context("A has_ function is called")
    @m.it("Returns True")
    def test_has_metadata_present(self, annotated_data_object):
        obj = DataObject(annotated_data_object)

        assert has_creation_metadata(obj)
        assert has_type_metadata(obj)
        assert has_checksum_metadata(obj)

    @m.context("When common metadata are absent")
    @m.context("A has_ function is called")
    @m.it("Returns False")
    def test_has_metadata_absent(self, annotated_data_object):
        obj = DataObject(annotated_data_object)

        obj.remove_metadata(AVU("dcterms:creator", "dummy creator"))
        assert not has_creation_metadata(obj)

        obj.remove_metadata(AVU(DataFile.TYPE, "txt"))
        assert not has_type_metadata(obj)

        obj.remove_metadata(AVU(DataFile.MD5, "39a4aa291ca849d601e4e5b8ed627a04"))
        assert not has_checksum_metadata(obj)

    @m.context("When common metadata are present")
    @m.context("An ensure_ function is called")
    @m.it("Returns False")
    def test_ensure_metadata_present(self, annotated_data_object):
        obj = DataObject(annotated_data_object)

        assert has_creation_metadata(obj)
        assert not ensure_creation_metadata(obj, creator="dummy creator")

        assert has_type_metadata(obj)
        assert not ensure_type_metadata(obj)

        assert has_checksum_metadata(obj)
        assert not ensure_checksum_metadata(obj)

    @m.context("When common metadata are absent")
    @m.context("An ensure_ function is called")
    @m.it("Adds absent metadata and returns True")
    def test_ensure_metadata_absent(self, annotated_data_object):
        obj = DataObject(annotated_data_object)

        obj.remove_metadata(AVU("dcterms:creator", "dummy creator"))
        assert not has_creation_metadata(obj)
        assert ensure_creation_metadata(obj, creator="dummy creator")
        assert has_creation_metadata(obj)

        obj.remove_metadata(AVU(DataFile.TYPE, "txt"))
        assert not has_type_metadata(obj)
        assert ensure_type_metadata(obj)
        assert has_type_metadata(obj)

        obj.remove_metadata(AVU(DataFile.MD5, "39a4aa291ca849d601e4e5b8ed627a04"))
        assert not has_checksum_metadata(obj)
        assert ensure_checksum_metadata(obj)
        assert has_checksum_metadata(obj)
