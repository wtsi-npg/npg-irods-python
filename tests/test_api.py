# -*- coding: utf-8 -*-
#
# Copyright © 2022, 2023 Genome Research Ltd. All rights reserved.
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

from datetime import datetime, timezone
from unittest.mock import patch

import pytest
from partisan.irods import AVU, DataObject, Replica
from pytest import mark as m

from helpers import tests_have_admin
from npg_irods.exception import ChecksumError
from npg_irods.metadata.common import (
    CompressSuffix,
    DataFile,
    RECOGNISED_FILE_SUFFIXES,
    ensure_checksum_metadata,
    ensure_creation_metadata,
    ensure_matching_checksum_metadata,
    ensure_type_metadata,
    has_checksum_metadata,
    has_complete_checksums,
    has_creation_metadata,
    has_matching_checksum_metadata,
    has_matching_checksums,
    has_target_metadata,
    has_type_metadata,
    make_creation_metadata,
    make_type_metadata,
    parse_object_type,
    requires_type_metadata,
)
from npg_irods.metadata.lims import has_consent_withdrawn_metadata


@m.describe("Checksums and checksum metadata predicate API")
class TestChecksumPredicates:
    @m.context(
        "When a data object has full checksum coverage and "
        "the checksums for each replica are the same as each other"
    )
    @m.it("Returns True")
    def test_has_complete_checksums_same(self):
        obj = DataObject("/dummy/path.txt")
        checksum = "aaaaaaaaaa"
        replicas = [
            Replica("dummy_resource", "dummy_location", 0, checksum="aaaaaaaaaa"),
            Replica("dummy_resource", "dummy_location", 1, checksum="aaaaaaaaaa"),
        ]

        with patch.multiple(obj, checksum=lambda: checksum, replicas=lambda: replicas):
            assert has_complete_checksums(obj)

    @m.context(
        "When a data object has full checksum coverage and "
        "the checksums for each replica are different from each other"
    )
    @m.it("Returns True")
    def test_has_complete_checksums_diff(self):
        obj = DataObject("/dummy/path.txt")
        checksum = "aaaaaaaaaa"
        replicas = [
            Replica("dummy_resource", "dummy_location", 0, checksum=checksum),
            Replica("dummy_resource", "dummy_location", 1, checksum="bbbbbbbbbb"),
        ]

        with patch.multiple(obj, checksum=lambda: checksum, replicas=lambda: replicas):
            assert has_complete_checksums(obj)

    @m.context(
        "When a data object does not have full checksum coverage and "
        "the replicas not covered are valid"
    )
    @m.it("Returns False")
    def test_has_complete_checksums_valid(self):
        obj = DataObject("/dummy/path.txt")
        checksum = "aaaaaaaaaa"
        replicas = [
            Replica("dummy_resource", "dummy_location", 0, checksum="aaaaaaaaaa"),
            Replica("dummy_resource", "dummy_location", 1, checksum=None),
        ]

        with patch.multiple(obj, checksum=lambda: checksum, replicas=lambda: replicas):
            assert not has_complete_checksums(obj)

    @m.context(
        "When a data object does not have full checksum coverage and "
        "the replicas not covered are not valid"
    )
    @m.it("Returns True")
    def test_has_complete_checksum_incomplete(self):
        obj = DataObject("/dummy/path.txt")
        checksum = "aaaaaaaaaa"
        replicas = [
            Replica("dummy_resource", "dummy_location", 0, checksum=checksum),
            Replica("dummy_resource", "dummy_location", 1, checksum=None, valid=False),
        ]

        with patch.multiple(obj, checksum=lambda: checksum, replicas=lambda: replicas):
            assert has_complete_checksums(obj)

    @m.context("When a data object cannot retrieve its replica information")
    @m.it("Raises an exception")
    def test_has_complete_checksums_error(self):
        obj = DataObject("/dummy/path.txt")
        with patch.object(obj, "replicas", return_value=[]):
            with pytest.raises(ValueError):
                has_complete_checksums(obj)

    @m.context(
        "When a data object has full checksum coverage and "
        "the valid replica checksums match"
    )
    @m.it("Returns True")
    def test_has_matching_checksums_same(self):
        obj = DataObject("/dummy/path.txt")
        checksum = "aaaaaaaaaa"
        replicas = [
            Replica("dummy_resource", "dummy_location", 0, checksum=checksum),
            Replica("dummy_resource", "dummy_location", 1, checksum=checksum),
        ]

        with patch.multiple(obj, checksum=lambda: checksum, replicas=lambda: replicas):
            assert has_complete_checksums(obj)
            assert has_matching_checksums(obj)

    @m.context(
        "When a data object has full checksum coverage and "
        "the valid replica checksums do not match"
    )
    @m.it("Returns False")
    def test_has_matching_checksums_diff(self):
        obj = DataObject("/dummy/path.txt")
        checksum = "aaaaaaaaaa"
        replicas = [
            Replica("dummy_resource", "dummy_location", 0, checksum=checksum),
            Replica("dummy_resource", "dummy_location", 1, checksum="invalid_checksum"),
        ]

        with patch.multiple(obj, checksum=lambda: checksum, replicas=lambda: replicas):
            assert has_complete_checksums(obj)
            assert not has_matching_checksums(obj)

    @m.context(
        "When a data object has complete checksums and "
        "there is a single checksum in the metadata and "
        "the data object checksum matches the metadata checksum"
    )
    @m.it("Returns True")
    def test_has_matching_checksum_metadata_same(self):
        obj = DataObject("/dummy/path.txt")
        checksum = "aaaaaaaaaa"
        replicas = [
            Replica("dummy_resource", "dummy_location", 0, checksum=checksum),
            Replica("dummy_resource", "dummy_location", 1, checksum=checksum),
        ]
        metadata = [AVU(DataFile.MD5, checksum)]

        with patch.multiple(
            obj,
            checksum=lambda: checksum,
            replicas=lambda: replicas,
            metadata=lambda attr=None: metadata,
        ):
            assert has_matching_checksum_metadata(obj)

    @m.context(
        "When a data object has complete checksums and "
        "there is a single checksum in the metadata and"
        "the data object checksum does not match the metadata checksum"
    )
    @m.it("Returns False")
    def test_has_matching_checksum_metadata_diff(self):
        checksum = "aaaaaaaaaa"
        obj = DataObject("/dummy/path.txt")
        replicas = [
            Replica("dummy_resource", "dummy_location", 0, checksum=checksum),
            Replica("dummy_resource", "dummy_location", 1, checksum=checksum),
        ]
        with patch.multiple(
            obj,
            replicas=lambda: replicas,
            checksum=lambda: "bbbbbbbbbb",
            metadata=lambda attr=None: [AVU(DataFile.MD5, checksum)],
        ):
            assert not has_matching_checksums(obj)

    @m.context(
        "When a data object has complete checksums and "
        "there are no checksum metadata"
    )
    @m.it("Returns False")
    def test_has_matching_checksum_metadata_none(self):
        checksum = "aaaaaaaaaa"
        obj = DataObject("/dummy/path.txt")
        replicas = [
            Replica("dummy_resource", "dummy_location", 0, checksum=checksum),
            Replica("dummy_resource", "dummy_location", 1, checksum=checksum),
        ]

        with patch.multiple(
            obj,
            replicas=lambda: replicas,
            checksum=lambda: checksum,
            metadata=lambda attr=None: [],
        ):
            assert not has_matching_checksum_metadata(obj)

    @m.context(
        "When a data object has complete checksums and "
        "there are extra, unexpected checksum metadata"
    )
    @m.it("Returns False")
    def test_has_matching_checksum_metadata_extra(self):
        checksum = "aaaaaaaaaa"
        unexpected_checksum = "bbbbbbbbbb"
        obj = DataObject("/dummy/path.txt")
        replicas = [
            Replica("dummy_resource", "dummy_location", 0, checksum=checksum),
            Replica("dummy_resource", "dummy_location", 1, checksum=checksum),
        ]
        metadata = [AVU(DataFile.MD5, checksum), AVU(DataFile.MD5, unexpected_checksum)]

        with patch.multiple(
            obj,
            replicas=lambda: replicas,
            checksum=lambda: checksum,
            metadata=lambda attr=None: metadata,
        ):
            assert not has_matching_checksum_metadata(obj)


@m.describe("Checksums and checksum metadata update API")
class TestEnsureChecksumMetadata:
    @m.context(
        "When a data object has complete checksums and "
        "it has matching checksums and "
        "there are no existing checksum metadata"
    )
    @m.it("Adds checksum metadata and returns True")
    def test_ensure_matching_checksum_metadata_none(self, simple_data_object):
        obj = DataObject(simple_data_object)

        assert not has_matching_checksum_metadata(obj)
        assert ensure_matching_checksum_metadata(obj)
        assert has_matching_checksum_metadata(obj)

    @m.context(
        "When a data object has complete checksums and "
        "it has matching checksums and "
        "there are existing, correct checksum metadata"
    )
    @m.it("Does nothing and returns False")
    def test_ensure_matching_checksum_metadata_correct(self, simple_data_object):
        obj = DataObject(simple_data_object)
        ensure_matching_checksum_metadata(obj)

        assert has_matching_checksum_metadata(obj)
        assert not ensure_matching_checksum_metadata(obj)

    @m.context("When a data object has incomplete checksums")
    @m.it("Raises an exception")
    def test_ensure_matching_checksum_metadata_incomplete(self, simple_data_object):
        obj = DataObject(simple_data_object)
        checksum = obj.checksum()
        replicas = [
            Replica("dummy_resource", "dummy_location", 0, checksum=checksum),
            Replica("dummy_resource", "dummy_location", 1, checksum=None),
        ]
        with patch.object(obj, "replicas", return_value=replicas):
            with pytest.raises(
                ChecksumError, match="not all of its valid replicas have a checksum"
            ):
                ensure_matching_checksum_metadata(obj)

    @m.context(
        "When a data object has complete checksums and "
        "it does not have matching checksums"
    )
    @m.it("Raises an exception")
    def test_ensure_matching_checksum_metadata_mismatch(self, simple_data_object):
        obj = DataObject(simple_data_object)
        checksum = obj.checksum()
        replicas = [
            Replica("dummy_resource", "dummy_location", 0, checksum=checksum),
            Replica("dummy_resource", "dummy_location", 1, checksum="invalid_checksum"),
        ]
        with patch.object(obj, "replicas", return_value=replicas):
            with pytest.raises(
                ChecksumError, match="checksums do not match each other"
            ):
                ensure_matching_checksum_metadata(obj)

    @m.context(
        "When a data object has complete checksums and "
        "it has matching checksums and "
        "there are existing, incorrect checksum metadata"
    )
    @m.it("Corrects the metadata")
    def test_ensure_matching_checksum_metadata_incorrect(self, simple_data_object):
        obj = DataObject(simple_data_object)
        obj.add_metadata(AVU(DataFile.MD5, "invalid_checksum"))
        obj.add_metadata(AVU(DataFile.MD5, "another_invalid_checksum"))

        assert ensure_matching_checksum_metadata(obj)
        assert has_matching_checksum_metadata(obj)


@m.describe("Consent metadata predicate API")
class TestConsentMetadataPredicates:
    @m.context("When consent withdrawn metadata are present")
    @m.context("A has_ function is called")
    @m.it("Returns True")
    def test_has_consent_withdrawn_metadata_present(
        self, consent_withdrawn_gapi_data_object, consent_withdrawn_npg_data_object
    ):
        assert has_consent_withdrawn_metadata(
            DataObject(consent_withdrawn_gapi_data_object)
        )
        assert has_consent_withdrawn_metadata(
            DataObject(consent_withdrawn_npg_data_object)
        )

    @m.context("When consent withdrawn metadata are absent")
    @m.context("A has_ function is called")
    @m.it("Returns False")
    def test_has_consent_withdrawn_metadata_absent(self, simple_data_object):
        assert not has_consent_withdrawn_metadata(DataObject(simple_data_object))


@m.describe("Type metadata API")
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


@m.describe("Creation metadata API")
class TestCreationMetadataAPI:
    @m.context("When creation metadata are created")
    @m.it("Has the correct form")
    def test_make_creation_metadata(self):
        now = datetime.now(timezone.utc)
        name = "dummy"
        assert make_creation_metadata(name, now) == [
            AVU("dcterms:creator", name),
            AVU("dcterms:created", now.isoformat(timespec="seconds")),
        ]


@m.describe("Common metadata API")
class TestCommonMetadataAPI:
    @tests_have_admin
    @m.context("When common metadata are present")
    @m.context("When a has_ function is called")
    @m.it("Returns True")
    def test_has_metadata_present(self, annotated_data_object):
        obj = DataObject(annotated_data_object)

        assert has_creation_metadata(obj)
        assert has_type_metadata(obj)
        assert has_checksum_metadata(obj)

    @m.context("When common metadata are absent")
    @m.context("When a has_ function is called")
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
    @m.context("When an ensure_ function is called")
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
    @m.context("When an ensure_ function is called")
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


@m.describe("Target metadata predicate API")
class TestTargetMetadataPredicates:
    @m.context("When target metadata are present")
    @m.context("When has_ function is called")
    @m.it("Returns True")
    def test_has_metadata_present(self, annotated_data_object):
        obj = DataObject(annotated_data_object)

        obj.add_metadata(AVU(DataFile.TARGET, "1"))
        assert has_target_metadata(obj)

    @m.context("When target metadata are not present")
    @m.context("When has_ function is called")
    @m.it("Returns False")
    def test_has_metadata_absent(self, annotated_data_object):
        obj = DataObject(annotated_data_object)

        assert not has_target_metadata(obj)
