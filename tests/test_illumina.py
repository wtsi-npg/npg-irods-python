# -*- coding: utf-8 -*-
#
# Copyright © 2023 Genome Research Ltd. All rights reserved.
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

import pytest
from partisan.irods import AC, AVU, DataObject, Permission
from pytest import mark as m

from conftest import history_in_meta
from npg_irods.illumina import ensure_secondary_metadata_updated
from npg_irods.metadata.lims import TrackedSample, TrackedStudy


class TestIlluminaMetadataUpdate(object):
    @m.context("When the data are not multiplexed")
    @m.context("When the metadata are absent")
    @m.it("Adds sample-specific and study-specific metadata")
    def test_updates_absent_metadata(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        path = illumina_synthetic_irods / "12345/12345.cram"
        obj = DataObject(path)
        expected_avus = [
            AVU(TrackedStudy.ID, "4000"),
            AVU(TrackedStudy.NAME, "Study A"),
            AVU(TrackedSample.DONOR_ID, "donor1"),
            AVU(TrackedSample.ID, "sample1"),
            AVU(TrackedSample.NAME, "sample 1"),
            AVU(TrackedSample.SUPPLIER_NAME, "supplier_name1"),
        ]

        for avu in expected_avus:
            assert avu not in obj.metadata()

        assert ensure_secondary_metadata_updated(
            obj, mlwh_session=illumina_synthetic_mlwh
        )

        for avu in expected_avus:
            assert avu in obj.metadata()

    @m.context("When the data are not multiplexed")
    @m.context("When correct metadata are already present")
    @m.it("Leaves the metadata unchanged")
    def test_updates_present_metadata(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        path = illumina_synthetic_irods / "12345/12345.cram"
        zone = "testZone"
        obj = DataObject(path)
        expected_avus = [
            AVU(TrackedStudy.ID, "4000"),
            AVU(TrackedStudy.NAME, "Study A"),
            AVU(TrackedSample.DONOR_ID, "donor1"),
            AVU(TrackedSample.ID, "sample1"),
            AVU(TrackedSample.NAME, "sample 1"),
            AVU(TrackedSample.SUPPLIER_NAME, "supplier_name1"),
        ]
        obj.add_metadata(*expected_avus)

        expected_acl = [AC("ss_4000", perm=Permission.READ, zone=zone)]
        obj.add_permissions(*expected_acl)

        for avu in expected_avus:
            assert avu in obj.metadata()

        assert not ensure_secondary_metadata_updated(
            obj, mlwh_session=illumina_synthetic_mlwh
        )

        for avu in expected_avus:
            assert avu in obj.metadata()

    @m.context("When the data are not multiplexed")
    @m.context("When incorrect metadata are present")
    @m.it("Updates the metadata and adds history metadata")
    def test_updates_changed_metadata(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        path = illumina_synthetic_irods / "12345/12345.cram"
        obj = DataObject(path)
        old_avus = [AVU(TrackedSample.NAME, "sample 99"), AVU(TrackedStudy.ID, "9999")]
        obj.add_metadata(*old_avus)

        for avu in old_avus:
            assert avu in obj.metadata()

        expected_avus = [
            AVU(TrackedStudy.ID, "4000"),
            AVU(TrackedStudy.NAME, "Study A"),
            AVU(TrackedSample.DONOR_ID, "donor1"),
            AVU(TrackedSample.ID, "sample1"),
            AVU(TrackedSample.NAME, "sample 1"),
            AVU(TrackedSample.SUPPLIER_NAME, "supplier_name1"),
        ]

        assert ensure_secondary_metadata_updated(
            obj, mlwh_session=illumina_synthetic_mlwh
        )

        for avu in old_avus:
            assert avu not in obj.metadata()
            assert history_in_meta(AVU.history(avu), obj.metadata())

        for avu in expected_avus:
            assert avu in obj.metadata()

    @m.context("When the data are not multiplexed")
    @m.context("When an attribute has multiple incorrect values")
    @m.it("Groups those values in the history metadata")
    def test_updates_multiple_metadata(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        path = illumina_synthetic_irods / "12345/12345.cram"
        obj = DataObject(path)
        old_avus = [
            AVU(TrackedSample.NAME, "sample 99"),
            AVU(TrackedSample.NAME, "sample 999"),
            AVU(TrackedSample.NAME, "sample 9999"),
            AVU(TrackedSample.NAME, "sample 99999"),
        ]
        obj.add_metadata(*old_avus)

        for avu in old_avus:
            assert avu in obj.metadata()

        assert ensure_secondary_metadata_updated(
            obj, mlwh_session=illumina_synthetic_mlwh
        )

        for avu in old_avus:
            assert avu not in obj.metadata()

        history = AVU.history(*old_avus)
        assert history_in_meta(history, obj.metadata())

    @m.context("When the data are multiplexed")
    @m.context("When the metadata are absent")
    @m.it("Adds sample-specific and study-specific metadata")
    def test_updates_absent_metadata_mx(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        path = illumina_synthetic_irods / "12345/12345#1.cram"
        obj = DataObject(path)
        expected_avus = [
            AVU(TrackedStudy.ID, "4000"),
            AVU(TrackedStudy.NAME, "Study A"),
            AVU(TrackedSample.DONOR_ID, "donor1"),
            AVU(TrackedSample.ID, "sample1"),
            AVU(TrackedSample.NAME, "sample 1"),
            AVU(TrackedSample.SUPPLIER_NAME, "supplier_name1"),
        ]

        for avu in expected_avus:
            assert avu not in obj.metadata()

        assert ensure_secondary_metadata_updated(
            obj, mlwh_session=illumina_synthetic_mlwh
        )

        # The data are two plexes of a single sample (from different flowcell positions)
        # that have been merged.
        for avu in expected_avus:
            assert avu in obj.metadata()

    @m.context("When the data are multiplexed")
    @m.context(
        "When spike-in controls are excluded, but the tag index is for a control"
    )
    @m.it("Raises an exception")
    def test_updates_control_metadata_mx(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        path = illumina_synthetic_irods / "12345/12345#888.cram"
        obj = DataObject(path)

        with pytest.raises(ValueError):
            ensure_secondary_metadata_updated(
                obj, mlwh_session=illumina_synthetic_mlwh, include_controls=False
            )

    @m.context("When the data are multiplexed")
    @m.context("When the data are associated with the computationally created tag 0")
    @m.context("When the metadata are absent")
    @m.it("Adds metadata from all samples and studies in the pool")
    def test_updates_absent_metadata_mx_tag0(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        path = illumina_synthetic_irods / "12345/12345#0.cram"
        obj = DataObject(path)
        expected_avus = [
            AVU(TrackedStudy.ID, "4000"),
            AVU(TrackedStudy.NAME, "Study A"),
            AVU(TrackedSample.DONOR_ID, "donor1"),
            AVU(TrackedSample.DONOR_ID, "donor2"),
            AVU(TrackedSample.ID, "sample1"),
            AVU(TrackedSample.ID, "sample2"),
            AVU(TrackedSample.NAME, "sample 1"),
            AVU(TrackedSample.NAME, "sample 2"),
            AVU(TrackedSample.SUPPLIER_NAME, "supplier_name1"),
            AVU(TrackedSample.SUPPLIER_NAME, "supplier_name2"),
        ]

        for avu in expected_avus:
            assert avu not in obj.metadata()

        assert ensure_secondary_metadata_updated(
            obj, mlwh_session=illumina_synthetic_mlwh
        )

        for avu in expected_avus:
            assert avu in obj.metadata()

    @m.context("When the data are multiplexed")
    @m.context("When the data are associated with the computationally created tag 0")
    @m.context("When spike-in controls are requested")
    @m.it("Adds extra spike-in control metadata")
    def test_updates_control_metadata_mx_tag0(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        path = illumina_synthetic_irods / "12345/12345#0.cram"
        obj = DataObject(path)
        expected_avus = [
            AVU(TrackedStudy.ID, "4000"),
            AVU(TrackedStudy.ID, "888"),
            AVU(TrackedStudy.NAME, "Study A"),
            AVU(TrackedStudy.NAME, "Control Study"),
            AVU(TrackedSample.DONOR_ID, "donor1"),
            AVU(TrackedSample.DONOR_ID, "donor2"),
            AVU(TrackedSample.ID, "sample1"),
            AVU(TrackedSample.ID, "sample2"),
            AVU(TrackedSample.NAME, "sample 1"),
            AVU(TrackedSample.NAME, "sample 2"),
            AVU(TrackedSample.NAME, "Phi X"),
            AVU(TrackedSample.SUPPLIER_NAME, "supplier_name1"),
            AVU(TrackedSample.SUPPLIER_NAME, "supplier_name2"),
        ]

        for avu in expected_avus:
            assert avu not in obj.metadata()

        assert ensure_secondary_metadata_updated(
            obj, mlwh_session=illumina_synthetic_mlwh, include_controls=True
        )

        for avu in expected_avus:
            assert avu in obj.metadata()


class TestIlluminaPermissionsUpdate:
    @m.context("When data are not multiplexed")
    @m.context("When the permissions are absent")
    @m.it("Adds study-specific permissions")
    def test_updates_absent_study_permissions(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        path = illumina_synthetic_irods / "12345/12345.cram"
        zone = "testZone"
        obj = DataObject(path)

        assert obj.permissions() == [AC("irods", perm=Permission.OWN, zone=zone)]
        assert ensure_secondary_metadata_updated(
            obj, mlwh_session=illumina_synthetic_mlwh
        )
        assert obj.permissions() == [
            AC("irods", perm=Permission.OWN, zone=zone),
            AC("ss_4000", perm=Permission.READ, zone=zone),
        ]

    @m.context("When data are not multiplexed")
    @m.context("When the permissions are already present")
    @m.it("Leaves the permissions unchanged")
    def test_updates_present_study_permissions(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        path = illumina_synthetic_irods / "12345/12345.cram"
        zone = "testZone"
        obj = DataObject(path)

        expected_metadata = [
            AVU(TrackedStudy.ID, "4000"),
            AVU(TrackedStudy.NAME, "Study A"),
            AVU(TrackedSample.DONOR_ID, "donor1"),
            AVU(TrackedSample.ID, "sample1"),
            AVU(TrackedSample.NAME, "sample 1"),
            AVU(TrackedSample.SUPPLIER_NAME, "supplier_name1"),
        ]
        obj.add_metadata(*expected_metadata)

        expected_acl = [
            AC("irods", perm=Permission.OWN, zone=zone),
            AC("ss_4000", perm=Permission.READ, zone=zone),
        ]
        obj.add_permissions(*expected_acl)

        assert obj.permissions() == expected_acl
        assert not ensure_secondary_metadata_updated(
            obj, mlwh_session=illumina_synthetic_mlwh
        )
        assert obj.permissions() == expected_acl

    @m.context("When data are not multiplexed")
    @m.context("When incorrect permissions are present")
    @m.it("Updated the permissions")
    def test_updates_changed_study_permissions(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        path = illumina_synthetic_irods / "12345/12345.cram"
        zone = "testZone"
        obj = DataObject(path)
        obj.add_permissions(AC("ss_1000", Permission.READ, zone=zone))
        old_permissions = [
            AC("irods", perm=Permission.OWN, zone=zone),
            AC("ss_1000", Permission.READ, zone=zone),
        ]

        assert obj.permissions() == old_permissions
        assert ensure_secondary_metadata_updated(
            obj, mlwh_session=illumina_synthetic_mlwh
        )
        new_permissions = [
            AC("irods", perm=Permission.OWN, zone=zone),
            AC("ss_4000", perm=Permission.READ, zone=zone),
        ]
        assert obj.permissions() == new_permissions

    @m.context("When data are multiplexed")
    @m.context("When data contain a human subset")
    @m.it("Removes managed access permissions")
    def test_updates_human_permissions(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        path = illumina_synthetic_irods / "12345/12345#1_human.cram"
        zone = "testZone"
        obj = DataObject(path)
        expected_acl = [
            AC("irods", perm=Permission.OWN, zone=zone),
            AC("ss_4000", perm=Permission.READ, zone=zone),
        ]
        obj.add_permissions(*expected_acl)

        assert obj.permissions() == expected_acl
        assert ensure_secondary_metadata_updated(
            obj, mlwh_session=illumina_synthetic_mlwh
        )
        assert obj.permissions() == [AC("irods", perm=Permission.OWN, zone=zone)]

    @m.context("When data are multiplexed")
    @m.context("When data contain a human X chromosome/autosome subset")
    @m.it("Removes managed access permissions")
    def test_updates_xahuman_permissions(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        path = illumina_synthetic_irods / "12345/12345#1_xahuman.cram"
        zone = "testZone"
        obj = DataObject(path)
        expected_acl = [
            AC("irods", perm=Permission.OWN, zone=zone),
            AC("ss_4000", perm=Permission.READ, zone=zone),
        ]
        obj.add_permissions(*expected_acl)

        assert obj.permissions() == expected_acl
        assert ensure_secondary_metadata_updated(
            obj, mlwh_session=illumina_synthetic_mlwh
        )
        assert obj.permissions() == [AC("irods", perm=Permission.OWN, zone=zone)]

    @m.context("When data are multiplexed")
    @m.context("When data are from multiple studies")
    @m.it("Removes managed access permissions")
    def test_multiple_study_permissions(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        path = illumina_synthetic_irods / "12345/12345#2.cram"
        zone = "testZone"
        obj = DataObject(path)
        obj.add_permissions(
            AC("ss_4000", Permission.READ, zone=zone),
            AC("ss_5000", Permission.READ, zone=zone),
        )

        assert ensure_secondary_metadata_updated(
            obj, mlwh_session=illumina_synthetic_mlwh
        )
        assert obj.permissions() == [AC("irods", perm=Permission.OWN, zone=zone)]
