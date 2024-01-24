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

from partisan.irods import AC, AVU, DataObject, Permission
from pytest import mark as m

from conftest import history_in_meta
from npg_irods.illumina import ensure_secondary_metadata_updated
from npg_irods.metadata.lims import (
    TrackedSample,
    TrackedStudy,
    ensure_consent_withdrawn,
)


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
            AVU(TrackedSample.COMMON_NAME, "common_name1"),
            AVU(TrackedSample.DONOR_ID, "donor_id1"),
            AVU(TrackedSample.ID, "id_sample_lims1"),
            AVU(TrackedSample.NAME, "name1"),
            AVU(TrackedSample.PUBLIC_NAME, "public_name1"),
            AVU(TrackedSample.SUPPLIER_NAME, "supplier_name1"),
            AVU(TrackedStudy.ID, "4000"),
            AVU(TrackedStudy.NAME, "Study A"),
        ]

        for avu in expected_avus:
            assert avu not in obj.metadata()

        assert ensure_secondary_metadata_updated(
            obj, mlwh_session=illumina_synthetic_mlwh
        )

        for avu in expected_avus:
            assert avu in obj.metadata()

        qc_path = illumina_synthetic_irods / "12345" / "qc" / "12345#1.genotype.json"
        qc_obj = DataObject(qc_path)

        assert ensure_secondary_metadata_updated(
            qc_obj, mlwh_session=illumina_synthetic_mlwh
        )
        assert qc_obj.metadata() == [AVU(TrackedStudy.ID, "4000")]

    @m.context("When the data are not multiplexed")
    @m.context("When correct metadata are already present")
    @m.it("Leaves the metadata unchanged")
    def test_updates_present_metadata(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        zone = "testZone"
        path = illumina_synthetic_irods / "12345/12345.cram"

        obj = DataObject(path)
        expected_avus = [
            AVU(TrackedSample.COMMON_NAME, "common_name1"),
            AVU(TrackedSample.DONOR_ID, "donor_id1"),
            AVU(TrackedSample.ID, "id_sample_lims1"),
            AVU(TrackedSample.NAME, "name1"),
            AVU(TrackedSample.PUBLIC_NAME, "public_name1"),
            AVU(TrackedSample.SUPPLIER_NAME, "supplier_name1"),
            AVU(TrackedStudy.ID, "4000"),
            AVU(TrackedStudy.NAME, "Study A"),
        ]
        expected_permissions = [
            AC("irods", perm=Permission.OWN, zone=zone),
            AC("ss_4000", perm=Permission.READ, zone=zone),
        ]
        obj.add_metadata(*expected_avus)
        obj.add_permissions(*expected_permissions)

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
            AVU(TrackedSample.DONOR_ID, "donor_id1"),
            AVU(TrackedSample.ID, "id_sample_lims1"),
            AVU(TrackedSample.NAME, "name1"),
            AVU(TrackedSample.SUPPLIER_NAME, "supplier_name1"),
            AVU(TrackedStudy.ID, "4000"),
            AVU(TrackedStudy.NAME, "Study A"),
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
            AVU(TrackedSample.COMMON_NAME, "common_name1"),
            AVU(TrackedSample.DONOR_ID, "donor_id1"),
            AVU(TrackedSample.ID, "id_sample_lims1"),
            AVU(TrackedSample.NAME, "name1"),
            AVU(TrackedSample.PUBLIC_NAME, "public_name1"),
            AVU(TrackedSample.SUPPLIER_NAME, "supplier_name1"),
            AVU(TrackedStudy.ID, "4000"),
            AVU(TrackedStudy.NAME, "Study A"),
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
    @m.context("When the tag index is for a control")
    @m.it("Adds metadata while respecting the include_controls option")
    def test_updates_control_metadata_mx(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        path = illumina_synthetic_irods / "12345/12345#888.cram"

        obj = DataObject(path)
        expected_avus = [
            AVU(TrackedSample.NAME, "Phi X"),
            AVU(TrackedStudy.ID, "888"),
            AVU(TrackedStudy.NAME, "Control Study"),
        ]

        for avu in expected_avus:
            assert avu not in obj.metadata()

        assert not ensure_secondary_metadata_updated(
            obj, mlwh_session=illumina_synthetic_mlwh, include_controls=False
        )

        for avu in expected_avus:
            assert avu not in obj.metadata()

        assert ensure_secondary_metadata_updated(
            obj, mlwh_session=illumina_synthetic_mlwh, include_controls=True
        )

        for avu in expected_avus:
            assert avu in obj.metadata()

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
            AVU(TrackedSample.COMMON_NAME, "common_name1"),
            AVU(TrackedSample.COMMON_NAME, "common_name2"),
            AVU(TrackedSample.DONOR_ID, "donor_id1"),
            AVU(TrackedSample.DONOR_ID, "donor_id2"),
            AVU(TrackedSample.ID, "id_sample_lims1"),
            AVU(TrackedSample.ID, "id_sample_lims2"),
            AVU(TrackedSample.NAME, "name1"),
            AVU(TrackedSample.NAME, "name2"),
            AVU(TrackedSample.PUBLIC_NAME, "public_name1"),
            AVU(TrackedSample.PUBLIC_NAME, "public_name2"),
            AVU(TrackedSample.SUPPLIER_NAME, "supplier_name1"),
            AVU(TrackedSample.SUPPLIER_NAME, "supplier_name2"),
            AVU(TrackedStudy.ID, "4000"),
            AVU(TrackedStudy.NAME, "Study A"),
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
    @m.it(
        "Adds extra spike-in control metadata while respecting "
        "the include_controls option"
    )
    def test_updates_control_metadata_mx_tag0(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        path = illumina_synthetic_irods / "12345/12345#0.cram"

        obj = DataObject(path)
        expected_avus = [
            AVU(TrackedSample.COMMON_NAME, "common_name1"),
            AVU(TrackedSample.COMMON_NAME, "common_name2"),
            AVU(TrackedSample.DONOR_ID, "donor_id1"),
            AVU(TrackedSample.DONOR_ID, "donor_id2"),
            AVU(TrackedSample.ID, "id_sample_lims1"),
            AVU(TrackedSample.ID, "id_sample_lims2"),
            AVU(TrackedSample.NAME, "name1"),
            AVU(TrackedSample.NAME, "name2"),
            AVU(TrackedSample.PUBLIC_NAME, "public_name1"),
            AVU(TrackedSample.PUBLIC_NAME, "public_name2"),
            AVU(TrackedSample.SUPPLIER_NAME, "supplier_name1"),
            AVU(TrackedSample.SUPPLIER_NAME, "supplier_name2"),
            AVU(TrackedStudy.ID, "4000"),
            AVU(TrackedStudy.NAME, "Study A"),
        ]
        control_avus = [
            AVU(TrackedSample.NAME, "Phi X"),
            AVU(TrackedStudy.ID, "888"),
            AVU(TrackedStudy.NAME, "Control Study"),
        ]

        for avu in expected_avus:
            assert avu not in obj.metadata()

        # Not False because some changes do take effect, aside from the controls
        assert ensure_secondary_metadata_updated(
            obj, mlwh_session=illumina_synthetic_mlwh, include_controls=False
        )

        for avu in expected_avus:
            assert avu in obj.metadata()
        for avu in control_avus:
            assert avu not in obj.metadata()

        assert ensure_secondary_metadata_updated(
            obj, mlwh_session=illumina_synthetic_mlwh, include_controls=True
        )

        for avu in expected_avus + control_avus:
            assert avu in obj.metadata()


class TestIlluminaPermissionsUpdate:
    @m.context("When data are not multiplexed")
    @m.context("When the permissions are absent")
    @m.it("Adds study-specific permissions")
    def test_updates_absent_study_permissions(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        zone = "testZone"
        path = illumina_synthetic_irods / "12345/12345.cram"
        qc_path = illumina_synthetic_irods / "12345" / "qc" / "12345.genotype.json"
        old_permissions = [AC("irods", perm=Permission.OWN, zone=zone)]
        new_permissions = [
            AC("irods", perm=Permission.OWN, zone=zone),
            AC("ss_4000", perm=Permission.READ, zone=zone),
        ]

        for obj in [DataObject(path), DataObject(qc_path)]:
            assert obj.permissions() == old_permissions
            assert ensure_secondary_metadata_updated(
                obj, mlwh_session=illumina_synthetic_mlwh
            )
            assert obj.permissions() == new_permissions

    @m.context("When data are not multiplexed")
    @m.context("When the permissions are already present")
    @m.it("Leaves the permissions unchanged")
    def test_updates_present_study_permissions(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        zone = "testZone"
        path = illumina_synthetic_irods / "12345/12345.cram"
        qc_path = illumina_synthetic_irods / "12345" / "qc" / "12345.genotype.json"
        expected_metadata = [
            AVU(TrackedSample.COMMON_NAME, "common_name1"),
            AVU(TrackedSample.DONOR_ID, "donor_id1"),
            AVU(TrackedSample.ID, "id_sample_lims1"),
            AVU(TrackedSample.NAME, "name1"),
            AVU(TrackedSample.PUBLIC_NAME, "public_name1"),
            AVU(TrackedSample.SUPPLIER_NAME, "supplier_name1"),
            AVU(TrackedStudy.ID, "4000"),
            AVU(TrackedStudy.NAME, "Study A"),
        ]
        old_permissions = [
            AC("irods", perm=Permission.OWN, zone=zone),
            AC("ss_4000", perm=Permission.READ, zone=zone),
        ]

        obj = DataObject(path)
        obj.add_metadata(*expected_metadata)
        qc_obj = DataObject(qc_path)
        qc_obj.add_metadata(AVU(TrackedStudy.ID, "4000"))

        for x in [obj, qc_obj]:
            x.add_permissions(*old_permissions)
            assert not ensure_secondary_metadata_updated(
                x, mlwh_session=illumina_synthetic_mlwh
            )
            assert x.permissions() == old_permissions

    @m.context("When data are not multiplexed")
    @m.context("When incorrect permissions are present")
    @m.it("Updated the permissions")
    def test_updates_changed_study_permissions(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        zone = "testZone"
        path = illumina_synthetic_irods / "12345/12345.cram"
        qc_path = illumina_synthetic_irods / "12345" / "qc" / "12345.genotype.json"
        old_permissions = [
            AC("irods", perm=Permission.OWN, zone=zone),
            AC("ss_1000", Permission.READ, zone=zone),
        ]
        new_permissions = [
            AC("irods", perm=Permission.OWN, zone=zone),
            AC("ss_4000", perm=Permission.READ, zone=zone),
        ]

        for obj in [DataObject(path), DataObject(qc_path)]:
            obj.add_permissions(*old_permissions)
            assert ensure_secondary_metadata_updated(
                obj, mlwh_session=illumina_synthetic_mlwh
            )
            assert obj.permissions() == new_permissions

    @m.context("When data are multiplexed")
    @m.context("When data contain a human subset")
    @m.it("Update managed access permissions to restricted human access group")
    def test_updates_human_permissions_mx(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        zone = "testZone"
        path = illumina_synthetic_irods / "12345/12345#1_human.cram"
        qc_path = (
            illumina_synthetic_irods / "12345" / "qc" / "12345#1_human.genotype.json"
        )
        old_permissions = [
            AC("irods", perm=Permission.OWN, zone=zone),
            AC("ss_4000", perm=Permission.READ, zone=zone),
        ]
        new_permissions = [
            AC("irods", perm=Permission.OWN, zone=zone),
            AC("ss_4000_human", perm=Permission.READ, zone=zone),
        ]

        for obj in [DataObject(path), DataObject(qc_path)]:
            obj.add_permissions(*old_permissions)
            assert ensure_secondary_metadata_updated(
                obj, mlwh_session=illumina_synthetic_mlwh
            )
            assert obj.permissions() == new_permissions

    @m.context("When data are multiplexed")
    @m.context("When data contain a human X chromosome/autosome subset")
    @m.it("Removes managed access permissions")
    def test_updates_xahuman_permissions_mx(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        zone = "testZone"
        path = illumina_synthetic_irods / "12345/12345#1_xahuman.cram"
        qc_path = (
            illumina_synthetic_irods / "12345" / "qc" / "12345#1_xahuman.genotype.json"
        )
        old_permissions = [
            AC("irods", perm=Permission.OWN, zone=zone),
            AC("ss_4000", perm=Permission.READ, zone=zone),
        ]
        new_permissions = [AC("irods", perm=Permission.OWN, zone=zone)]

        for obj in [DataObject(path), DataObject(qc_path)]:
            obj.add_permissions(*old_permissions)
            assert ensure_secondary_metadata_updated(
                obj, mlwh_session=illumina_synthetic_mlwh
            )
            assert obj.permissions() == new_permissions

    @m.context("When data are multiplexed")
    @m.context("When data are from multiple studies")
    @m.it("Removes managed access permissions")
    def test_multiple_study_permissions_mx(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        zone = "testZone"
        path = illumina_synthetic_irods / "12345/12345#2.cram"
        qc_path = illumina_synthetic_irods / "12345" / "qc" / "12345#2.genotype.json"
        old_permissions = [
            AC("ss_4000", Permission.READ, zone=zone),
            AC("ss_5000", Permission.READ, zone=zone),
        ]
        new_permissions = [AC("irods", perm=Permission.OWN, zone=zone)]

        for obj in [DataObject(path), DataObject(qc_path)]:
            obj.add_permissions(*old_permissions)
            assert ensure_secondary_metadata_updated(
                obj, mlwh_session=illumina_synthetic_mlwh
            )
            assert obj.permissions() == new_permissions

    @m.context("When data are not multiplexed")
    @m.context("When data have had consent withdrawn")
    @m.it("Does not restore access permissions")
    def test_retains_consent_withdrawn(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        zone = "testZone"
        path = illumina_synthetic_irods / "12345/12345.cram"
        qc_path = illumina_synthetic_irods / "12345" / "qc" / "12345.genotype.json"
        old_permissions = [AC("irods", perm=Permission.OWN, zone=zone)]

        for obj in [DataObject(path), DataObject(qc_path)]:
            assert ensure_consent_withdrawn(obj)
            assert obj.permissions() == old_permissions
            assert ensure_secondary_metadata_updated(
                obj, mlwh_session=illumina_synthetic_mlwh
            )
            assert obj.permissions() == old_permissions

    @m.context("When data are multiplexed")
    @m.context("When data have had consent withdrawn")
    @m.it("Does not restore access permissions")
    def test_retains_consent_withdrawn_mx(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        zone = "testZone"
        path = illumina_synthetic_irods / "12345/12345#1_human.cram"
        qc_path = (
            illumina_synthetic_irods / "12345" / "qc" / "12345#1_human.genotype.json"
        )
        old_permissions = [AC("irods", perm=Permission.OWN, zone=zone)]

        for obj in [DataObject(path), DataObject(qc_path)]:
            assert ensure_consent_withdrawn(obj)
            assert obj.permissions() == old_permissions
            assert ensure_secondary_metadata_updated(
                obj, mlwh_session=illumina_synthetic_mlwh
            )
            assert obj.permissions() == old_permissions
