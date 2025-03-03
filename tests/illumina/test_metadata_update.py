# -*- coding: utf-8 -*-
#
# Copyright © 2023, 2024 Genome Research Ltd. All rights reserved.
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

from pathlib import Path
import subprocess
from partisan.irods import AC, AVU, DataObject, Permission
from pytest import mark as m

from helpers import history_in_meta
from npg_irods.illumina import Component, ensure_secondary_metadata_updated, split_name
from npg_irods.metadata.common import SeqConcept, SeqSubset
from npg_irods.metadata.lims import (
    TrackedSample,
    TrackedStudy,
    ensure_consent_withdrawn,
    make_public_read_acl,
)
from npg_irods.cli.update_uuid_lims_metadata import (
    Status,
    add_lims_uuid_to_iRODS_object,
)

from npg_irods.db.mlwh import session_context


class TestIlluminaAPI:
    @m.context("When a component AVU is available")
    @m.it("Can be used to construct a Component")
    def test_make_component_from_avu(self):
        c = Component.from_avu(
            AVU(
                SeqConcept.COMPONENT,
                '{"id_run":12345, "position":1}',
            )
        )
        assert c.id_run == 12345
        assert c.position == 1
        assert c.tag_index is None
        assert c.subset is None

        c = Component.from_avu(
            AVU(
                SeqConcept.COMPONENT,
                '{"id_run":12345, "position":1, "tag_index":1, "subset":"human"}',
            )
        )
        assert c.id_run == 12345
        assert c.position == 1
        assert c.tag_index == 1
        assert c.subset == SeqSubset.HUMAN

    @m.context("When parsing names of Illumina data objects")
    @m.it("Can split the name into a prefix and a suffix")
    def test_split_name(self):
        for base in [
            "12345",  # expt, not multiplexed
            "12345_phix",  # control, not multiplexed
            "12345#1",  # expt, multiplexed
            "12345_phix#1",  # control, multiplexed
            "12345_1#1",  # expt with lane, multiplexed
            "12345_phix_1#1",  # control with lane, multiplexed
        ]:
            assert split_name(f"{base}.cram") == (base, ".cram")
            assert split_name(f"{base}.cram.crai") == (base, ".cram.crai")
            assert split_name(f"{base}_F0x900.stats") == (base, ".F0x900.stats")
            assert split_name(f"{base}_F0xB00.stats") == (base, ".F0xB00.stats")
            assert split_name(f"{base}_F0xF04_target.stats") == (
                base,
                ".F0xF04_target.stats",
            )
            assert split_name(f"{base}_F0xF04_target_autosome.stats") == (
                base,
                ".F0xF04_target_autosome.stats",
            )
            assert split_name(f"{base}_F0xB00.samtools_stats.json") == (
                base,
                ".F0xB00.samtools_stats.json",
            )
            assert split_name(f"{base}_quality_cycle_caltable.txt") == (
                base,
                ".quality_cycle_caltable.txt",
            )
            assert split_name(f"{base}_quality_cycle_surv.txt") == (
                base,
                ".quality_cycle_surv.txt",
            )
            assert split_name(f"{base}_quality_error.txt") == (
                base,
                ".quality_error.txt",
            )
            assert split_name(f"{base}.vcf") == (
                base,
                ".vcf",
            )

    @m.context("When parsing names of library-merged Illumina data objects")
    @m.it("Can split the name into a prefix and a suffix")
    def test_split_name_library_merge(self):
        assert split_name("9930555.ACXX.paired158.550b751b96.cram") == (
            "9930555.ACXX.paired158.550b751b96",
            ".cram",
        )
        assert split_name("9930555.ACXX.paired158.550b751b96.cram.crai") == (
            "9930555.ACXX.paired158.550b751b96",
            ".cram.crai",
        )
        assert split_name("9930555.ACXX.paired158.550b751b96_F0x900.stats") == (
            "9930555.ACXX.paired158.550b751b96",
            ".F0x900.stats",
        )
        assert split_name("9930555.ACXX.paired158.550b751b96_F0xB00.stats") == (
            "9930555.ACXX.paired158.550b751b96",
            ".F0xB00.stats",
        )
        assert split_name("9930555.ACXX.paired158.550b751b96_F0xF04_target.stats") == (
            "9930555.ACXX.paired158.550b751b96",
            ".F0xF04_target.stats",
        )
        assert split_name("9930555.ACXX.paired158.550b751b96.flagstat") == (
            "9930555.ACXX.paired158.550b751b96",
            ".flagstat",
        )
        assert split_name("9930555.ACXX.paired158.550b751b96.g.vcf.gz") == (
            "9930555.ACXX.paired158.550b751b96",
            ".g.vcf.gz",
        )


class TestIlluminaMetadataUpdate:
    @m.context("When the data are not multiplexed")
    @m.context("When the metadata are absent")
    @m.it("Adds sample-specific and study-specific metadata")
    def test_updates_absent_metadata(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        path = illumina_synthetic_irods / "12345" / "12345.cram"

        obj = DataObject(path)
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

        for avu in expected_metadata:
            assert avu not in obj.metadata()

        assert ensure_secondary_metadata_updated(
            obj, mlwh_session=illumina_synthetic_mlwh
        )

        for avu in expected_metadata:
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
        path = illumina_synthetic_irods / "12345" / "12345.cram"

        obj = DataObject(path)
        expected_metadata = [
            AVU(TrackedSample.COMMON_NAME, "common_name1"),
            AVU(TrackedSample.DONOR_ID, "donor_id1"),
            AVU(TrackedSample.ID, "id_sample_lims1"),
            AVU(TrackedSample.LIMS, "LIMS_01"),
            AVU(TrackedSample.NAME, "name1"),
            AVU(TrackedSample.PUBLIC_NAME, "public_name1"),
            AVU(TrackedSample.SUPPLIER_NAME, "supplier_name1"),
            AVU(TrackedSample.UUID, "52429892-0ab6-11ee-b5ba-fa163eac3af1"),
            AVU(TrackedStudy.ID, "4000"),
            AVU(TrackedStudy.NAME, "Study A"),
        ]
        expected_permissions = [
            AC("irods", perm=Permission.OWN, zone=zone),
            AC("ss_4000", perm=Permission.READ, zone=zone),
        ]
        obj.add_metadata(*expected_metadata)
        obj.add_permissions(*expected_permissions)

        for avu in expected_metadata:
            assert avu in obj.metadata()

        assert not ensure_secondary_metadata_updated(
            obj, mlwh_session=illumina_synthetic_mlwh
        )

        for avu in expected_metadata:
            assert avu in obj.metadata()

    @m.context("When the data are not multiplexed")
    @m.context("When incorrect metadata are present")
    @m.it("Updates the metadata and adds history metadata")
    def test_updates_changed_metadata(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        path = illumina_synthetic_irods / "12345" / "12345.cram"

        obj = DataObject(path)
        old_avus = [AVU(TrackedSample.NAME, "sample 99"), AVU(TrackedStudy.ID, "9999")]
        obj.add_metadata(*old_avus)

        for avu in old_avus:
            assert avu in obj.metadata()

        expected_metadata = [
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

        for avu in expected_metadata:
            assert avu in obj.metadata()

    @m.context("When the data are not multiplexed")
    @m.context("When an attribute has multiple incorrect values")
    @m.it("Groups those values in the history metadata")
    def test_updates_multiple_metadata(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        path = illumina_synthetic_irods / "12345" / "12345.cram"

        obj = DataObject(path)
        old_metadata = [
            AVU(TrackedSample.NAME, "sample 99"),
            AVU(TrackedSample.NAME, "sample 999"),
            AVU(TrackedSample.NAME, "sample 9999"),
            AVU(TrackedSample.NAME, "sample 99999"),
        ]
        obj.add_metadata(*old_metadata)

        for avu in old_metadata:
            assert avu in obj.metadata()

        assert ensure_secondary_metadata_updated(
            obj, mlwh_session=illumina_synthetic_mlwh
        )

        for avu in old_metadata:
            assert avu not in obj.metadata()

        history = AVU.history(*old_metadata)
        assert history_in_meta(history, obj.metadata())

    @m.context("When the data are multiplexed")
    @m.context("When the metadata are absent")
    @m.it("Adds sample-specific and study-specific metadata")
    def test_updates_absent_metadata_mx(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        path = illumina_synthetic_irods / "12345/" / "12345#1.cram"

        obj = DataObject(path)
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

        for avu in expected_metadata:
            assert avu not in obj.metadata()

        assert ensure_secondary_metadata_updated(
            obj, mlwh_session=illumina_synthetic_mlwh
        )

        # The data are two plexes of a single sample (from different flowcell positions)
        # that have been merged.
        for avu in expected_metadata:
            assert avu in obj.metadata()

    @m.context("When the data are multiplexed")
    @m.context("When the tag index is for a control")
    @m.it("Adds metadata while respecting the include_controls option")
    def test_updates_control_metadata_mx(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        path = illumina_synthetic_irods / "12345" / "12345#888.cram"

        obj = DataObject(path)
        expected_metadata = [
            AVU(TrackedSample.NAME, "Phi X"),
            AVU(TrackedStudy.ID, "888"),
            AVU(TrackedStudy.NAME, "Control Study"),
        ]

        for avu in expected_metadata:
            assert avu not in obj.metadata()

        assert not ensure_secondary_metadata_updated(
            obj, mlwh_session=illumina_synthetic_mlwh, include_controls=False
        )

        for avu in expected_metadata:
            assert avu not in obj.metadata()

        assert ensure_secondary_metadata_updated(
            obj, mlwh_session=illumina_synthetic_mlwh, include_controls=True
        )

        for avu in expected_metadata:
            assert avu in obj.metadata()

    @m.context("When the data are multiplexed")
    @m.context("When the data are associated with the computationally created tag 0")
    @m.context("When the metadata are absent")
    @m.it("Adds metadata from all samples and studies in the pool")
    def test_updates_absent_metadata_mx_tag0(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        path = illumina_synthetic_irods / "12345" / "12345#0.cram"

        obj = DataObject(path)
        expected_metadata = [
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

        for avu in expected_metadata:
            assert avu not in obj.metadata()

        assert ensure_secondary_metadata_updated(
            obj, mlwh_session=illumina_synthetic_mlwh
        )

        for avu in expected_metadata:
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
        path = illumina_synthetic_irods / "12345" / "12345#0.cram"

        obj = DataObject(path)
        expected_metadata = [
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

        for avu in expected_metadata:
            assert avu not in obj.metadata()

        # Not False because some changes do take effect, aside from the controls
        assert ensure_secondary_metadata_updated(
            obj, mlwh_session=illumina_synthetic_mlwh, include_controls=False
        )

        for avu in expected_metadata:
            assert avu in obj.metadata()
        for avu in control_avus:
            assert avu not in obj.metadata()

        assert ensure_secondary_metadata_updated(
            obj, mlwh_session=illumina_synthetic_mlwh, include_controls=True
        )

        for avu in expected_metadata + control_avus:
            assert avu in obj.metadata()


class TestIlluminaPermissionsUpdate:
    @m.context("When data are not multiplexed")
    @m.context("When the permissions are absent")
    @m.it("Adds study-specific permissions")
    def test_updates_absent_study_permissions(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        zone = "testZone"
        path = illumina_synthetic_irods / "12345" / "12345.cram"
        anc_path = illumina_synthetic_irods / "12345" / "12345.vcf"
        qc_path = illumina_synthetic_irods / "12345" / "qc" / "12345.genotype.json"
        old_permissions = [AC("irods", perm=Permission.OWN, zone=zone)]
        new_permissions = [
            AC("irods", perm=Permission.OWN, zone=zone),
            AC("ss_4000", perm=Permission.READ, zone=zone),
        ]

        for obj in [DataObject(path), DataObject(anc_path), DataObject(qc_path)]:
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
        path = illumina_synthetic_irods / "12345" / "12345.cram"
        anc_path = illumina_synthetic_irods / "12345" / "12345.vcf"
        qc_path = illumina_synthetic_irods / "12345" / "qc" / "12345.genotype.json"
        old_metadata = [
            AVU(TrackedSample.COMMON_NAME, "common_name1"),
            AVU(TrackedSample.DONOR_ID, "donor_id1"),
            AVU(TrackedSample.ID, "id_sample_lims1"),
            AVU(TrackedSample.LIMS, "LIMS_01"),
            AVU(TrackedSample.NAME, "name1"),
            AVU(TrackedSample.PUBLIC_NAME, "public_name1"),
            AVU(TrackedSample.SUPPLIER_NAME, "supplier_name1"),
            AVU(TrackedSample.UUID, "52429892-0ab6-11ee-b5ba-fa163eac3af1"),
            AVU(TrackedStudy.ID, "4000"),
            AVU(TrackedStudy.NAME, "Study A"),
        ]
        old_permissions = [
            AC("irods", perm=Permission.OWN, zone=zone),
            AC("ss_4000", perm=Permission.READ, zone=zone),
        ]

        obj = DataObject(path)
        obj.add_metadata(*old_metadata)

        other = [DataObject(anc_path), DataObject(qc_path)]
        for o in other:
            o.add_metadata(AVU(TrackedStudy.ID, "4000"))

        for x in [obj, *other]:
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
        path = illumina_synthetic_irods / "12345" / "12345.cram"
        anc_path = illumina_synthetic_irods / "12345" / "12345.vcf"
        qc_path = illumina_synthetic_irods / "12345" / "qc" / "12345.genotype.json"
        old_permissions = [
            AC("irods", perm=Permission.OWN, zone=zone),
            AC("ss_1000", Permission.READ, zone=zone),
        ]
        new_permissions = [
            AC("irods", perm=Permission.OWN, zone=zone),
            AC("ss_4000", perm=Permission.READ, zone=zone),
        ]

        for obj in [DataObject(path), DataObject(anc_path), DataObject(qc_path)]:
            obj.add_permissions(*old_permissions)
            assert ensure_secondary_metadata_updated(
                obj, mlwh_session=illumina_synthetic_mlwh
            )
            assert obj.permissions() == new_permissions

    @m.context("When data are multiplexed")
    @m.context("When data contain a human subset")
    @m.it("Updates managed access permissions to restricted human access group")
    def test_updates_human_permissions_mx(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        zone = "testZone"
        path = illumina_synthetic_irods / "12345" / "12345#1_human.cram"
        anc_path = illumina_synthetic_irods / "12345" / "12345#1_human.vcf"
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

        for obj in [DataObject(path), DataObject(anc_path), DataObject(qc_path)]:
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
        path = illumina_synthetic_irods / "12345" / "12345#1_xahuman.cram"
        anc_path = illumina_synthetic_irods / "12345" / "12345#1_xahuman.vcf"
        qc_path = (
            illumina_synthetic_irods / "12345" / "qc" / "12345#1_xahuman.genotype.json"
        )
        old_permissions = [
            AC("irods", perm=Permission.OWN, zone=zone),
            AC("ss_4000", perm=Permission.READ, zone=zone),
        ]
        new_permissions = [AC("irods", perm=Permission.OWN, zone=zone)]

        for obj in [DataObject(path), DataObject(anc_path), DataObject(qc_path)]:
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
        path = illumina_synthetic_irods / "12345" / "12345#2.cram"
        anc_path = illumina_synthetic_irods / "12345" / "12345#2.vcf"
        qc_path = illumina_synthetic_irods / "12345" / "qc" / "12345#2.genotype.json"
        old_permissions = [
            AC("ss_4000", Permission.READ, zone=zone),
            AC("ss_5000", Permission.READ, zone=zone),
        ]
        new_permissions = [AC("irods", perm=Permission.OWN, zone=zone)]

        for obj in [DataObject(path), DataObject(anc_path), DataObject(qc_path)]:
            obj.add_permissions(*old_permissions)
            assert ensure_secondary_metadata_updated(
                obj, mlwh_session=illumina_synthetic_mlwh
            )
            assert obj.permissions() == new_permissions

    @m.context("When the data are multiplexed")
    @m.context("When the tag index is for a control")
    @m.it("Manages permissions while respecting the include_controls option")
    def test_updates_control_permissions_mx(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        zone = "testZone"
        path = illumina_synthetic_irods / "12345" / "12345#888.cram"

        obj = DataObject(path)
        old_permissions = [AC("irods", perm=Permission.OWN, zone=zone)]
        new_permissions = [
            AC("irods", perm=Permission.OWN, zone=zone),
            AC("ss_888", perm=Permission.READ, zone=zone),
        ]

        assert obj.permissions() == old_permissions
        assert not ensure_secondary_metadata_updated(
            obj, mlwh_session=illumina_synthetic_mlwh, include_controls=False
        )
        assert obj.permissions() == old_permissions

        assert ensure_secondary_metadata_updated(
            obj, mlwh_session=illumina_synthetic_mlwh, include_controls=True
        )
        assert obj.permissions() == new_permissions

    @m.context("When data are not multiplexed")
    @m.context("When data have had consent withdrawn")
    @m.it("Does not restore access permissions")
    def test_retains_consent_withdrawn(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        zone = "testZone"
        path = illumina_synthetic_irods / "12345" / "12345.cram"
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
        path = illumina_synthetic_irods / "12345" / "12345#1_human.cram"
        anc_path = illumina_synthetic_irods / "12345" / "12345#1_human.vcf"
        qc_path = (
            illumina_synthetic_irods / "12345" / "qc" / "12345#1_human.genotype.json"
        )
        old_permissions = [AC("irods", perm=Permission.OWN, zone=zone)]

        for obj in [DataObject(path), DataObject(anc_path), DataObject(qc_path)]:
            assert ensure_consent_withdrawn(obj)
            assert obj.permissions() == old_permissions
            assert ensure_secondary_metadata_updated(
                obj, mlwh_session=illumina_synthetic_mlwh
            )
            assert obj.permissions() == old_permissions

    @m.context("When a data object is intended to have public access")
    @m.context("When public access permissions are present")
    @m.it("Retains public access permissions")
    def test_retains_public_access_unmanaged(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        zone = "testZone"
        path = illumina_synthetic_irods / "12345" / "qc" / "12345#1.bam_flagstats.json"
        public_read = make_public_read_acl(zone=zone).pop()

        obj = DataObject(path)
        obj.add_permissions(public_read)
        old_permissions = [AC("irods", perm=Permission.OWN, zone=zone), public_read]

        assert obj.permissions() == old_permissions

        # Not False because some changes do take effect - study_id AVU is added
        assert ensure_secondary_metadata_updated(
            obj, mlwh_session=illumina_synthetic_mlwh
        )
        assert obj.permissions() == old_permissions

    @m.context("When a data object has managed access removed")
    @m.context("When public access permissions are not present")
    @m.it("Adds public access permissions")
    def test_adds_public_access_unmanaged(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        zone = "testZone"
        path = illumina_synthetic_irods / "12345" / "qc" / "12345#1.bam_flagstats.json"
        old_permissions = [
            AC("irods", perm=Permission.OWN, zone=zone),
            AC("ss_4000", perm=Permission.READ, zone=zone),
        ]
        public_read = make_public_read_acl(zone=zone).pop()

        obj = DataObject(path)
        obj.add_permissions(*old_permissions)
        new_permissions = [AC("irods", perm=Permission.OWN, zone=zone), public_read]

        assert obj.permissions() == old_permissions

        assert ensure_secondary_metadata_updated(
            obj, mlwh_session=illumina_synthetic_mlwh
        )
        assert obj.permissions() == new_permissions

    @m.context("When a data object is not intended to have public access")
    @m.context("When public access permissions are present")
    @m.it("Removes public access permissions")
    def test_removes_public_access_managed(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        zone = "testZone"
        path = illumina_synthetic_irods / "12345" / "qc" / "12345#1.genotype.json"
        public_read = make_public_read_acl(zone=zone).pop()

        obj = DataObject(path)
        obj.add_permissions(public_read)
        old_permissions = [AC("irods", perm=Permission.OWN, zone=zone), public_read]
        new_permissions = [
            AC("irods", perm=Permission.OWN, zone=zone),
            AC("ss_4000", perm=Permission.READ, zone=zone),
        ]

        assert obj.permissions() == old_permissions

        assert ensure_secondary_metadata_updated(
            obj, mlwh_session=illumina_synthetic_mlwh
        )
        assert obj.permissions() == new_permissions

    @m.context("When the back-population script runs with data in STDIN")
    @m.context("When sample_uuid and sample_lims are not present")
    @m.it("Add the metadata with no compilation errors")
    def test_add_sample_compile_ok(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        path1 = illumina_synthetic_irods / "12345" / "12345.cram"
        path2 = illumina_synthetic_irods / "67890" / "67890#1.cram"

        obj1 = DataObject(path1)
        obj2 = DataObject(path2)
        for obj in [obj1, obj2]:
            obj.add_metadata(AVU(TrackedSample.ID, "id_sample_lims1"))
        expected_metadata = [
            AVU(TrackedSample.LIMS, "LIMS_01"),
            AVU(TrackedSample.UUID, "52429892-0ab6-11ee-b5ba-fa163eac3af1"),
        ]

        input_file = Path("input.txt")
        with open(file=input_file, mode="w") as fh:
            fh.writelines("\n".join([str(path1), str(path2)]))

        for avu in expected_metadata:
            assert avu not in obj1.metadata()
            assert avu not in obj2.metadata()

        echo_proc = subprocess.Popen(
            ["cat", fh.name], stdout=subprocess.PIPE, text=True
        )
        summary_file = Path("summary.txt")
        update_proc = subprocess.Popen(
            [
                "update-uuid-lims-metadata",
                "--db-config",
                "tests/testdb.ini",
                "--verbose",
                "--db-section",
                "github",
                "--summary",
                summary_file.name,
            ],
            stdin=echo_proc.stdout,
            stdout=subprocess.PIPE,
            text=True,
        )
        output, error = update_proc.communicate()

        for avu in expected_metadata:
            assert avu in obj1.metadata()
            assert avu in obj2.metadata()
        assert summary_file.exists()
        input_file.unlink()
        summary_file.unlink()

    @m.context("When the sample_id is in the metadata")
    @m.context("When sample_uuid and sample_lims are not present")
    @m.it("Add sample_uuid and sample_lims")
    def test_add_sample_uuid_lims(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh, connection_engine
    ):
        path = illumina_synthetic_irods / "12345" / "12345.cram"

        obj = DataObject(path)
        obj.add_metadata(AVU(TrackedSample.ID, "id_sample_lims1"))
        expected_metadata = [
            AVU(TrackedSample.LIMS, "LIMS_01"),
            AVU(TrackedSample.UUID, "52429892-0ab6-11ee-b5ba-fa163eac3af1"),
        ]

        with session_context(connection_engine) as mlwh_session:
            statuses = add_lims_uuid_to_iRODS_object(str(path), mlwh_session)
            for status in statuses:
                assert status == Status.UPDATED

        for avu in expected_metadata:
            assert avu in obj.metadata()

    @m.context("When the sample_id is in the metadata")
    @m.context("When sample_uuid and sample_lims are already present")
    @m.it("Skip the update of sample_uuid and sample_lims")
    def test_add_sample_uuid_lims_present(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh, connection_engine
    ):
        path = illumina_synthetic_irods / "12345" / "12345.cram"

        obj = DataObject(path)
        expected_metadata = [
            AVU(TrackedSample.ID, "id_sample_lims1"),
            AVU(TrackedSample.LIMS, "LIMS_01"),
            AVU(TrackedSample.UUID, "52429892-0ab6-11ee-b5ba-fa163eac3af1"),
        ]
        obj.add_metadata(*expected_metadata)
        for avu in expected_metadata:
            assert avu in obj.metadata()

        with session_context(connection_engine) as mlwh_session:
            statuses = add_lims_uuid_to_iRODS_object(str(path), mlwh_session)
            for status in statuses:
                assert status == Status.SKIPPED

    @m.context("When the sample_id is in the metadata")
    @m.context("When sample_lims is already present, but uuid is missing")
    @m.it("Add the missing sample_uuid")
    def test_add_sample_uuid_only(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh, connection_engine
    ):
        path = illumina_synthetic_irods / "12345" / "12345.cram"

        uuid_avu = AVU(TrackedSample.UUID, "52429892-0ab6-11ee-b5ba-fa163eac3af1")

        obj = DataObject(path)
        expected_metadata = [
            AVU(TrackedSample.ID, "id_sample_lims1"),
            AVU(TrackedSample.LIMS, "LIMS_01"),
        ]
        obj.add_metadata(*expected_metadata)
        for avu in expected_metadata:
            assert avu in obj.metadata()

        with session_context(connection_engine) as mlwh_session:
            statuses = add_lims_uuid_to_iRODS_object(str(path), mlwh_session)
            assert statuses.count(Status.UPDATED) == 1
            assert statuses.count(Status.SKIPPED) == 1
        assert uuid_avu in obj.metadata()

    @m.context("When the sample_id is in the metadata")
    @m.context("When sample_uuid is already present, but sample_lims is missing")
    @m.it("Add the missing sample_lims")
    def test_add_sample_lims_only(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh, connection_engine
    ):
        path = illumina_synthetic_irods / "12345" / "12345.cram"

        lims_avu = AVU(TrackedSample.LIMS, "LIMS_01")

        obj = DataObject(path)
        expected_metadata = [
            AVU(TrackedSample.ID, "id_sample_lims1"),
            AVU(TrackedSample.UUID, "52429892-0ab6-11ee-b5ba-fa163eac3af1"),
        ]
        obj.add_metadata(*expected_metadata)
        for avu in expected_metadata:
            assert avu in obj.metadata()

        with session_context(connection_engine) as mlwh_session:
            statuses = add_lims_uuid_to_iRODS_object(str(path), mlwh_session)
            assert statuses.count(Status.UPDATED) == 1
            assert statuses.count(Status.SKIPPED) == 1
        assert lims_avu in obj.metadata()

    @m.context("When the data are multiplexed")
    @m.context("When the data are associated with the computationally created tag 0")
    @m.context("When the sample_uuid and sample_lims metadata are absent")
    @m.it("Adds sample_uuid and sample_lims from all samples and studies in the pool")
    def test_add_sample_lims_uuid_tag0(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh, connection_engine
    ):
        path = illumina_synthetic_irods / "12345" / "12345#0.cram"

        obj = DataObject(path)
        required_metadata = [
            AVU(TrackedSample.ID, "id_sample_lims1"),
            AVU(TrackedSample.ID, "id_sample_lims2"),
        ]
        obj.add_metadata(*required_metadata)
        expected_metadata = [
            AVU(TrackedSample.LIMS, "LIMS_01"),
            AVU(TrackedSample.LIMS, "LIMS_01"),
            AVU(TrackedSample.UUID, "52429892-0ab6-11ee-b5ba-fa163eac3af1"),
            AVU(TrackedSample.UUID, "52429892-0ab6-11ee-b5ba-fa163eac3af2"),
        ]

        for avu in expected_metadata:
            assert avu not in obj.metadata()

        with session_context(connection_engine) as mlwh_session:
            statuses = add_lims_uuid_to_iRODS_object(str(path), mlwh_session)
            assert statuses.count(Status.UPDATED) == 3
            assert statuses.count(Status.SKIPPED) == 1

        for avu in expected_metadata:
            assert avu in obj.metadata()
