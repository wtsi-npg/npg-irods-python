# -*- coding: utf-8 -*-
#
# Copyright © 2024 Genome Research Ltd. All rights reserved.
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

from pathlib import PurePath

import pytest
from partisan.irods import AC, AVU, DataObject, Permission
from pytest import mark as m

from helpers import history_in_meta
from npg_irods.metadata.common import SeqConcept, SeqSubset
from npg_irods.metadata.lims import TrackedSample, TrackedStudy
from npg_irods.metadata.pacbio import Instrument
from npg_irods.pacbio import Component, ensure_secondary_metadata_updated


class TestPacBioComponent:
    @m.context("When component AVUs are available")
    @m.it("They be used to construct a Component")
    def test_make_component_from_avus(self):
        c = Component.from_avus(
            AVU(Instrument.RUN_NAME, "run1"),
            AVU(Instrument.WELL_LABEL, "A01"),
            AVU(Instrument.TAG_SEQUENCE, "tag1"),
            AVU(Instrument.PLATE_NUMBER, "1"),
            AVU(SeqConcept.SUBSET, SeqSubset.HUMAN.value),
        )
        assert c.run_name == "run1"
        assert c.well_label == "A01"
        assert c.tag_sequence == "tag1"
        assert c.plate_number == 1
        assert c.subset == SeqSubset.HUMAN

    @m.context("When component AVUs are available")
    @m.context("When there are multiple AVUs with the same key")
    @m.it("Raises an error")
    def test_make_component_from_avus_duplicate_keys(self):
        with pytest.raises(ValueError):
            Component.from_avus(
                AVU(Instrument.RUN_NAME, "run1"),
                AVU(Instrument.RUN_NAME, "run2"),
            )

    @m.context("When component AVUs are available")
    @m.context("When the run AVU is missing")
    @m.it("Raises an error")
    def test_make_component_from_avus_missing_run(self):
        with pytest.raises(ValueError):
            Component.from_avus(
                AVU(Instrument.WELL_LABEL, "A01"),
                AVU(Instrument.TAG_SEQUENCE, "tag1"),
                AVU(Instrument.PLATE_NUMBER, "1"),
                AVU(SeqConcept.SUBSET, SeqSubset.HUMAN.value),
            )

    @m.context("When component AVUs are available")
    @m.context("When the well label AVU is missing")
    @m.it("Raises an error")
    def test_make_component_from_avus_missing_well(self):
        with pytest.raises(ValueError):
            Component.from_avus(
                AVU(Instrument.RUN_NAME, "run1"),
                AVU(Instrument.TAG_SEQUENCE, "tag1"),
                AVU(Instrument.PLATE_NUMBER, "1"),
                AVU(SeqConcept.SUBSET, SeqSubset.HUMAN.value),
            )


@m.describe("PacBio iRODS metadata updates")
class TestPacBioMetadataUpdate:
    @m.context("When the metadata are absent")
    @m.it("Adds sample-specific and study-specific metadata")
    def test_updates_absent_metadata(
        self, pacbio_synthetic_mlwh, pacbio_synthetic_irods
    ):
        path = pacbio_synthetic_irods / PurePath(
            "r12345_20246789_98765",
            "1_A01",
            "m12345_246789_987655_s3.hifi_reads.bc1000.bam",
        )

        obj = DataObject(path)
        expected_metadata = [
            AVU(TrackedSample.ACCESSION_NUMBER, "ACC1"),
            AVU(TrackedSample.COMMON_NAME, "common_name1"),
            AVU(TrackedSample.DONOR_ID, "donor_id1"),
            AVU(TrackedSample.ID, "id_sample_lims1"),
            AVU(TrackedSample.NAME, "name1"),
            AVU(TrackedSample.PUBLIC_NAME, "public_name1"),
            AVU(TrackedSample.SUPPLIER_NAME, "supplier_name1"),
            AVU(TrackedStudy.ID, "1000"),
            AVU(TrackedStudy.NAME, "Study X"),
        ]

        for avu in expected_metadata:
            assert avu not in obj.metadata()

        assert ensure_secondary_metadata_updated(
            obj, mlwh_session=pacbio_synthetic_mlwh
        )

        for avu in expected_metadata:
            assert avu in obj.metadata()

    @m.context("When the metadata are already present")
    @m.it("Leaves the metadata unchanged")
    def test_updates_present_metadata(
        self, pacbio_synthetic_mlwh, pacbio_synthetic_irods
    ):
        zone = "testZone"
        path = pacbio_synthetic_irods / PurePath(
            "r12345_20246789_98765",
            "1_A01",
            "m12345_246789_987655_s3.hifi_reads.bc1000.bam",
        )

        obj = DataObject(path)
        expected_metadata = [
            AVU(TrackedSample.ACCESSION_NUMBER, "ACC1"),
            AVU(TrackedSample.COMMON_NAME, "common_name1"),
            AVU(TrackedSample.DONOR_ID, "donor_id1"),
            AVU(TrackedSample.ID, "id_sample_lims1"),
            AVU(TrackedSample.LIMS, "LIMS_01"),
            AVU(TrackedSample.NAME, "name1"),
            AVU(TrackedSample.PUBLIC_NAME, "public_name1"),
            AVU(TrackedSample.SUPPLIER_NAME, "supplier_name1"),
            AVU(TrackedSample.UUID, "72429892-0ab6-11ee-b5ba-fa163eac3af1"),
            AVU(TrackedStudy.ID, "1000"),
            AVU(TrackedStudy.NAME, "Study X"),
        ]
        expected_permissions = [
            AC("irods", perm=Permission.OWN, zone=zone),
            AC("ss_1000", perm=Permission.READ, zone=zone),
        ]
        obj.add_metadata(*expected_metadata)
        obj.add_permissions(*expected_permissions)

        for avu in expected_metadata:
            assert avu in obj.metadata()

        assert not ensure_secondary_metadata_updated(
            obj, mlwh_session=pacbio_synthetic_mlwh
        )

        for avu in expected_metadata:
            assert avu in obj.metadata()

    @m.context("When incorrect metadata are present")
    @m.it("Updates the metadata and adds history metadata")
    def test_updates_changed_metadata(
        self, pacbio_synthetic_mlwh, pacbio_synthetic_irods
    ):
        path = pacbio_synthetic_irods / PurePath(
            "r12345_20246789_98765",
            "1_A01",
            "m12345_246789_987655_s3.hifi_reads.bc1000.bam",
        )

        obj = DataObject(path)
        old_metadata = [
            AVU(TrackedSample.NAME, "sample 99"),
            AVU(TrackedStudy.ID, "9999"),
        ]
        obj.add_metadata(*old_metadata)

        for avu in old_metadata:
            assert avu in obj.metadata()

        assert ensure_secondary_metadata_updated(
            obj, mlwh_session=pacbio_synthetic_mlwh
        )

        for avu in old_metadata:
            assert avu not in obj.metadata()
            assert history_in_meta(AVU.history(avu), obj.metadata())

        expected_metadata = [
            AVU(TrackedSample.ACCESSION_NUMBER, "ACC1"),
            AVU(TrackedSample.COMMON_NAME, "common_name1"),
            AVU(TrackedSample.DONOR_ID, "donor_id1"),
            AVU(TrackedSample.ID, "id_sample_lims1"),
            AVU(TrackedSample.NAME, "name1"),
            AVU(TrackedSample.PUBLIC_NAME, "public_name1"),
            AVU(TrackedSample.SUPPLIER_NAME, "supplier_name1"),
            AVU(TrackedStudy.ID, "1000"),
            AVU(TrackedStudy.NAME, "Study X"),
        ]
        for avu in expected_metadata:
            assert avu in obj.metadata()


class TestPacBioPermissionsUpdate:
    @m.context("When the permissions are absent")
    @m.it("Adds study-specific permissions")
    def test_updates_absent_study_permissions(
        self, pacbio_synthetic_mlwh, pacbio_synthetic_irods
    ):
        zone = "testZone"
        path = pacbio_synthetic_irods / PurePath(
            "r12345_20246789_98765",
            "1_A01",
            "m12345_246789_987655_s3.hifi_reads.bc1000.bam",
        )
        old_permissions = [AC("irods", perm=Permission.OWN, zone=zone)]
        new_permissions = [
            AC("irods", perm=Permission.OWN, zone=zone),
            AC("ss_1000", perm=Permission.READ, zone=zone),
        ]

        obj = DataObject(path)

        assert obj.permissions() == old_permissions
        assert ensure_secondary_metadata_updated(
            obj, mlwh_session=pacbio_synthetic_mlwh
        )
        assert obj.permissions() == new_permissions

    @m.context("When the permissions are already present")
    @m.it("Leaves the permissions unchanged")
    def test_updates_present_study_permissions(
        self, pacbio_synthetic_mlwh, pacbio_synthetic_irods
    ):
        zone = "testZone"
        path = pacbio_synthetic_irods / PurePath(
            "r12345_20246789_98765",
            "1_A01",
            "m12345_246789_987655_s3.hifi_reads.bc1000.bam",
        )
        old_metadata = [
            AVU(TrackedSample.ACCESSION_NUMBER, "ACC1"),
            AVU(TrackedSample.COMMON_NAME, "common_name1"),
            AVU(TrackedSample.DONOR_ID, "donor_id1"),
            AVU(TrackedSample.ID, "id_sample_lims1"),
            AVU(TrackedSample.LIMS, "LIMS_01"),
            AVU(TrackedSample.NAME, "name1"),
            AVU(TrackedSample.PUBLIC_NAME, "public_name1"),
            AVU(TrackedSample.SUPPLIER_NAME, "supplier_name1"),
            AVU(TrackedSample.UUID, "72429892-0ab6-11ee-b5ba-fa163eac3af1"),
            AVU(TrackedStudy.ID, "1000"),
            AVU(TrackedStudy.NAME, "Study X"),
        ]
        old_permissions = [
            AC("irods", perm=Permission.OWN, zone=zone),
            AC("ss_1000", perm=Permission.READ, zone=zone),
        ]

        obj = DataObject(path)
        obj.add_metadata(*old_metadata)
        obj.add_permissions(*old_permissions)

        assert not ensure_secondary_metadata_updated(
            obj, mlwh_session=pacbio_synthetic_mlwh
        )
        assert obj.permissions() == old_permissions

    @m.context("When incorrect permissions are present")
    @m.it("Updated the permissions")
    def test_updates_changed_study_permissions(
        self, pacbio_synthetic_mlwh, pacbio_synthetic_irods
    ):
        zone = "testZone"
        path = pacbio_synthetic_irods / PurePath(
            "r12345_20246789_98765",
            "1_A01",
            "m12345_246789_987655_s3.hifi_reads.bc1000.bam",
        )
        old_permissions = [
            AC("irods", perm=Permission.OWN, zone=zone),
            AC("ss_2000", perm=Permission.READ, zone=zone),
        ]
        new_permissions = [
            AC("irods", perm=Permission.OWN, zone=zone),
            AC("ss_1000", perm=Permission.READ, zone=zone),
        ]

        obj = DataObject(path)
        obj.add_permissions(*old_permissions)

        assert ensure_secondary_metadata_updated(
            obj, mlwh_session=pacbio_synthetic_mlwh
        )
        assert obj.permissions() == new_permissions


# Early PacBio runs are like this:
#
# /seq/pacbio/27857_702/A01_1:
#   m140602_154634_00127_c100650732550000001823117710071455_s1_p0.metadata.xml
#   C- /seq/pacbio/27857_702/A01_1/Analysis_Results
# /seq/pacbio/27857_702/A01_1/Analysis_Results:
#   m140602_154634_00127_c100650732550000001823117710071455_s1_p0.1.bax.h5
#   m140602_154634_00127_c100650732550000001823117710071455_s1_p0.2.bax.h5
#   m140602_154634_00127_c100650732550000001823117710071455_s1_p0.3.bax.h5
#   m140602_154634_00127_c100650732550000001823117710071455_s1_p0.bas.h5
#   m140602_154634_00127_c100650732550000001823117710071455_s1_p0.sts.xml
#
# The h5 files have run: and well: attributes in their metadata.

# Later PacBio runs are like this:
#
#  C- /seq/pacbio/r54097_20161110_145040/1_A01
# /seq/pacbio/r54097_20161110_145040/1_A01:
#   m54097_161110_145910.adapters.fasta
#   m54097_161110_145910.scraps.bam
#   m54097_161110_145910.scraps.bam.pbi
#   m54097_161110_145910.sts.xml
#   m54097_161110_145910.subreads.bam
#   m54097_161110_145910.subreads.bam.pbi
#   m54097_161110_145910.subreadset.xml
#
# The bam files have run: and well: attributes in their metadata.

# Later PacBio runs are like this:

# /seq/pacbio/r64097e_20230309_153535:
#   C- /seq/pacbio/r64097e_20230309_153535/1_A01
# /seq/pacbio/r64097e_20230309_153535/1_A01:
#   demultiplex.bc1012_BAK8A_OA--bc1012_BAK8A_OA.bam
#   demultiplex.bc1012_BAK8A_OA--bc1012_BAK8A_OA.bam.pbi
#   demultiplex.bc1012_BAK8A_OA--bc1012_BAK8A_OA.consensusreadset.xml
#   m64097e_230309_154741.consensusreadset.xml
#   m64097e_230309_154741.hifi_reads.bam
#   m64097e_230309_154741.hifi_reads.bam.pbi
#   m64097e_230309_154741.primary_qc.tar.xz
#   m64097e_230309_154741.sts.xml
#   m64097e_230309_154741.zmw_metrics.json.gz
#   merged_analysis_report.json
#
# The bam files have run: , well: , tag_index: and tag_sequence: attributes in their
# metadata.

# Later PacBio runs are like this:
#
# /seq/pacbio/r84098_20240122_143954/1_A01:
#   m84098_240122_144715_s3.fail_reads.bc2017.bam
#   m84098_240122_144715_s3.fail_reads.bc2017.bam.pbi
#   m84098_240122_144715_s3.fail_reads.bc2017.consensusreadset.xml
#   m84098_240122_144715_s3.fail_reads.consensusreadset.xml
#   m84098_240122_144715_s3.fail_reads.unassigned.bam
#   m84098_240122_144715_s3.fail_reads.unassigned.bam.pbi
#   m84098_240122_144715_s3.fail_reads.unassigned.consensusreadset.xml
#   m84098_240122_144715_s3.hifi_reads.bc2017.bam
#   m84098_240122_144715_s3.hifi_reads.bc2017.bam.pbi
#   m84098_240122_144715_s3.hifi_reads.bc2017.consensusreadset.xml
#   m84098_240122_144715_s3.hifi_reads.consensusreadset.xml
#   m84098_240122_144715_s3.hifi_reads.unassigned.bam
#   m84098_240122_144715_s3.hifi_reads.unassigned.bam.pbi
#   m84098_240122_144715_s3.hifi_reads.unassigned.consensusreadset.xml
#   m84098_240122_144715_s3.primary_qc.tar.xz
#   m84098_240122_144715_s3.sts.xml
#   m84098_240122_144715_s3.zmw_metrics.json.gz
#   merged_analysis_report.json
#
# The bam files have run: , plate_number: , well: , tag_index: and tag_sequence:
# attributes in their metadata.
