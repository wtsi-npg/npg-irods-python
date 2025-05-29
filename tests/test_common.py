# -*- coding: utf-8 -*-
#
# Copyright © 2024, 2025 Genome Research Ltd. All rights reserved.
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

from partisan.irods import AC, AVU, DataObject, Permission
from pytest import mark as m

from npg_irods.common import ensure_secondary_metadata_updated
from npg_irods.metadata.lims import TrackedSample, TrackedStudy


class TestCommonFunctions:
    @m.context(
        "When an iRODS object has both one study and one sample ID metadata and "
        "it wants both study and sample metadata enhanced"
    )
    @m.it("Updates study and sample metadata from MLWH")
    def test_ensure_secondary_metadata_updated(
        self, single_study_and_single_sample_data_object, study_and_samples_mlwh
    ):
        sample_id = "id_sample_lims1"
        study_id = "1000"

        expected_avus = [
            AVU(TrackedStudy.ID, study_id),
            AVU(TrackedStudy.NAME, "Study X"),
            AVU(TrackedStudy.TITLE, "Test Study Title"),
            AVU(TrackedStudy.ACCESSION_NUMBER, "Test Accession"),
            AVU(TrackedSample.ID, sample_id),
            AVU(TrackedSample.LIMS, "LIMS_01"),
            AVU(TrackedSample.ACCESSION_NUMBER, "Test Accession"),
            AVU(TrackedSample.COMMON_NAME, "common_name1"),
            AVU(TrackedSample.DONOR_ID, "donor_id1"),
            AVU(TrackedSample.NAME, "name1"),
            AVU(TrackedSample.PUBLIC_NAME, "public_name1"),
            AVU(TrackedSample.SUPPLIER_NAME, "supplier_name1"),
            AVU(TrackedSample.UUID, "82429892-0ab6-11ee-b5ba-fa163eac3ag7"),
        ]

        obj = DataObject(single_study_and_single_sample_data_object)
        assert ensure_secondary_metadata_updated(obj, study_and_samples_mlwh)

        for avu in expected_avus:
            assert avu in obj.metadata()

    @m.context(
        "When an iRODS object has one study ID and multiple sample ID metadata and "
        "it wants both study and sample metadata enhanced"
    )
    @m.it("Updates study and sample metadata from MLWH")
    def test_ensure_secondary_metadata_updated_multiple_samples(
        self,
        single_study_and_multi_sample_data_object,
        study_and_samples_mlwh,
    ):
        sample_id1 = "id_sample_lims1"
        sample_id2 = "id_sample_lims2"
        study_id = "1000"

        expected_avus = [
            AVU(TrackedStudy.ID, study_id),
            AVU(TrackedStudy.NAME, "Study X"),
            AVU(TrackedStudy.TITLE, "Test Study Title"),
            AVU(TrackedStudy.ACCESSION_NUMBER, "Test Accession"),
            AVU(TrackedSample.ID, sample_id1),
            AVU(TrackedSample.ACCESSION_NUMBER, "Test Accession"),
            AVU(TrackedSample.COMMON_NAME, "common_name1"),
            AVU(TrackedSample.DONOR_ID, "donor_id1"),
            AVU(TrackedSample.NAME, "name1"),
            AVU(TrackedSample.PUBLIC_NAME, "public_name1"),
            AVU(TrackedSample.SUPPLIER_NAME, "supplier_name1"),
            AVU(TrackedSample.ID, sample_id2),
            AVU(TrackedSample.ACCESSION_NUMBER, "Test Accession"),
            AVU(TrackedSample.COMMON_NAME, "common_name2"),
            AVU(TrackedSample.DONOR_ID, "donor_id2"),
            AVU(TrackedSample.NAME, "name2"),
            AVU(TrackedSample.PUBLIC_NAME, "public_name2"),
            AVU(TrackedSample.SUPPLIER_NAME, "supplier_name2"),
        ]

        obj = DataObject(single_study_and_multi_sample_data_object)
        assert ensure_secondary_metadata_updated(obj, study_and_samples_mlwh)

        for avu in expected_avus:
            assert avu in obj.metadata()

    @m.context(
        "When an iRODS object has both study and sample ID metadat and "
        "it wants both study and sample metadata enhanced"
    )
    @m.it("Updates permissions according to the study")
    def test_ensure_secondary_metadata_permissions_updated(
        self, single_study_and_single_sample_data_object, study_and_samples_mlwh
    ):
        zone = "testZone"

        obj = DataObject(single_study_and_single_sample_data_object)
        assert obj.permissions() == [AC("irods", perm=Permission.OWN, zone=zone)]
        assert ensure_secondary_metadata_updated(obj, study_and_samples_mlwh)
        assert obj.permissions() == [
            AC("irods", perm=Permission.OWN, zone=zone),
            AC("ss_1000", perm=Permission.READ, zone=zone),
        ]

    @m.context(
        "When an iRODS object has one study ID, but no sample ID metadata and "
        "it wants study metadata enhanced"
    )
    @m.it("Updates permissions according to the study")
    def test_ensure_secondary_metadata_permissions_updated_no_sample(
        self, single_study_and_single_sample_data_object, study_and_samples_mlwh
    ):
        zone = "testZone"

        obj = DataObject(single_study_and_single_sample_data_object)
        obj.remove_metadata(AVU(TrackedSample.ID, "id_sample_lims1"))

        assert obj.permissions() == [AC("irods", perm=Permission.OWN, zone=zone)]
        assert ensure_secondary_metadata_updated(obj, study_and_samples_mlwh)
        assert obj.permissions() == [
            AC("irods", perm=Permission.OWN, zone=zone),
            AC("ss_1000", perm=Permission.READ, zone=zone),
        ]

    @m.context(
        "When an iRODS object has no study ID and one sample ID metadata and "
        "it wants study metadata enhanced"
    )
    @m.it("Removes access permissions")
    def test_ensure_secondary_metadata_permissions_updated_no_study(
        self, single_study_and_single_sample_data_object, study_and_samples_mlwh
    ):
        zone = "testZone"

        obj = DataObject(single_study_and_single_sample_data_object)
        obj.remove_metadata(AVU(TrackedStudy.ID, "1000"))
        obj.add_permissions(
            AC("ss_1000", perm=Permission.READ, zone=zone),
        )

        assert obj.permissions() == [
            AC("irods", perm=Permission.OWN, zone=zone),
            AC("ss_1000", perm=Permission.READ, zone=zone),
        ]
        assert ensure_secondary_metadata_updated(obj, study_and_samples_mlwh)
        assert obj.permissions() == [AC("irods", perm=Permission.OWN, zone=zone)]
