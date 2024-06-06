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

from partisan.irods import AC, AVU, DataObject, Permission
from pytest import mark as m

from npg_irods.common import ensure_secondary_metadata_updated
from npg_irods.metadata.lims import TrackedSample, TrackedStudy


class TestCommonFunctions:
    @m.context("When an iRODS object has both study and sample ID metadata")
    @m.context("When it wants both study and sample metadata enhanced")
    @m.it("Updates study and sample metadata from MLWH")
    def test_ensure_secondary_metadata_updated(
        self, simple_study_and_sample_data_object, simple_study_and_sample_mlwh
    ):
        sample_id = "id_sample_lims1"
        study_id = "1000"

        expected_avus = [
            AVU(TrackedStudy.ID, study_id),
            AVU(TrackedStudy.NAME, "Study X"),
            AVU(TrackedStudy.TITLE, "Test Study Title"),
            AVU(TrackedStudy.ACCESSION_NUMBER, "Test Accession"),
            AVU(TrackedSample.ID, sample_id),
            AVU(TrackedSample.ACCESSION_NUMBER, "Test Accession"),
            AVU(TrackedSample.COMMON_NAME, "common_name1"),
            AVU(TrackedSample.DONOR_ID, "donor_id1"),
            AVU(TrackedSample.NAME, "name1"),
            AVU(TrackedSample.PUBLIC_NAME, "public_name1"),
            AVU(TrackedSample.SUPPLIER_NAME, "supplier_name1"),
        ]

        obj = DataObject(simple_study_and_sample_data_object)
        assert ensure_secondary_metadata_updated(obj, simple_study_and_sample_mlwh)

        for avu in expected_avus:
            assert avu in obj.metadata()

    @m.context("When an iRODS object has both study and sample ID metadata")
    @m.context("When it wants both study and sample metadata enhanced")
    @m.it("Updates permissions according to the study")
    def test_ensure_secondary_metadata_permissions_updated(
        self, simple_study_and_sample_data_object, simple_study_and_sample_mlwh
    ):
        zone = "testZone"

        obj = DataObject(simple_study_and_sample_data_object)
        assert obj.permissions() == [AC("irods", perm=Permission.OWN, zone=zone)]
        assert ensure_secondary_metadata_updated(obj, simple_study_and_sample_mlwh)
        assert obj.permissions() == [
            AC("irods", perm=Permission.OWN, zone=zone),
            AC("ss_1000", perm=Permission.READ, zone=zone),
        ]

    @m.context("When an iRODS object has study ID, but not sample ID metadata")
    @m.context("When it wants study metadata enhanced")
    @m.it("Updates permissions according to the study")
    def test_ensure_secondary_metadata_permissions_updated_no_sample(
        self, simple_study_and_sample_data_object, simple_study_and_sample_mlwh
    ):
        zone = "testZone"

        obj = DataObject(simple_study_and_sample_data_object)
        obj.remove_metadata(AVU(TrackedSample.ID, "id_sample_lims1"))

        assert obj.permissions() == [AC("irods", perm=Permission.OWN, zone=zone)]
        assert ensure_secondary_metadata_updated(obj, simple_study_and_sample_mlwh)
        assert obj.permissions() == [
            AC("irods", perm=Permission.OWN, zone=zone),
            AC("ss_1000", perm=Permission.READ, zone=zone),
        ]

    @m.context("When an iRODS object has no study ID")
    @m.context("When it wants study metadata enhanced")
    @m.it("Removes access permissions")
    def test_ensure_secondary_metadata_permissions_updated_no_study(
        self, simple_study_and_sample_data_object, simple_study_and_sample_mlwh
    ):
        zone = "testZone"

        obj = DataObject(simple_study_and_sample_data_object)
        obj.remove_metadata(AVU(TrackedStudy.ID, "1000"))
        obj.add_permissions(
            AC("ss_1000", perm=Permission.READ, zone=zone),
        )

        assert obj.permissions() == [
            AC("irods", perm=Permission.OWN, zone=zone),
            AC("ss_1000", perm=Permission.READ, zone=zone),
        ]
        assert ensure_secondary_metadata_updated(obj, simple_study_and_sample_mlwh)
        assert obj.permissions() == [AC("irods", perm=Permission.OWN, zone=zone)]
