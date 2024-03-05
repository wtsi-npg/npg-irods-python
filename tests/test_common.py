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

from partisan.irods import AVU, DataObject
from pytest import mark as m

from npg_irods.common import update_secondary_metadata_from_mlwh
from npg_irods.metadata.lims import TrackedSample, TrackedStudy


class TestCommonFunctions:
    @m.context("When an iRODS object has both study and sample ID metadata")
    @m.context("When it wants both study and sample metadata enhanced")
    @m.it("Updates study and sample metadata from MLWH")
    def test_update_secondary_metadata_from_mlwh(
        self, general_synthetic_irods, general_synthetic_mlwh
    ):
        path = general_synthetic_irods / "lorem.txt"
        obj = DataObject(path)

        old_avus = [
            AVU(TrackedStudy.ID, "1000"),
            AVU(TrackedSample.ID, "id_sample_lims1"),
        ]

        for avu in old_avus:
            assert avu in obj.metadata()

        expected_avus = [
            AVU(TrackedStudy.ID, "1000"),
            AVU(TrackedStudy.NAME, "Study X"),
            AVU(TrackedStudy.TITLE, "Test Study Title"),
            AVU(TrackedStudy.ACCESSION_NUMBER, "Test Accession"),
            AVU(TrackedSample.ID, "id_sample_lims1"),
            AVU(TrackedSample.ACCESSION_NUMBER, "Test Accession"),
            AVU(TrackedSample.COMMON_NAME, "common_name1"),
            AVU(TrackedSample.DONOR_ID, "donor_id1"),
            AVU(TrackedSample.NAME, "name1"),
            AVU(TrackedSample.PUBLIC_NAME, "public_name1"),
            AVU(TrackedSample.SUPPLIER_NAME, "supplier_name1"),
        ]

        assert update_secondary_metadata_from_mlwh(
            obj, general_synthetic_mlwh, "1000", "id_sample_lims1"
        )

        for avu in expected_avus:
            assert avu in obj.metadata()

    @m.context("When an iRODS object has study ID, but not sample ID metadata")
    @m.context("When it wants study metadata enhanced")
    @m.it("Updates study metadata from MLWH")
    def test_update_secondary_metadata_from_mlwh_no_sample(
        self, general_synthetic_irods, general_synthetic_mlwh
    ):
        path = general_synthetic_irods / "lorem.txt"
        obj = DataObject(path)

        obj.remove_metadata(AVU(TrackedSample.ID, "id_sample_lims1"))

        old_avus = [AVU(TrackedStudy.ID, "1000")]

        for avu in old_avus:
            assert avu in obj.metadata()

        expected_avus = [
            AVU(TrackedStudy.ID, "1000"),
            AVU(TrackedStudy.NAME, "Study X"),
            AVU(TrackedStudy.TITLE, "Test Study Title"),
            AVU(TrackedStudy.ACCESSION_NUMBER, "Test Accession"),
        ]

        assert update_secondary_metadata_from_mlwh(
            obj, general_synthetic_mlwh, "1000", None
        )

        for avu in expected_avus:
            assert avu in obj.metadata()
