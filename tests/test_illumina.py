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
from partisan.irods import AVU, DataObject
from pytest import mark as m

from conftest import history_in_meta
from npg_irods.illumina import MetadataUpdate
from npg_irods.metadata.lims import TrackedSample, TrackedStudy


class TestMetadataUpdateZZZ(object):
    @m.context("When the data are not multiplexed")
    @m.context("When the metadata are absent")
    @m.it("Adds sample- and study-specific metadata")
    def test_updates_absent_metadata(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        path = illumina_synthetic_irods / "12345/12345.cram"
        obj = DataObject(path)
        assert AVU(TrackedSample.NAME, "sample 1") not in obj.metadata()
        assert AVU(TrackedStudy.ID, "3000") not in obj.metadata()

        num_input, num_updated, num_errors = MetadataUpdate().update_secondary_metadata(
            [path], mlwh_session=illumina_synthetic_mlwh
        )
        assert num_input == 1
        assert num_updated == 1
        assert num_errors == 0
        assert AVU(TrackedSample.NAME, "sample 1") in obj.metadata()
        assert AVU(TrackedStudy.ID, "3000") in obj.metadata()

    @m.context("When the data are not multiplexed")
    @m.context("When correct metadata are already present")
    @m.it("Leaves the metadata unchanged")
    def test_updates_present_metadata(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        path = illumina_synthetic_irods / "12345/12345.cram"
        obj = DataObject(path)
        obj.add_metadata(
            AVU(TrackedSample.NAME, "sample 1"), AVU(TrackedStudy.ID, "3000")
        )

        num_input, num_updated, num_errors = MetadataUpdate().update_secondary_metadata(
            [path], mlwh_session=illumina_synthetic_mlwh
        )
        assert num_input == 1
        assert num_updated == 1
        assert num_errors == 0
        assert AVU(TrackedSample.NAME, "sample 1") in obj.metadata()
        assert AVU(TrackedStudy.ID, "3000") in obj.metadata()

    @m.context("When the data are not multiplexed")
    @m.context("When incorrect metadata are present")
    @m.it("Updates the metadata and adds history metadata")
    def test_updates_changed_metadata(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        path = illumina_synthetic_irods / "12345/12345.cram"
        obj = DataObject(path)
        obj.add_metadata(
            AVU(TrackedSample.NAME, "sample 99"), AVU(TrackedStudy.ID, "9999")
        )

        num_input, num_updated, num_errors = MetadataUpdate().update_secondary_metadata(
            [path], mlwh_session=illumina_synthetic_mlwh
        )
        assert num_input == 1
        assert num_updated == 1
        assert num_errors == 0
        assert AVU(TrackedSample.NAME, "sample 1") in obj.metadata()
        assert AVU(TrackedStudy.ID, "3000") in obj.metadata()
        assert AVU(TrackedSample.NAME, "sample 99") not in obj.metadata()
        assert AVU(TrackedStudy.ID, "9999") not in obj.metadata()

        history = AVU.history(AVU(TrackedSample.NAME, "sample 99"))
        assert history_in_meta(history, obj.metadata())

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

        num_input, num_updated, num_errors = MetadataUpdate().update_secondary_metadata(
            [path], mlwh_session=illumina_synthetic_mlwh
        )
        assert num_input == 1
        assert num_updated == 1
        assert num_errors == 0
        for avu in old_avus:
            assert avu not in obj.metadata()

        history = AVU.history(*old_avus)
        assert history_in_meta(history, obj.metadata())

    @m.context("When the data are multiplexed")
    @m.context("When the metadata are absent")
    @m.it("Adds sample- and study-specific metadata")
    def test_updates_absent_metadata_mx(
        self, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        path = illumina_synthetic_irods / "12345/12345#1.cram"
        obj = DataObject(path)
        assert AVU(TrackedSample.NAME, "sample 1") not in obj.metadata()
        assert AVU(TrackedStudy.ID, "3000") not in obj.metadata()

        num_input, num_updated, num_errors = MetadataUpdate().update_secondary_metadata(
            [path], mlwh_session=illumina_synthetic_mlwh
        )
        assert num_input == 1
        assert num_updated == 1
        assert num_errors == 0
        # The data are two plexes of single sample (from different flowcell positions)
        # that have been merged.
        assert AVU(TrackedSample.NAME, "sample 1") in obj.metadata()
        assert AVU(TrackedStudy.ID, "3000") in obj.metadata()

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
            AVU(TrackedStudy.ID, "3000"),
            AVU(TrackedSample.DONOR_ID, "donor1"),
            AVU(TrackedSample.DONOR_ID, "donor2"),
            AVU(TrackedSample.ID, "sanger_sample1"),
            AVU(TrackedSample.ID, "sanger_sample2"),
            AVU(TrackedSample.NAME, "sample 1"),
            AVU(TrackedSample.NAME, "sample 2"),
            AVU(TrackedSample.SUPPLIER_NAME, "supplier_name1"),
            AVU(TrackedSample.SUPPLIER_NAME, "supplier_name2"),
        ]
        for avu in expected_avus:
            assert avu not in obj.metadata()

        num_input, num_updated, num_errors = MetadataUpdate().update_secondary_metadata(
            [path], mlwh_session=illumina_synthetic_mlwh
        )
        assert num_input == 1
        assert num_updated == 1
        assert num_errors == 0
        for avu in expected_avus:
            assert avu in obj.metadata()
        pytest.fail()
