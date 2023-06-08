# -*- coding: utf-8 -*-
#
# Copyright © 2021, 2022, 2023 Genome Research Ltd. All rights reserved.
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

from partisan.irods import AC, AVU, Collection, Permission, format_timestamp
from pytest import mark as m

from datetime import datetime
from npg_irods import ont
from conftest import LATEST, ont_tag_identifier, tests_have_admin, ont_history_in_meta
from npg_irods.metadata.lims import SeqConcept, TrackedSample, TrackedStudy
from npg_irods.ont import MetadataUpdate, annotate_results_collection


class TestONT(object):
    @tests_have_admin
    @m.context("When an ONT experiment collection is annotated")
    @m.context("When the experiment is single-sample")
    @m.it("Adds sample and study metadata to the run-folder collection")
    def test_add_new_sample_metadata(self, ont_synthetic, mlwh_session):
        expt = "simple_experiment_001"
        slot = 1

        path = ont_synthetic / expt / "20190904_1514_GA10000_flowcell011_69126024"
        annotate_results_collection(
            path, experiment_name=expt, instrument_slot=slot, mlwh_session=mlwh_session
        )

        coll = Collection(path)
        for avu in [
            AVU(TrackedSample.NAME, "sample 1"),
            AVU(TrackedStudy.ID, "2000"),
            AVU(TrackedStudy.NAME, "Study Y"),
        ]:
            assert avu in coll.metadata(), f"{avu} is in {coll} metadata"

        expected_acl = [
            AC("irods", Permission.OWN, zone="testZone"),
            AC("ss_2000", Permission.READ, zone="testZone"),
        ]
        assert coll.acl() == expected_acl
        for item in coll.contents():
            assert item.acl() == expected_acl

    @tests_have_admin
    @m.context("When the experiment is multiplexed")
    @m.it("Adds {tag_index_from_id => <n>} metadata to barcode<0n> sub-collections")
    def test_add_new_plex_metadata(self, ont_synthetic, mlwh_session):
        expt = "multiplexed_experiment_001"
        slot = 1

        path = ont_synthetic / expt / "20190904_1514_GA10000_flowcell101_cf751ba1"

        annotate_results_collection(
            path, experiment_name=expt, instrument_slot=slot, mlwh_session=mlwh_session
        )

        for subcoll in ["fast5_fail", "fast5_pass", "fastq_fail", "fastq_pass"]:
            for tag_index in range(1, 12):
                tag_identifier = ont_tag_identifier(tag_index)
                bc_coll = Collection(
                    path / subcoll / ont.barcode_name_from_id(tag_identifier)
                )
                avu = AVU(SeqConcept.TAG_INDEX, ont.tag_index_from_id(tag_identifier))
                assert avu in bc_coll.metadata(), f"{avu} is in {bc_coll} metadata"

    @tests_have_admin
    @m.it("Adds sample and study metadata to barcode<0n> sub-collections")
    def test_add_new_plex_sample_metadata(self, ont_synthetic, mlwh_session):
        expt = "multiplexed_experiment_001"
        slot = 1

        path = ont_synthetic / expt / "20190904_1514_GA10000_flowcell101_cf751ba1"

        annotate_results_collection(
            path, experiment_name=expt, instrument_slot=slot, mlwh_session=mlwh_session
        )

        for subcoll in ["fast5_fail", "fast5_pass", "fastq_fail", "fastq_pass"]:
            for tag_index in range(1, 12):
                tag_id = ont_tag_identifier(tag_index)
                bc_coll = Collection(path / subcoll / ont.barcode_name_from_id(tag_id))

                for avu in [
                    AVU(TrackedSample.NAME, f"sample {tag_index}"),
                    AVU(TrackedStudy.ID, "3000"),
                    AVU(TrackedStudy.NAME, "Study Z"),
                ]:
                    assert avu in bc_coll.metadata(), f"{avu} is in {bc_coll} metadata"

                expected_acl = [
                    AC("irods", Permission.OWN, zone="testZone"),
                    AC("ss_3000", Permission.READ, zone="testZone"),
                ]

                assert bc_coll.acl() == expected_acl
                for item in bc_coll.contents():
                    assert item.acl() == expected_acl


class TestMetadataUpdate(object):
    @tests_have_admin
    @m.context("When an ONT metadata update is requested")
    @m.context("When no experiment name is specified")
    @m.context("When no time window is specified")
    @m.it("Finds all collections")
    def test_find_all(self, ont_synthetic, mlwh_session):
        num_simple_expts = 5
        num_multiplexed_expts = 3
        num_slots = 5

        update = MetadataUpdate()
        num_found, num_updated, num_errors = update.update_secondary_metadata(
            mlwh_session=mlwh_session
        )
        num_expected = (num_simple_expts * num_slots) + (
            num_multiplexed_expts * num_slots
        )

        assert num_found == num_expected, f"Found {num_expected} collections"
        assert num_updated == num_expected
        assert num_errors == 0

    @m.context("When no experiment name is specified")
    @m.context("When a time window is specified")
    @m.it("Finds only collections updated in that time window")
    def test_find_recent_updates(self, ont_synthetic, mlwh_session):
        update = MetadataUpdate()
        num_found, num_updated, num_errors = update.update_secondary_metadata(
            mlwh_session=mlwh_session, since=LATEST
        )

        # Only slots 1, 3 and 5 of multiplexed experiments 1 and 3 were updated in
        # the MLWH since time LATEST i.e.
        expected_colls = [
            Collection(ont_synthetic / path)
            for path in [
                "multiplexed_experiment_001/20190904_1514_GA10000_flowcell101_cf751ba1",
                "multiplexed_experiment_001/20190904_1514_GA30000_flowcell103_cf751ba1",
                "multiplexed_experiment_001/20190904_1514_GA50000_flowcell105_cf751ba1",
                "multiplexed_experiment_003/20190904_1514_GA10000_flowcell101_cf751ba1",
                "multiplexed_experiment_003/20190904_1514_GA30000_flowcell103_cf751ba1",
                "multiplexed_experiment_003/20190904_1514_GA50000_flowcell105_cf751ba1",
            ]
        ]
        num_expected = len(expected_colls)

        assert num_found == num_expected, (
            f"Found {num_expected} collections "
            "(slots 1, 3 and 5 of multiplexed experiments 1 and 3)"
        )
        assert num_updated == num_expected
        assert num_errors == 0

    @m.context("When an experiment name is specified")
    @m.it("Finds only collections with that experiment name")
    def test_find_updates_for_experiment(self, ont_synthetic, mlwh_session):
        update = MetadataUpdate(experiment_name="simple_experiment_001")
        num_found, num_updated, num_errors = update.update_secondary_metadata(
            mlwh_session=mlwh_session
        )

        expected_colls = [
            Collection(ont_synthetic / path)
            for path in [
                "simple_experiment_001/20190904_1514_G100000_flowcell011_69126024",
                "simple_experiment_001/20190904_1514_G200000_flowcell012_69126024",
                "simple_experiment_001/20190904_1514_G300000_flowcell013_69126024",
                "simple_experiment_001/20190904_1514_G400000_flowcell014_69126024",
                "simple_experiment_001/20190904_1514_G500000_flowcell015_69126024",
            ]
        ]
        num_expected = len(expected_colls)

        assert (
            num_found == num_expected
        ), f"Found {num_expected} collections (all slots from simple experiment 1)"
        assert num_updated == num_expected
        assert num_errors == 0

    @m.context("When an experiment name is specified")
    @m.context("When a slot position is specified")
    @m.it("Finds only collections with that experiment name and slot position")
    def test_find_updates_for_experiment_slot(self, ont_synthetic, mlwh_session):
        update = MetadataUpdate(
            experiment_name="simple_experiment_001", instrument_slot=1
        )
        num_found, num_updated, num_errors = update.update_secondary_metadata(
            mlwh_session=mlwh_session
        )

        expected_colls = [
            Collection(
                ont_synthetic
                / "simple_experiment_001/20190904_1514_G100000_flowcell011_69126024"
            )
        ]
        num_expected = len(expected_colls)

        assert (
            num_found == num_expected
        ), f"Found {num_expected} collections (slot 1 from simple experiment 1)"
        assert num_updated == num_expected
        assert num_errors == 0

    @m.context("When metadata is updated")
    @m.context("When the metadata is absent")
    @m.it("Adds the metadata")
    def test_updates_absent_metadata(self, ont_synthetic, mlwh_session):
        coll = Collection(
            ont_synthetic
            / "simple_experiment_001/20190904_1514_G100000_flowcell011_69126024"
        )
        assert AVU(TrackedSample.NAME, "sample 1") not in coll.metadata()
        update = MetadataUpdate(
            experiment_name="simple_experiment_001", instrument_slot=1
        )
        update.update_secondary_metadata(mlwh_session=mlwh_session)
        assert AVU(TrackedSample.NAME, "sample 1") in coll.metadata()

    @m.context("When correct metadata is already present")
    @m.it("Leaves the metadata unchanged")
    def test_updates_present_metadata(self, ont_synthetic, mlwh_session):
        coll = Collection(
            ont_synthetic
            / "simple_experiment_001/20190904_1514_G100000_flowcell011_69126024"
        )
        coll.add_metadata(AVU(TrackedSample.NAME, "sample 1"))
        update = MetadataUpdate(
            experiment_name="simple_experiment_001", instrument_slot=1
        )
        update.update_secondary_metadata(mlwh_session=mlwh_session)
        assert AVU(TrackedSample.NAME, "sample 1") in coll.metadata()

    @m.context("When incorrect metadata is present")
    @m.it("Changes the metadata and adds history metadata")
    def test_updates_changed_metadata(self, ont_synthetic, mlwh_session):
        coll = Collection(
            ont_synthetic
            / "simple_experiment_001/20190904_1514_G100000_flowcell011_69126024"
        )
        coll.add_metadata(AVU(TrackedSample.NAME, "sample 0"))
        update = MetadataUpdate(
            experiment_name="simple_experiment_001", instrument_slot=1
        )
        update.update_secondary_metadata(mlwh_session=mlwh_session)
        assert AVU(TrackedSample.NAME, "sample 1") in coll.metadata()
        assert AVU(TrackedSample.NAME, "sample 0") not in coll.metadata()
        assert ont_history_in_meta(
            AVU.history(AVU(TrackedSample.NAME, "sample 0")), coll.metadata()
        )

    @m.context("When an attribute has multiple incorrect values")
    @m.it("Groups those values in the history metadata")
    def test_updates_multiple_metadata(self, ont_synthetic, mlwh_session):
        coll = Collection(
            ont_synthetic
            / "simple_experiment_001/20190904_1514_G100000_flowcell011_69126024"
        )
        coll.add_metadata(AVU(TrackedStudy.NAME, "Study A"))
        coll.add_metadata(AVU(TrackedStudy.NAME, "Study B"))
        update = MetadataUpdate(
            experiment_name="simple_experiment_001", instrument_slot=1
        )
        update.update_secondary_metadata(mlwh_session=mlwh_session)
        assert AVU(TrackedStudy.NAME, "Study Y") in coll.metadata()
        assert AVU(TrackedStudy.NAME, "Study A") not in coll.metadata()
        assert AVU(TrackedStudy.NAME, "Study B") not in coll.metadata()
        assert ont_history_in_meta(
            AVU(
                f"{TrackedStudy.NAME}_history",
                f"[{format_timestamp(datetime.utcnow())}] Study A,Study B",
            ),
            coll.metadata(),
        )
