# -*- coding: utf-8 -*-
#
# Copyright © 2021, 2022, 2023, 2024 Genome Research Ltd. All rights reserved.
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

from datetime import datetime
from pathlib import PurePath

from partisan.irods import AC, AVU, Collection, DataObject, Permission, format_timestamp
from pytest import mark as m, raises

from helpers import (
    LATEST,
    add_rods_path,
    history_in_meta,
    remove_rods_path,
    tests_have_admin,
)
from ont.conftest import ont_tag_identifier
from npg_irods import ont
from npg_irods.metadata.common import SeqConcept
from npg_irods.metadata.lims import (
    TrackedSample,
    TrackedStudy,
    ensure_consent_withdrawn,
)
from npg_irods.ont import (
    Component,
    Instrument,
    annotate_results_collection,
    apply_metadata,
    ensure_secondary_metadata_updated,
    is_minknow_report,
    barcode_collections,
)


class TestONTFindUpdates:
    @tests_have_admin
    @m.context("When an ONT metadata update is requested")
    @m.context("When no experiment name is specified")
    @m.context("When no time window is specified")
    @m.it("Finds all collections")
    def test_find_all(self, ont_synthetic_irods, ont_synthetic_mlwh):
        num_simple_expts = 5
        num_multiplexed_expts = 3
        num_slots = 5
        num_rebasecalled_multiplexed_expts = 2
        num_rebasecalled_slots = 1

        num_found, num_updated, num_errors = apply_metadata(
            mlwh_session=ont_synthetic_mlwh
        )
        num_expected = (
            (num_simple_expts * num_slots)
            + (num_multiplexed_expts * num_slots)
            + (num_rebasecalled_multiplexed_expts * num_rebasecalled_slots)
        )

        assert num_found == num_expected, f"Found {num_expected} collections"
        assert num_updated == num_expected
        assert num_errors == 0

    @m.context("When an ONT metadata update is requested")
    @m.context("When no experiment name is specified")
    @m.context("When a time window is specified")
    @m.it("Finds only collections updated in that time window")
    def test_find_recent_updates(self, ont_synthetic_irods, ont_synthetic_mlwh):
        num_found, num_updated, num_errors = apply_metadata(
            mlwh_session=ont_synthetic_mlwh, since=LATEST
        )

        # Only slots 1, 3 and 5 of multiplexed experiments 1 and 3 were updated in
        # the MLWH since time LATEST i.e.
        expected_colls = [
            Collection(ont_synthetic_irods / path)
            for path in [
                "multiplexed_experiment_001/20190904_1514_GA10000_flowcell101_cf751ba1",
                "multiplexed_experiment_001/20190904_1514_GA30000_flowcell103_cf751ba1",
                "multiplexed_experiment_001/20190904_1514_GA50000_flowcell105_cf751ba1",
                "multiplexed_experiment_003/20190904_1514_GA10000_flowcell101_cf751ba1",
                "multiplexed_experiment_003/20190904_1514_GA30000_flowcell103_cf751ba1",
                "multiplexed_experiment_003/20190904_1514_GA50000_flowcell105_cf751ba1",
                "old_rebasecalled_multiplexed_experiment_001/20190904_1514_GA10000_flowcell201_b4a1fd79",
                "rebasecalled_multiplexed_experiment_001/20190904_1514_GA10000_flowcell301_08c179cd",
            ]
        ]
        num_expected = len(expected_colls)

        assert num_found == num_expected, (
            f"Found {num_expected} collections "
            "(slots 1, 3 and 5 of multiplexed experiments 1 and 3)"
        )
        assert num_updated == num_expected
        assert num_errors == 0

    @m.context("When an ONT metadata update is requested")
    @m.context("When an experiment name is specified")
    @m.it("Finds only collections with that experiment name")
    def test_find_updates_for_experiment(self, ont_synthetic_irods, ont_synthetic_mlwh):
        expt = "simple_experiment_001"
        slot = 1

        num_found, num_updated, num_errors = apply_metadata(
            experiment_name=expt, mlwh_session=ont_synthetic_mlwh
        )

        expected_colls = [
            Collection(ont_synthetic_irods / path)
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

    @m.context("When an ONT metadata update is requested")
    @m.context("When an experiment name is specified")
    @m.context("When a slot position is specified")
    @m.it("Finds only collections with that experiment name and slot position")
    def test_find_updates_for_experiment_slot(
        self, ont_synthetic_irods, ont_synthetic_mlwh
    ):
        expt = "simple_experiment_001"
        slot = 1
        path = ont_synthetic_irods / expt / "20190904_1514_G100000_flowcell011_69126024"

        num_found, num_updated, num_errors = apply_metadata(
            experiment_name=expt, instrument_slot=slot, mlwh_session=ont_synthetic_mlwh
        )

        expected_colls = [Collection(path)]
        num_expected = len(expected_colls)

        assert (
            num_found == num_expected
        ), f"Found {num_expected} collections (slot 1 from simple experiment 1)"
        assert num_updated == num_expected
        assert num_errors == 0


class TestONTMetadataCreation(object):
    @tests_have_admin
    @m.context("When an ONT experiment collection is annotated")
    @m.context("When the experiment is single-sample")
    @m.it("Adds sample and study metadata to the run-folder collection")
    def test_add_new_sample_metadata(self, ont_synthetic_irods, ont_synthetic_mlwh):
        zone = "testZone"
        expt = "simple_experiment_001"
        slot = 1
        path = ont_synthetic_irods / expt / "20190904_1514_GA10000_flowcell011_69126024"

        c = Component(experiment_name=expt, instrument_slot=slot)

        assert annotate_results_collection(path, c, mlwh_session=ont_synthetic_mlwh)

        coll = Collection(path)
        for avu in [
            AVU(TrackedSample.ACCESSION_NUMBER, "ACC1"),
            AVU(TrackedSample.COMMON_NAME, "common_name1"),
            AVU(TrackedSample.DONOR_ID, "donor_id1"),
            AVU(TrackedSample.ID, "id_sample_lims1"),
            AVU(TrackedSample.NAME, "name1"),
            AVU(TrackedSample.SUPPLIER_NAME, "supplier_name1"),
            AVU(TrackedSample.PUBLIC_NAME, "public_name1"),
            AVU(TrackedStudy.ID, "2000"),
            AVU(TrackedStudy.NAME, "Study Y"),
        ]:
            assert avu in coll.metadata(), f"{avu} is in {coll} metadata"

        expected_acl = [
            AC("irods", Permission.OWN, zone=zone),
            AC("ss_2000", Permission.READ, zone=zone),
        ]
        assert coll.acl() == expected_acl, f"ACL of {coll} is { expected_acl}"
        for item in coll.contents():
            assert item.acl() == expected_acl, f"ACL of {item} is {expected_acl}"

    @tests_have_admin
    @m.context("When an ONT experiment collection is annotated")
    @m.context("When the experiment is multiplexed")
    @m.it("Adds {tag_index_from_id => <n>} metadata to barcode<0n> sub-collections")
    def test_add_new_plex_metadata(self, ont_synthetic_irods, ont_synthetic_mlwh):
        expt = "multiplexed_experiment_001"
        slot = 1
        path = ont_synthetic_irods / expt / "20190904_1514_GA10000_flowcell101_cf751ba1"

        c = Component(experiment_name=expt, instrument_slot=slot)

        assert annotate_results_collection(path, c, mlwh_session=ont_synthetic_mlwh)

        for subcoll in ["fast5_fail", "fast5_pass", "fastq_fail", "fastq_pass"]:
            for tag_index in range(1, 12):
                tag_identifier = ont_tag_identifier(tag_index)
                bc_coll = Collection(
                    path / subcoll / ont.barcode_name_from_id(tag_identifier)
                )
                avu = AVU(SeqConcept.TAG_INDEX, ont.tag_index_from_id(tag_identifier))
                assert avu in bc_coll.metadata(), f"{avu} is in {bc_coll} metadata"

    @tests_have_admin
    @m.context("When an ONT experiment collection is annotated")
    @m.context("When the experiment is multiplexed")
    @m.it("Adds sample and study metadata to barcode<0n> sub-collections")
    def test_add_new_plex_sample_metadata(
        self, ont_synthetic_irods, ont_synthetic_mlwh
    ):
        zone = "testZone"
        expt = "multiplexed_experiment_001"
        slot = 1
        path = ont_synthetic_irods / expt / "20190904_1514_GA10000_flowcell101_cf751ba1"

        c = Component(experiment_name=expt, instrument_slot=slot)

        assert annotate_results_collection(path, c, mlwh_session=ont_synthetic_mlwh)

        for subcoll in ["fast5_fail", "fast5_pass", "fastq_fail", "fastq_pass"]:
            for tag_index in range(1, 12):
                tag_id = ont_tag_identifier(tag_index)
                bc_coll = Collection(path / subcoll / ont.barcode_name_from_id(tag_id))

                for avu in [
                    AVU(TrackedSample.ACCESSION_NUMBER, f"ACC{tag_index}"),
                    AVU(TrackedSample.COMMON_NAME, f"common_name{tag_index}"),
                    AVU(TrackedSample.DONOR_ID, f"donor_id{tag_index}"),
                    AVU(TrackedSample.ID, f"id_sample_lims{tag_index}"),
                    AVU(TrackedSample.NAME, f"name{tag_index}"),
                    AVU(TrackedSample.PUBLIC_NAME, f"public_name{tag_index}"),
                    AVU(TrackedSample.SUPPLIER_NAME, f"supplier_name{tag_index}"),
                    AVU(TrackedStudy.ID, "3000"),
                    AVU(TrackedStudy.NAME, "Study Z"),
                ]:
                    assert avu in bc_coll.metadata(), f"{avu} is in {bc_coll} metadata"

                expected_acl = [
                    AC("irods", Permission.OWN, zone=zone),
                    AC("ss_3000", Permission.READ, zone=zone),
                ]

                assert bc_coll.acl() == expected_acl
                for item in bc_coll.contents():
                    assert item.acl() == expected_acl

    @tests_have_admin
    @m.context("When ONT experiment collections of rebasecalled data are annotated")
    @m.context("When experiments are multiplexed")
    @m.it("Adds tag_index, sample and study metadata to barcode<0n> sub-collections")
    def test_add_new_plex_metadata_on_rebasecalled(
        self, ont_synthetic_irods, ont_synthetic_mlwh
    ):
        zone = "testZone"
        slot = 1

        subpath = PurePath(
            "dorado",
            "7.2.13",
            "sup",
            "simplex",
            "normal",
            "default",
        )
        testdata = {
            "old_rebasecalled_multiplexed_experiment_001": PurePath(
                "old_rebasecalled_multiplexed_experiment_001",
                "20190904_1514_GA10000_flowcell201_b4a1fd79",
                subpath,
            ),
            "rebasecalled_multiplexed_experiment_001": PurePath(
                "rebasecalled_multiplexed_experiment_001",
                "20190904_1514_GA10000_flowcell301_08c179cd",
                subpath,
                "pass",
            ),
        }

        for expt, rel_path in testdata.items():
            path = ont_synthetic_irods / rel_path

            c = Component(experiment_name=expt, instrument_slot=slot)

            assert annotate_results_collection(path, c, mlwh_session=ont_synthetic_mlwh)

            for tag_index in range(1, 5):
                tag_identifier = ont_tag_identifier(tag_index)
                bpath = path / ont.barcode_name_from_id(tag_identifier)
                bc_coll = Collection(bpath)

                for avu in [
                    AVU(SeqConcept.TAG_INDEX, ont.tag_index_from_id(tag_identifier)),
                    AVU(TrackedSample.ACCESSION_NUMBER, f"ACC{tag_index}"),
                    AVU(TrackedSample.COMMON_NAME, f"common_name{tag_index}"),
                    AVU(TrackedSample.DONOR_ID, f"donor_id{tag_index}"),
                    AVU(TrackedSample.ID, f"id_sample_lims{tag_index}"),
                    AVU(TrackedSample.NAME, f"name{tag_index}"),
                    AVU(TrackedSample.PUBLIC_NAME, f"public_name{tag_index}"),
                    AVU(TrackedSample.SUPPLIER_NAME, f"supplier_name{tag_index}"),
                    AVU(TrackedStudy.ID, "3000"),
                    AVU(TrackedStudy.NAME, "Study Z"),
                ]:
                    assert avu in bc_coll.metadata(), f"{avu} is in {bc_coll} metadata"

                expected_acl = [
                    AC("irods", Permission.OWN, zone=zone),
                    AC("ss_3000", Permission.READ, zone=zone),
                ]

                assert bc_coll.acl() == expected_acl
                for item in bc_coll.contents():
                    assert item.acl() == expected_acl


class TestONTMetadataUpdate(object):
    @m.context("When ONT metadata are applied")
    @m.context("When the metadata are absent")
    @m.it("Adds the metadata")
    def test_updates_absent_metadata(self, ont_synthetic_irods, ont_synthetic_mlwh):
        expt = "simple_experiment_001"
        slot = 1
        path = ont_synthetic_irods / expt / "20190904_1514_G100000_flowcell011_69126024"

        coll = Collection(path)

        assert AVU(TrackedSample.NAME, "name1") not in coll.metadata()

        num_found, num_updated, num_errors = apply_metadata(
            experiment_name=expt, instrument_slot=slot, mlwh_session=ont_synthetic_mlwh
        )

        assert AVU(TrackedSample.NAME, "name1") in coll.metadata()
        assert num_found == 1
        assert num_updated == 1
        assert num_errors == 0

    @m.context("When ONT metadata are applied")
    @m.context("When correct metadata are already present")
    @m.it("Leaves the metadata unchanged")
    def test_updates_present_metadata(self, ont_synthetic_irods, ont_synthetic_mlwh):
        expt = "simple_experiment_001"
        slot = 1
        path = ont_synthetic_irods / expt / "20190904_1514_G100000_flowcell011_69126024"

        coll = Collection(path)
        coll.add_metadata(AVU(TrackedSample.NAME, "name1"))

        num_found, num_updated, num_errors = apply_metadata(
            experiment_name=expt, instrument_slot=slot, mlwh_session=ont_synthetic_mlwh
        )

        assert AVU(TrackedSample.NAME, "name1") in coll.metadata()
        assert num_found == 1
        assert num_updated == 1
        assert num_errors == 0

    @m.context("When ONT metadata are applied")
    @m.context("When incorrect metadata are present")
    @m.it("Changes the metadata and adds history metadata")
    def test_updates_changed_metadata(self, ont_synthetic_irods, ont_synthetic_mlwh):
        expt = "simple_experiment_001"
        slot = 1
        path = ont_synthetic_irods / expt / "20190904_1514_G100000_flowcell011_69126024"

        coll = Collection(path)
        coll.add_metadata(AVU(TrackedSample.NAME, "name0"))

        num_found, num_updated, num_errors = apply_metadata(
            experiment_name=expt, instrument_slot=slot, mlwh_session=ont_synthetic_mlwh
        )

        assert AVU(TrackedSample.NAME, "name1") in coll.metadata()
        assert AVU(TrackedSample.NAME, "name0") not in coll.metadata()
        assert history_in_meta(
            AVU.history(AVU(TrackedSample.NAME, "name0")), coll.metadata()
        )
        assert num_found == 1
        assert num_updated == 1
        assert num_errors == 0

    @m.context("When ONT metadata are applied")
    @m.context("When an attribute has multiple incorrect values")
    @m.it("Groups those values in the history metadata")
    def test_updates_multiple_metadata(self, ont_synthetic_irods, ont_synthetic_mlwh):
        expt = "simple_experiment_001"
        slot = 1
        path = ont_synthetic_irods / expt / "20190904_1514_G100000_flowcell011_69126024"

        coll = Collection(path)
        coll.add_metadata(AVU(TrackedStudy.NAME, "Study A"))
        coll.add_metadata(AVU(TrackedStudy.NAME, "Study B"))

        num_found, num_updated, num_errors = apply_metadata(
            experiment_name=expt, instrument_slot=slot, mlwh_session=ont_synthetic_mlwh
        )

        assert AVU(TrackedStudy.NAME, "Study Y") in coll.metadata()
        assert AVU(TrackedStudy.NAME, "Study A") not in coll.metadata()
        assert AVU(TrackedStudy.NAME, "Study B") not in coll.metadata()
        assert history_in_meta(
            AVU(
                f"{TrackedStudy.NAME}_history",
                f"[{format_timestamp(datetime.utcnow())}] Study A,Study B",
            ),
            coll.metadata(),
        )
        assert num_found == 1
        assert num_updated == 1
        assert num_errors == 0

    @m.context("When ONT metadata are updated")
    @m.context("When an iRODS path has metadata identifying its run component")
    @m.it("Updates the metadata")
    def test_updates_annotated_collection(
        self, ont_synthetic_irods, ont_synthetic_mlwh
    ):
        expt = "simple_experiment_001"
        slot = 1
        path = ont_synthetic_irods / expt / "20190904_1514_G100000_flowcell011_69126024"

        coll = Collection(path)
        coll.add_metadata(
            AVU(Instrument.EXPERIMENT_NAME, expt), AVU(Instrument.INSTRUMENT_SLOT, slot)
        )

        assert AVU(TrackedSample.NAME, "name1") not in coll.metadata()
        assert ensure_secondary_metadata_updated(coll, mlwh_session=ont_synthetic_mlwh)
        assert AVU(TrackedSample.NAME, "name1") in coll.metadata()

    @m.context("When rebasecalled ONT metadata are updated")
    @m.context("When an iRODS path has metadata identifying its run component")
    @m.it("Updates the metadata")
    def test_updates_rebasecalled_annotated_collection(
        self, ont_synthetic_irods, ont_synthetic_mlwh
    ):
        slot = 1
        subpath = PurePath(
            "dorado",
            "7.2.13",
            "sup",
            "simplex",
            "normal",
            "default",
        )
        testdata = {
            "old_rebasecalled_multiplexed_experiment_001": {
                "runfolder": PurePath(
                    "old_rebasecalled_multiplexed_experiment_001",
                    "20190904_1514_GA10000_flowcell201_b4a1fd79",
                    subpath,
                ),
                "subfolder": "",
            },
            "rebasecalled_multiplexed_experiment_001": {
                "runfolder": PurePath(
                    "rebasecalled_multiplexed_experiment_001",
                    "20190904_1514_GA10000_flowcell301_08c179cd",
                    subpath,
                ),
                "subfolder": "pass",
            },
        }

        for expt in testdata.keys():
            path = ont_synthetic_irods / testdata[expt]["runfolder"]
            coll = Collection(path)
            coll.add_metadata(
                AVU(Instrument.EXPERIMENT_NAME, expt),
                AVU(Instrument.INSTRUMENT_SLOT, slot),
            )

            samples_paths: tuple[str, Collection] = []
            for tag_index in range(1, 5):
                tag_identifier = ont_tag_identifier(tag_index)
                bpath = (
                    path
                    / testdata[expt]["subfolder"]
                    / ont.barcode_name_from_id(tag_identifier)
                )
                bcoll = Collection(bpath)
                samples_paths.append((f"name{tag_index}", bcoll))

            for sample_name, _ in samples_paths:
                assert AVU(TrackedSample.NAME, sample_name) not in coll.metadata()
            for sample_name, bcoll in samples_paths:
                assert AVU(TrackedSample.NAME, sample_name) not in bcoll.metadata()
            assert ensure_secondary_metadata_updated(
                coll, mlwh_session=ont_synthetic_mlwh
            )
            for sample_name, bcoll in samples_paths:
                assert AVU(TrackedSample.NAME, sample_name) in bcoll.metadata()


class TestONTPermissionsUpdate:
    @tests_have_admin
    @m.context("When ONT permissions are updated")
    @m.context("When the experiment is multiplexed")
    @m.it("Makes report files publicly readable")
    def test_public_read_reports(self, ont_synthetic_irods, ont_synthetic_mlwh):
        zone = "testZone"
        expt = "multiplexed_experiment_001"
        slot = 1
        path = ont_synthetic_irods / expt / "20190904_1514_GA10000_flowcell101_cf751ba1"

        c = Component(experiment_name=expt, instrument_slot=slot)
        expected_acl = [
            AC("irods", Permission.OWN, zone=zone),
            AC("public", Permission.READ, zone=zone),
        ]

        assert annotate_results_collection(path, c, mlwh_session=ont_synthetic_mlwh)

        for ext in ["html", "md", "json.gz"]:
            assert (
                DataObject(path / f"report_multiplexed_synthetic.{ext}").acl()
                == expected_acl
            )

    @m.context("When ONT permissions are updated")
    @m.context("When the experiment is single-sample")
    @m.context("When permissions are absent")
    @m.it("Add study-specific permissions")
    def test_updates_absent_study_permissions(
        self, ont_synthetic_irods, ont_synthetic_mlwh
    ):
        zone = "testZone"
        expt = "simple_experiment_001"
        slot = 1
        path = ont_synthetic_irods / expt / "20190904_1514_G100000_flowcell011_69126024"

        coll = Collection(path)
        coll.add_metadata(
            AVU(Instrument.EXPERIMENT_NAME, expt), AVU(Instrument.INSTRUMENT_SLOT, slot)
        )

        assert coll.permissions() == [AC("irods", perm=Permission.OWN, zone=zone)]
        assert ensure_secondary_metadata_updated(coll, mlwh_session=ont_synthetic_mlwh)
        assert coll.permissions() == [
            AC("irods", perm=Permission.OWN, zone=zone),
            AC("ss_2000", perm=Permission.READ, zone=zone),
        ]

    @m.context("When ONT permissions are updated")
    @m.context("When data are single-sample")
    @m.context("When the permissions are already present")
    @m.it("Leaves the permissions unchanged")
    def test_updates_present_study_permissions(
        self, ont_synthetic_irods, ont_synthetic_mlwh
    ):
        zone = "testZone"
        expt = "simple_experiment_001"
        slot = 1
        path = ont_synthetic_irods / expt / "20190904_1514_G100000_flowcell011_69126024"

        coll = Collection(path)
        coll.add_metadata(
            AVU(Instrument.EXPERIMENT_NAME, expt), AVU(Instrument.INSTRUMENT_SLOT, slot)
        )
        expected_acl = [
            AC("irods", perm=Permission.OWN, zone=zone),
            AC("ss_2000", perm=Permission.READ, zone=zone),
        ]
        coll.add_permissions(*expected_acl)

        assert coll.permissions() == expected_acl
        assert ensure_secondary_metadata_updated(coll, mlwh_session=ont_synthetic_mlwh)
        assert coll.permissions() == expected_acl

    @m.context("When ONT permissions are updated")
    @m.context("When the experiment is single-sample")
    @m.context("When incorrect permissions are present")
    @m.it("Updated the permissions")
    def test_updates_changed_study_permissions(
        self, ont_synthetic_irods, ont_synthetic_mlwh
    ):
        zone = "testZone"
        expt = "simple_experiment_001"
        slot = 1
        path = ont_synthetic_irods / expt / "20190904_1514_G100000_flowcell011_69126024"

        coll = Collection(path)
        coll.add_metadata(
            AVU(Instrument.EXPERIMENT_NAME, expt), AVU(Instrument.INSTRUMENT_SLOT, slot)
        )
        coll.add_permissions(AC("ss_1000", Permission.READ, zone=zone), recurse=True)

        assert coll.permissions() == [
            AC("irods", perm=Permission.OWN, zone=zone),
            AC("ss_1000", Permission.READ, zone=zone),
        ]
        assert ensure_secondary_metadata_updated(coll, mlwh_session=ont_synthetic_mlwh)
        assert coll.permissions() == [
            AC("irods", perm=Permission.OWN, zone=zone),
            AC("ss_2000", perm=Permission.READ, zone=zone),
        ]

    @m.context("When ONT permissions are updated")
    @m.context("When the experiment is single-sample")
    @m.context("When data have had consent withdrawn")
    @m.it("Does not restore access permissions")
    def test_retains_consent_withdrawn(self, ont_synthetic_irods, ont_synthetic_mlwh):
        zone = "testZone"
        expt = "simple_experiment_001"
        slot = 1
        path = ont_synthetic_irods / expt / "20190904_1514_G100000_flowcell011_69126024"

        c = Component(experiment_name=expt, instrument_slot=slot)
        expected_acl = [AC("irods", perm=Permission.OWN, zone=zone)]

        coll = Collection(path)
        assert ensure_consent_withdrawn(coll)
        assert coll.acl() == expected_acl

        assert annotate_results_collection(path, c, mlwh_session=ont_synthetic_mlwh)

        assert coll.acl() == expected_acl, f"ACL of {coll} is {expected_acl}"
        for item in coll.contents(acl=True, recurse=True):
            assert item.acl() == expected_acl, f"ACL of {item} is {expected_acl}"

    @m.context("When ONT permissions are updated")
    @m.context("When the experiment is multiplexed")
    @m.context("When data have had consent withdrawn")
    @m.it("Does not restore access permissions")
    def test_retains_consent_withdrawn_mx(
        self, ont_synthetic_irods, ont_synthetic_mlwh
    ):
        zone = "testZone"
        expt = "multiplexed_experiment_001"
        slot = 1
        path = ont_synthetic_irods / expt / "20190904_1514_GA10000_flowcell101_cf751ba1"

        c = Component(experiment_name=expt, instrument_slot=slot)
        expected_acl = [AC("irods", perm=Permission.OWN, zone=zone)]
        expected_report_acl = [
            AC("irods", perm=Permission.OWN, zone=zone),
            AC("public", perm=Permission.READ, zone=zone),
        ]

        coll = Collection(path)
        sub_colls = ["fast5_fail", "fast5_pass", "fastq_fail", "fastq_pass"]
        bc_colls = [
            it
            for it in coll.contents(recurse=True)
            if it.rods_type == Collection and "barcode" in it.path.name
        ]
        assert len(bc_colls) == 12 * len(sub_colls)

        for bc_coll in bc_colls:
            assert ensure_consent_withdrawn(
                bc_coll
            )  # Mark barcode collections as consent withdrawn

            assert (
                bc_coll.acl() == expected_acl
            ), f"ACL of barcode collection {bc_coll} is {expected_acl}"
            for item in bc_coll.contents(acl=True, recurse=True):
                assert (
                    item.acl() == expected_acl
                ), f"ACL of barcode collection member {item} is {expected_acl}"

        assert annotate_results_collection(path, c, mlwh_session=ont_synthetic_mlwh)

        for bc_coll in bc_colls:
            assert bc_coll.acl() == expected_acl, f"ACL of {c} is {expected_acl}"
            for item in bc_coll.contents(acl=True, recurse=True):
                assert (
                    item.acl() == expected_acl
                ), f"ACL of barcode collection member {item} is {expected_acl}"

        assert (
            coll.acl() == expected_acl
        ), f"ACL of root collection {coll} is {expected_acl}"
        for item in coll.contents(acl=True, recurse=True):
            if is_minknow_report(item):
                assert (
                    item.acl() == expected_report_acl
                ), f"ACL of MinKNOW report {item} is {expected_report_acl}"
            else:
                assert (
                    item.acl() == expected_acl
                ), f"ACL of root collection member {item} is {expected_acl}"


class TestBarcodeRelatedFunctions(object):
    @m.context("When rebasecalled ONT runs are plexed")
    @m.context("When barcode folders lie one level down in the output folder")
    @m.it("Barcode collections number is correct")
    def test_barcode_collections_under_subfolder(self):
        expected_bcolls = 5
        root_path = PurePath(
            "/testZone/home/irods/test/ont_synthetic_irods/synthetic/barcode_collection_test"
        )
        expt = "multiplexed_folder_experiment_001"
        path = root_path / expt / "20190904_1514_GA10000_flowcell401_ba641ab1"
        tag_identifiers = [ont_tag_identifier(tag_index) for tag_index in range(1, 6)]
        for tag_identifier in tag_identifiers:
            bpath = path / "pass" / ont.barcode_name_from_id(tag_identifier)
            Collection(bpath).create(parents=True)

        bcolls = barcode_collections(Collection(path), *tag_identifiers)
        assert len(bcolls) == expected_bcolls
        remove_rods_path(root_path)

    @m.context("When rebasecalled ONT runs are plexed")
    @m.context("When barcodes are right under the output folder")
    @m.it("Barcode collections number is correct")
    def test_barcode_collections_under_output_folder(self):
        expected_bcolls = 5
        root_path = PurePath(
            "/testZone/home/irods/test/ont_synthetic_irods/synthetic/barcode_collection_test"
        )
        expt = "multiplexed_folder_experiment_002"
        path = root_path / expt / "20190904_1514_GA10000_flowcell402_ca641bc1"
        tag_identifiers = [ont_tag_identifier(tag_index) for tag_index in range(1, 6)]
        for tag_identifier in tag_identifiers:
            bpath = path / ont.barcode_name_from_id(tag_identifier)
            Collection(bpath).create(parents=True)

        bcolls = barcode_collections(Collection(path), *tag_identifiers)
        assert len(bcolls) == expected_bcolls
        remove_rods_path(root_path)

    @m.context("When rebasecalled ONT runs are plexed")
    @m.context("When the barcode folder is duplicated under a barcode collection")
    @m.it("Raises exception for duplicated barcode folders")
    def test_barcode_collections_duplicates(self):
        root_path = PurePath(
            "/testZone/home/irods/test/ont_synthetic_irods/synthetic/barcode_collection_test"
        )
        expt = "multiplexed_folder_experiment_003"
        path = root_path / expt / "20190904_1514_GA10000_flowcell403_de531cf1"
        tag_identifiers = [ont_tag_identifier(tag_index) for tag_index in range(1, 6)]
        for tag_identifier in tag_identifiers:
            barcode_name = ont.barcode_name_from_id(tag_identifier)
            bpath = path / barcode_name / barcode_name
            Collection(bpath).create(parents=True)
        with raises(ValueError):
            barcode_collections(Collection(path), *tag_identifiers)
        remove_rods_path(root_path)

    @m.context("When rebasecalled ONT runs are plexed")
    @m.context(
        "When some barcode folders are missing although they were used in the lab"
    )
    @m.it("Workflow continues with no error")
    def test_barcode_collections_missing_folders(self):
        expected_bcolls = 3
        root_path = PurePath(
            "/testZone/home/irods/test/ont_synthetic_irods/synthetic/barcode_collection_test"
        )
        expt = "multiplexed_folder_experiment_004"
        path = root_path / expt / "20190904_1514_GA10000_flowcell404_fg345hil"
        expected_tag_identifiers = [
            ont_tag_identifier(tag_index) for tag_index in range(1, 6)
        ]
        actual_tag_identifies = [
            ont_tag_identifier(tag_index) for tag_index in [1, 3, 5]
        ]
        for tag_identifier in actual_tag_identifies:
            bpath = path / ont.barcode_name_from_id(tag_identifier)
            Collection(bpath).create(parents=True)

        bcolls = barcode_collections(Collection(path), *expected_tag_identifiers)
        assert len(bcolls) == expected_bcolls
        remove_rods_path(root_path)
