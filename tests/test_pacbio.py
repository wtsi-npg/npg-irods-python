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
# @author Michael Kubiak <mk35@sanger.ac.uk>

import json
import os

from npg_id_generation.pac_bio import PacBioEntity
from pytest import mark as m

from npg_irods.metadata.common import DataFile
from npg_irods.metadata.lims import has_id_product_metadata, SeqConcept
from npg_irods.metadata.pacbio import (
    ensure_id_product,
    Instrument,
    backfill_id_products,
)
from npg_irods.mlwh_locations.pacbio import write_mlwh_json
from partisan.irods import DataObject, AVU


@m.describe("Ml Warehouse Locations file writer")
class TestWriteMlwhJson:
    @m.context("When no locations are provided")
    @m.it("Returns False and does not write")
    def test_write_mlwh_json_no_location(self, tmp_path):
        path = str(tmp_path / "empty.json")
        assert not write_mlwh_json({}, path)
        assert not os.path.exists(path)

    @m.context("When provided with a correct set of locations")
    @m.context(
        "When there are no objects where both root collection and id_product are the same"
    )
    @m.it("Returns True and writes the correct file")
    def test_write_mlwh_json_distinct(self, tmp_path):
        expected_path = "tests/data/pacbio/no_duplicates.json"
        actual_path = str(tmp_path / "no_duplicates.json")
        assert write_mlwh_json(
            {
                "/testZone/home/irods/pacbio/run/A01/test.bam": "abcdef123456",
                "/testZone/home/irods/pacbio/run/A01/test2.bam": "ghijkl78901",
                "/testZone/home/irods/pacbio/run/B01/test2.bam": "mnopq234567",
            },
            actual_path,
        )
        with open(actual_path) as actual, open(expected_path) as expected:
            assert json.load(actual) == json.load(expected)

    @m.context("When provided with a correct set of locations")
    @m.context(
        "When some entries share both their root collection and their id_product"
    )
    @m.it("Returns True and writes the correct file")
    def test_write_mlwh_json_secondary(self, tmp_path):
        expected_path = "tests/data/pacbio/duplicates.json"
        actual_path = str(tmp_path / "duplicates.json")
        assert write_mlwh_json(
            {
                "/testZone/home/irods/pacbio/run/A01/test.bam": "abcdef123456",
                "/testZone/home/irods/pacbio/run/B01/test2.bam": "ghijkl78901",
                "/testZone/home/irods/pacbio/run/B01/extra.bam": "ghijkl78901",
            },
            actual_path,
        )
        with open(actual_path) as actual, open(expected_path) as expected:
            assert json.load(actual) == json.load(expected)


@m.describe("Product ID metadata")
class TestIDProductMetadata:
    @m.context("When id_product metadata are present")
    @m.context("When has_ function is called")
    @m.it("Returns True")
    def test_has_metadata_present(self, pacbio_has_id):
        obj = DataObject(pacbio_has_id)

        obj.add_metadata(AVU(SeqConcept.ID_PRODUCT, "abcde12345"))
        assert has_id_product_metadata(obj)

    @m.context("When id_product metadata are absent")
    @m.context("When has_ function is called")
    @m.it("Returns False")
    def test_has_metadata_absent(self, pacbio_requires_id):
        obj = DataObject(pacbio_requires_id)

        assert not has_id_product_metadata(obj)

    @m.context("When id_product metadata are present")
    @m.context("When ensure_ function is called without overwrite flag")
    @m.it("Returns True without overwriting the metadata")
    def test_ensure_metadata_present(self, pacbio_has_id):
        obj = DataObject(pacbio_has_id)

        obj.add_metadata(AVU(SeqConcept.ID_PRODUCT, "abcde12345"))
        assert ensure_id_product(obj)

        for avu in obj.metadata():
            if avu.attribute == SeqConcept.ID_PRODUCT:
                assert avu.value == "abcde12345"

    @m.context("When id_product metadata are present")
    @m.context("When ensure_ function is called with overwrite flag")
    @m.it("Overwrites id_product metadata with the correct value and returns True")
    def test_ensure_metadata_present(self, pacbio_has_id):
        obj = DataObject(pacbio_has_id)

        obj.add_metadata(AVU(Instrument.RUN_NAME, "RUN-01"))
        obj.add_metadata(AVU(Instrument.WELL_LABEL, "A01"))
        assert ensure_id_product(obj, overwrite=True)

        expected_id_product = PacBioEntity(
            run_name="RUN-01", well_label="A1"
        ).hash_product_id()
        for avu in obj.metadata():
            if avu.attribute == SeqConcept.ID_PRODUCT:
                assert avu.value == expected_id_product

    @m.context("When id_product metadata are absent")
    @m.context("When id_product metadata are required")
    @m.context("When target metadata are present")
    @m.context("When ensure_ function is called")
    @m.it("Adds id_product metadata generated with tag_sequence and returns True")
    def test_ensure_metadata_absent(self, pacbio_requires_id):
        obj = DataObject(pacbio_requires_id)

        obj.add_metadata(AVU(DataFile.TARGET, "1"))
        obj.add_metadata(AVU(Instrument.RUN_NAME, "RUN-01"))
        obj.add_metadata(AVU(Instrument.WELL_LABEL, "A01"))
        obj.add_metadata(AVU(Instrument.TAG_SEQUENCE, "ACTCAGTC"))
        assert ensure_id_product(obj)

        expected_id_product = PacBioEntity(
            run_name="RUN-01", well_label="A1", tags="ACTCAGTC"
        ).hash_product_id()
        present = False
        for avu in obj.metadata():
            if avu.attribute == SeqConcept.ID_PRODUCT.value:
                assert avu.value == expected_id_product
                present = True
        assert present

    @m.context("When id_product metadata are absent")
    @m.context("When id_product metadata are not required")
    @m.context("When ensure_ function is called")
    @m.it("Returns False")
    def test_ensure_metadata_absent_not_required(self, annotated_data_object):
        obj = DataObject(annotated_data_object)

        assert not ensure_id_product(obj)

    @m.context("When id_product metadata are present")
    @m.context("When backfill_id_products is run")
    @m.it("Returns True and makes no change")
    def test_backfill_id_products_present(self, pacbio_has_id, tmp_path):
        assert backfill_id_products([pacbio_has_id], str(tmp_path / "empty.json"))

        obj = DataObject(pacbio_has_id)
        for avu in obj.metadata():
            if avu.attribute == SeqConcept.ID_PRODUCT:
                assert avu.value == "abcde12345"

    @m.context("When id_product metadata are absent")
    @m.context("When backfill_id_products is run")
    @m.it("Returns True, adds correct metadata and outputs correct json file")
    def test_backfill_id_products_absent(self, pacbio_requires_id, tmp_path):
        obj = DataObject(pacbio_requires_id)
        obj.add_metadata(AVU(Instrument.RUN_NAME, "RUN-01"))
        obj.add_metadata(AVU(Instrument.WELL_LABEL, "A01"))

        expected_path = "tests/data/pacbio/backfill.json"
        actual_path = str(tmp_path / "backfill.json")

        assert backfill_id_products([pacbio_requires_id], actual_path)

        expected_id_product = PacBioEntity(
            run_name="RUN-01", well_label="A1"
        ).hash_product_id()
        present = False
        for avu in obj.metadata():
            if avu.attribute == SeqConcept.ID_PRODUCT.value:
                assert avu.value == expected_id_product
                present = True
        assert present

        with open(actual_path) as actual, open(expected_path) as expected:
            assert expected_path == "tests/data/pacbio/backfill.json"
            assert json.load(actual) == json.load(expected)
