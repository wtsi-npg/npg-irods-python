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

from npg_id_generation.pac_bio import PacBioEntity
from pytest import mark as m

from npg_irods.metadata.lims import has_id_product_metadata, SeqConcept
from npg_irods.metadata.pacbio import ensure_id_product, Instrument
from partisan.irods import DataObject, AVU


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
            if avu.attribute() == SeqConcept.ID_PRODUCT:
                assert avu.value() == "abcde12345"

    @m.context("When id_product metadata are present")
    @m.context("When ensure_ function is called with overwrite flag")
    @m.it("Overwrites id_product metadata with the correct value and returns True")
    def test_ensure_metadata_present(self, pacbio_has_id):
        obj = DataObject(pacbio_has_id)

        obj.add_metadata(AVU(Instrument.RUN_NAME, "RUN-01"))
        obj.add_metadata(AVU(Instrument.WELL_LABEL, "A01"))
        assert ensure_id_product(obj, overwrite=True)

        expected_id_product = PacBioEntity(run_name="RUN-01", well_label="A1").hash_product_id()
        for avu in obj.metadata():
            if avu.attribute() == SeqConcept.ID_PRODUCT:
                assert avu.value() == expected_id_product

    @m.context("When id_product metadata are absent")
    @m.context("When id_product metadata are required")
    @m.context("When ensure_ function is called")
    @m.it("Adds id_product metadata and returns True")
    def test_ensure_metadata_absent(self, pacbio_requires_id):
        obj = DataObject(pacbio_requires_id)

        obj.add_metadata(AVU(Instrument.RUN_NAME, "RUN-01"))
        obj.add_metadata(AVU(Instrument.WELL_LABEL, "A01"))
        assert ensure_id_product(obj)

        expected_id_product = PacBioEntity(run_name="RUN-01", well_label="A1").hash_product_id()
        for avu in obj.metadata():
            if avu.attribute() == SeqConcept.ID_PRODUCT:
                assert avu.value() == expected_id_product

    @m.context("When id_product metadata are absent")
    @m.context("When id_product metadata are not required")
    @m.context("When ensure_ function is called")
    @m.it("Returns False")
    def test_ensure_metadata_absent_not_required(self, annotated_data_object):
        obj = DataObject(annotated_data_object)

        assert not ensure_id_product(obj)
