# -*- coding: utf-8 -*-
#
# Copyright © 2022 Genome Research Ltd. All rights reserved.
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

from pytest import mark as m, raises

from npg_irods.mlwh_locations import illumina
from typing import Dict

from partisan.irods import Collection
import json
from multiprocessing import Pool


def assert_excluded_object(obj_path: str):
    with raises(illumina.ExcludedObjectException):
        illumina.create_product_dict(obj_path, "cram")


class ApplyResult:  # Mock class because pytest doesn't like directly multiprocessing
    def __init__(self, dictionary: Dict):
        self.d = dictionary

    def get(self, timeout: int):
        if "fail_missing_meta" in self.d.keys():
            raise illumina.MissingMetadataError
        if "fail_excluded_object" in self.d.keys():
            raise illumina.ExcludedObjectException
        if "fail_io" in self.d.keys():
            raise IOError
        return self.d


def applied_function(product_dict: Dict):
    if "missing_meta" in product_dict.keys():
        raise illumina.MissingMetadataError
    if "excluded_object" in product_dict.keys():
        raise illumina.ExcludedObjectException
    if "unexpected_error" in product_dict.keys():
        raise IOError
    return product_dict


@m.describe("Making a product dictionary for a data object")
class TestCreateProductDict:
    @m.context("When the data object has expected metadata")
    @m.it("Creates an accurate dictionary")
    def test_expected_object(self, illumina_products):
        expected = {
            "seq_platform_name": "illumina",
            "pipeline_name": illumina.NPG_PROD,
            "irods_root_collection": f"{illumina_products}/12345",
            "irods_data_relative_path": "12345#1.cram",
            "id_product": "31a3d460bb3c7d98845187c716a30db81c44b615",
        }
        assert (
            illumina.create_product_dict(
                illumina_products / "12345/12345#1.cram", "cram"
            )
            == expected
        )

    @m.context("When the data object has alt_process metadata")
    @m.it("Sets pipeline_name as npg-prod-alt-process")
    def test_alt_process_object(self, illumina_products):
        expected = {
            "seq_platform_name": "illumina",
            "pipeline_name": "alt_Alternative Process",
            "irods_root_collection": f"{illumina_products}/12345",
            "irods_data_relative_path": "12345#2.cram",
            "id_product": "0b3bd00f1d186247f381aa87e213940b8c7ab7e5",
        }
        assert (
            illumina.create_product_dict(
                illumina_products / "12345/12345#2.cram", "cram"
            )
            == expected
        )

    @m.context("When the data object has any subset in its metadata")
    @m.it("Fails with an ExcludedObjectException")
    def test_subset_object(self, illumina_products):
        assert_excluded_object(illumina_products / "12345/12345#1_phix.cram")

    @m.context("When the data object has tag 0")
    @m.it("Fails with an ExcludedObjectException")
    def test_tag_0_object(self, illumina_products):
        assert_excluded_object(illumina_products / "12345/12345#0.cram")

    @m.context("When the data object uses a PhiX reference")
    @m.it("Fails with an ExcludedObjectException")
    def test_phix_reference_object(self, illumina_products):
        assert_excluded_object(illumina_products / "12345/12345#888.cram")

    @m.context("When the data object is from a 10x collection")
    @m.it("Fails with an ExcludedObjectException")
    def test_10x_object(self, illumina_products):
        assert_excluded_object(illumina_products / "12345/cellranger/12345.cram")

    @m.context("When the object does not have the requested extension")
    @m.it("Fails with an ExcludedObjectException")
    def test_wrong_extension(self, illumina_products):
        assert_excluded_object(illumina_products / "12345/12345#1.bam")


@m.describe("Extracting product dictionaries from results of multiprocessing")
class TestExtractProducts:
    @m.xfail(
        reason="pytest doesn't like multiprocessing - see https://github.com/pytest-dev/pytest/issues/958"
    )
    def test_product_result_using_pool(self, illumina_products):
        expected = [
            {
                "seq_platform_name": "illumina",
                "pipeline_name": illumina.NPG_PROD,
                "irods_root_collection": f"{illumina_products}/12345",
                "irods_data_relative_path": "12345#1.cram",
                "id_product": "31a3d460bb3c7d98845187c716a30db81c44b615",
            }
        ]
        with Pool(1) as p:
            results = [p.apply_async(applied_function, product) for product in expected]
        assert illumina.extract_products(results, timeout=10) == expected

    @m.context("When the result contains a dict")
    @m.it("Returns a list containing that dict")
    def test_product_result(self, illumina_products):
        expected = [
            {
                "seq_platform_name": "illumina",
                "pipeline_name": illumina.NPG_PROD,
                "irods_root_collection": f"{illumina_products}/12345",
                "irods_data_relative_path": "12345#1.cram",
                "id_product": "31a3d460bb3c7d98845187c716a30db81c44b615",
            }
        ]
        results = [ApplyResult(expected[0])]
        assert illumina.extract_products(results) == expected

    @m.context("When the result contains an expected error")
    @m.it("Returns an empty list")
    def test_expected_error_result(self):
        missing_meta_results = [ApplyResult({"fail_missing_meta": True})]
        excluded_object_results = [ApplyResult({"fail_excluded_object": True})]
        assert illumina.extract_products(missing_meta_results) == []
        assert illumina.extract_products(excluded_object_results) == []

    @m.context("When the result contains an unexpected error")
    @m.it("Fails")
    def test_unexpected_error_result(self):
        results = [ApplyResult({"fail_io": True})]
        with raises(IOError):
            illumina.extract_products(results)

    @m.context("When there are multiple results, some expected errors")
    @m.it("Produces a list of the good results and handles the errors")
    def test_mixed_result(self, illumina_products):
        expected = [
            {
                "seq_platform_name": "illumina",
                "pipeline_name": illumina.NPG_PROD,
                "irods_root_collection": f"{illumina_products}/12345",
                "irods_data_relative_path": "12345#1.cram",
                "id_product": "31a3d460bb3c7d98845187c716a30db81c44b615",
            },
            {
                "seq_platform_name": "illumina",
                "pipeline_name": "alt_Alternative Process",
                "irods_root_collection": f"{illumina_products}/12345",
                "irods_data_relative_path": "12345#2.cram",
                "id_product": "0b3bd00f1d186247f381aa87e213940b8c7ab7e5",
            },
            {
                "seq_platform_name": "illumina",
                "pipeline_name": illumina.NPG_PROD,
                "irods_root_collection": f"{illumina_products}/54321",
                "irods_data_relative_path": "54321#1.bam",
                "id_product": "1a08a7027d9f9c20d01909989370ea6b70a5bccc",
            },
        ]
        results = (
            [ApplyResult(product) for product in expected]
            + [ApplyResult({"fail_excluded_object": True})]
            + [ApplyResult({"fail_missing_meta": True})]
        )
        assert illumina.extract_products(results) == expected


@m.describe("Making a list of product dictionaries for a collection")
class TestFindProducts:
    @m.context("When the collection has a mixture of included and excluded objects")
    @m.it("Includes the correct objects")
    def test_mixed_coll(self, illumina_products):
        expected = [
            {
                "seq_platform_name": "illumina",
                "pipeline_name": illumina.NPG_PROD,
                "irods_root_collection": f"{illumina_products}/12345",
                "irods_data_relative_path": "12345#1.cram",
                "id_product": "31a3d460bb3c7d98845187c716a30db81c44b615",
            },
            {
                "seq_platform_name": "illumina",
                "pipeline_name": "alt_Alternative Process",
                "irods_root_collection": f"{illumina_products}/12345",
                "irods_data_relative_path": "12345#2.cram",
                "id_product": "0b3bd00f1d186247f381aa87e213940b8c7ab7e5",
            },
        ]
        assert (
            illumina.find_products(Collection(illumina_products / "12345"), 4)
            == expected
        )

    @m.context("When the collection does not contain any included cram files")
    @m.it("Includes bam files instead")
    def test_bam_only_coll(self, illumina_products):
        expected = [
            {
                "seq_platform_name": "illumina",
                "pipeline_name": illumina.NPG_PROD,
                "irods_root_collection": f"{illumina_products}/54321",
                "irods_data_relative_path": "54321#1.bam",
                "id_product": "1a08a7027d9f9c20d01909989370ea6b70a5bccc",
            }
        ]
        assert (
            illumina.find_products(Collection(illumina_products / "54321"), 1)
            == expected
        )


@m.describe(
    "Writing a file with entries for each product in a list of collection paths"
)
class TestGenerateFiles:
    @m.context("When all collections exist")
    @m.it(
        "Writes a file containing product information for objects in those collections"
    )
    def test_existing_colls(self, illumina_products, tmp_path):
        expected = {
            "version": illumina.JSON_FILE_VERSION,
            "products": [
                {
                    "seq_platform_name": "illumina",
                    "pipeline_name": illumina.NPG_PROD,
                    "irods_root_collection": f"{illumina_products}/12345",
                    "irods_data_relative_path": "12345#1.cram",
                    "id_product": "31a3d460bb3c7d98845187c716a30db81c44b615",
                },
                {
                    "seq_platform_name": "illumina",
                    "pipeline_name": "alt_Alternative Process",
                    "irods_root_collection": f"{illumina_products}/12345",
                    "irods_data_relative_path": "12345#2.cram",
                    "id_product": "0b3bd00f1d186247f381aa87e213940b8c7ab7e5",
                },
                {
                    "seq_platform_name": "illumina",
                    "pipeline_name": illumina.NPG_PROD,
                    "irods_root_collection": f"{illumina_products}/54321",
                    "irods_data_relative_path": "54321#1.bam",
                    "id_product": "1a08a7027d9f9c20d01909989370ea6b70a5bccc",
                },
            ],
        }
        json_path = f"{tmp_path}/existing_colls.json"
        illumina.generate_files(
            [illumina_products / "12345", illumina_products / "54321"],
            4,
            json_path,
        )
        with open(json_path) as json_file:
            assert json.load(json_file) == expected

    @m.context("When a mixture of real and fake collections are passed")
    @m.it("Adds objects from the existing collections to the list of products")
    def test_coll_mixture(self, illumina_products, tmp_path):
        expected = {
            "version": illumina.JSON_FILE_VERSION,
            "products": [
                {
                    "seq_platform_name": "illumina",
                    "pipeline_name": illumina.NPG_PROD,
                    "irods_root_collection": f"{illumina_products}/12345",
                    "irods_data_relative_path": "12345#1.cram",
                    "id_product": "31a3d460bb3c7d98845187c716a30db81c44b615",
                },
                {
                    "seq_platform_name": "illumina",
                    "pipeline_name": "alt_Alternative Process",
                    "irods_root_collection": f"{illumina_products}/12345",
                    "irods_data_relative_path": "12345#2.cram",
                    "id_product": "0b3bd00f1d186247f381aa87e213940b8c7ab7e5",
                },
            ],
        }
        json_path = f"{tmp_path}/mixed_colls.json"
        illumina.generate_files(
            [
                illumina_products / "12345",
                illumina_products / "fake_file",
                illumina_products / "23456",
            ],
            4,
            json_path,
        )
        with open(json_path) as json_file:
            assert json.load(json_file) == expected
