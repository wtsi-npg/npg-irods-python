import json
import os

from partisan.irods import DataObject
from pytest import mark as m

from npg_irods.mlwh_locations.writer import LocationWriter, PACBIO


@m.describe("Ml Warehouse Locations file writer")
class TestWriteMlwhJson:
    @m.context("When no locations are provided")
    @m.it("Returns False and does not write")
    def test_write_mlwh_json_no_location(self, tmp_path):
        path = str(tmp_path / "empty.json")
        writer = LocationWriter(PACBIO, path=path)
        assert not writer.write()
        assert not os.path.exists(path)

    @m.context("When provided with a correct set of locations")
    @m.context(
        "When there are no objects where both root collection and id_product are the same"
    )
    @m.it("Returns True and writes the correct file")
    def test_write_mlwh_json_distinct(self, tmp_path):
        expected_path = "tests/data/pacbio/no_duplicates.json"
        actual_path = str(tmp_path / "no_duplicates.json")
        writer = LocationWriter(PACBIO, path=actual_path)
        writer.add_product(
            DataObject("/testZone/home/irods/pacbio/run/A01/test.bam"), "abcdef123456"
        )
        writer.add_product(
            DataObject("/testZone/home/irods/pacbio/run/A01/test2.bam"), "ghijkl78901"
        )
        writer.add_product(
            DataObject("/testZone/home/irods/pacbio/run/B01/test2.bam"), "mnopq234567"
        )
        assert writer.write()
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
        writer = LocationWriter(PACBIO, path=actual_path)
        writer.add_product(
            DataObject("/testZone/home/irods/pacbio/run/A01/test.bam"), "abcdef123456"
        )
        writer.add_product(
            DataObject("/testZone/home/irods/pacbio/run/B01/test2.bam"), "ghijkl78901"
        )
        writer.add_product(
            DataObject("/testZone/home/irods/pacbio/run/B01/extra.bam"), "ghijkl78901"
        )
        assert writer.write()
        with open(actual_path) as actual, open(expected_path) as expected:
            assert json.load(actual) == json.load(expected)
