from pytest import mark as m

from conftest import BEGIN, LATEST
from npg_irods.cli.locate_data_objects import illumina_updates


@m.describe("Locating data objects in iRODS")
class TestLocateDataObjects:
    @m.context("When the MLWH contains records of updated Illumina metadata")
    @m.context("When iRODS contains the corresponding data objects")
    @m.it("Should print the paths of the data objects to be updated")
    def test_illumina_updates(
        self, capsys, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        path = illumina_synthetic_irods / "12345"
        expected = [
            (path / p).as_posix()
            for p in [
                "12345#0.cram",
                "12345#1.cram",
                "12345#1_human.cram",
                "12345#1_phix.cram",
                "12345#1_xahuman.cram",
                "12345#2.cram",
                "12345#888.cram",
                "12345.cram",
                "qc/12345#1.genotype.json",
                "qc/12345#1_human.genotype.json",
                "qc/12345#1_xahuman.genotype.json",
                "qc/12345#1_yhuman.genotype.json",
                "qc/12345#2.genotype.json",
                "qc/12345.genotype.json",
            ]
        ]

        np, ne = illumina_updates(illumina_synthetic_mlwh, BEGIN, LATEST)
        stdout_lines = [line for line in capsys.readouterr().out.split("\n") if line]

        assert np == 7, "Number of MLWH records processed"
        assert ne == 0, "Number of errors"
        assert stdout_lines == expected
