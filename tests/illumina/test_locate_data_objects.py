# -*- coding: utf-8 -*-
#
# Copyright © 2023, 2026 Genome Research Ltd. All rights reserved.
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
                "qc/12345#1.bam_flagstats.json",
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

    @m.context("When MLWH updates are timestamp-only for samples and studies")
    @m.it("Should return fewer updates after priming the MLWH cache")
    def test_illumina_updates_cache_reduces_updates(
        self, capsys, tmp_path, illumina_synthetic_irods, illumina_synthetic_mlwh
    ):
        np_uncached, ne_uncached = illumina_updates(
            illumina_synthetic_mlwh, BEGIN, LATEST
        )

        assert np_uncached == 7, "Number of MLWH records processed without cache"
        assert ne_uncached == 0, "Number of errors without cache"
        capsys.readouterr()  # Clear output from the uncached run

        cache_path = tmp_path / "mlwh_cache.sqlite"
        np_cached, ne_cached = illumina_updates(
            illumina_synthetic_mlwh,
            BEGIN,
            LATEST,
            cache_path=cache_path,
            prime_cache=True,
        )
        stdout_lines = [line for line in capsys.readouterr().out.split("\n") if line]

        assert np_cached == 0, "Number of MLWH records processed with cache"
        assert ne_cached == 0, "Number of errors with cache"
        assert stdout_lines == []
