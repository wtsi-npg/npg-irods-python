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
#
from datetime import timedelta

from pytest import mark as m

from helpers import BEGIN, EARLY, LATE, LATEST
from npg_irods.pacbio import Component, find_updated_components
from pacbio.conftest import pacbio_synthetic_mlwh


@m.describe("Finding updated PacBio runs by datetime")
class TestPacBioMLWarehouseQueries:
    @m.context("When a query date is provided")
    @m.it("Finds the correct run, well, tag sequence tuples")
    def test_find_updated_components(self, pacbio_synthetic_mlwh):
        # All run records fall between BEGIN and LATEST
        all_runs = [
            Component(r, w, tag_sequence=t)
            for (r, w, t) in [
                ("run1", "A01", "AAAAAAAAAA"),
                ("run1", "A01", "GGGGGGGGGG"),
                ("run1", "A02", "CCCCCCCCCC"),
                ("run1", "A02", "TTTTTTTTTT"),
                #
                ("run2", "A01", "AAACCCCCCC"),
                ("run2", "A01", "AAAGGGGGGG"),
                ("run2", "A02", "AAATTTTTTT"),
                ("run2", "A02", "GGGAAAAAAA"),
            ]
        ]
        assert [
            c for c in find_updated_components(pacbio_synthetic_mlwh, BEGIN, LATEST)
        ] == all_runs

        # Even run records are done EARLY and before LATE
        before_late = LATE - timedelta(days=1)
        even_runs = [
            Component(r, w, tag_sequence=t)
            for (r, w, t) in [
                ("run2", "A01", "AAACCCCCCC"),
                ("run2", "A01", "AAAGGGGGGG"),
                ("run2", "A02", "AAATTTTTTT"),
                ("run2", "A02", "GGGAAAAAAA"),
            ]
        ]
        assert [
            c
            for c in find_updated_components(pacbio_synthetic_mlwh, EARLY, before_late)
        ] == even_runs

        # Odd run records are done LATE and before LATEST
        odd_runs = [
            Component(r, w, tag_sequence=t)
            for (r, w, t) in [
                ("run1", "A01", "AAAAAAAAAA"),
                ("run1", "A01", "GGGGGGGGGG"),
                ("run1", "A02", "CCCCCCCCCC"),
                ("run1", "A02", "TTTTTTTTTT"),
            ]
        ]
        assert [
            c
            for c in find_updated_components(pacbio_synthetic_mlwh, before_late, LATEST)
        ] == odd_runs

        # No run records are done after LATE
        after_late = LATE + timedelta(days=1)
        assert [
            c
            for c in find_updated_components(pacbio_synthetic_mlwh, after_late, LATEST)
        ] == []
