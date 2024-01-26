# -*- coding: utf-8 -*-
#
# Copyright © 2020, 2022, 2023 Genome Research Ltd. All rights reserved.
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


from datetime import timedelta

from _pytest.mark import MARK_GEN as m

from helpers import EARLY, LATE, LATEST
from npg_irods.ont import Component, find_recent_expt, find_updated_components
from ont.conftest import ont_tag_identifier


@m.describe("Finding updated ONT experiments by datetime")
class TestONTMLWarehouseQueries(object):
    @m.context("When a query date is provided")
    @m.it("Finds the correct experiments")
    def test_find_recent_expt(self, ont_synthetic_mlwh):
        all_expts = [
            "simple_experiment_001",
            "simple_experiment_002",
            "simple_experiment_003",
            "simple_experiment_004",
            "simple_experiment_005",
            "multiplexed_experiment_001",
            "multiplexed_experiment_002",
            "multiplexed_experiment_003",
        ]
        assert find_recent_expt(ont_synthetic_mlwh, EARLY) == all_expts

        before_late = LATE - timedelta(days=1)
        odd_expts = [
            "simple_experiment_001",
            "simple_experiment_003",
            "simple_experiment_005",
            "multiplexed_experiment_001",
            "multiplexed_experiment_003",
        ]
        assert find_recent_expt(ont_synthetic_mlwh, before_late) == odd_expts

        after_latest = LATEST + timedelta(days=1)
        none = find_recent_expt(ont_synthetic_mlwh, after_latest)
        assert none == []

    @m.describe("Finding updated experiments and positions by datetime")
    @m.context("When a query dates are provided")
    @m.it("Finds the correct experiment, slot tuples")
    def test_find_updated_components(self, ont_synthetic_mlwh):
        before_late = LATE - timedelta(days=1)
        even_expts = [
            Component(*args)
            for args in [
                ("multiplexed_experiment_002", 1),
                ("multiplexed_experiment_002", 2),
                ("multiplexed_experiment_002", 3),
                ("multiplexed_experiment_002", 4),
                ("multiplexed_experiment_002", 5),
                ("simple_experiment_002", 1),
                ("simple_experiment_002", 2),
                ("simple_experiment_002", 3),
                ("simple_experiment_002", 4),
                ("simple_experiment_002", 5),
                ("simple_experiment_004", 1),
                ("simple_experiment_004", 2),
                ("simple_experiment_004", 3),
                ("simple_experiment_004", 4),
                ("simple_experiment_004", 5),
            ]
        ]
        assert [
            c
            for c in find_updated_components(
                ont_synthetic_mlwh, EARLY, before_late, include_tags=False
            )
        ] == even_expts

        odd_expts = [
            Component(*args)
            for args in [
                ("multiplexed_experiment_001", 1),
                ("multiplexed_experiment_001", 2),
                ("multiplexed_experiment_001", 3),
                ("multiplexed_experiment_001", 4),
                ("multiplexed_experiment_001", 5),
                ("multiplexed_experiment_003", 1),
                ("multiplexed_experiment_003", 2),
                ("multiplexed_experiment_003", 3),
                ("multiplexed_experiment_003", 4),
                ("multiplexed_experiment_003", 5),
                ("simple_experiment_001", 1),
                ("simple_experiment_001", 2),
                ("simple_experiment_001", 3),
                ("simple_experiment_001", 4),
                ("simple_experiment_001", 5),
                ("simple_experiment_003", 1),
                ("simple_experiment_003", 2),
                ("simple_experiment_003", 3),
                ("simple_experiment_003", 4),
                ("simple_experiment_003", 5),
                ("simple_experiment_005", 1),
                ("simple_experiment_005", 2),
                ("simple_experiment_005", 3),
                ("simple_experiment_005", 4),
                ("simple_experiment_005", 5),
            ]
        ]
        assert [
            c
            for c in find_updated_components(
                ont_synthetic_mlwh, before_late, LATEST, include_tags=False
            )
        ] == odd_expts

        before_latest = LATEST - timedelta(days=1)
        odd_positions = [
            Component(*args)
            for args in [
                ("multiplexed_experiment_001", 1),
                ("multiplexed_experiment_001", 3),
                ("multiplexed_experiment_001", 5),
                ("multiplexed_experiment_003", 1),
                ("multiplexed_experiment_003", 3),
                ("multiplexed_experiment_003", 5),
            ]
        ]
        assert [
            c
            for c in find_updated_components(
                ont_synthetic_mlwh, before_latest, LATEST, include_tags=False
            )
        ] == odd_positions

        after_latest1 = LATEST + timedelta(days=1)
        after_latest100 = LATEST + timedelta(days=100)
        assert [
            c
            for c in find_updated_components(
                ont_synthetic_mlwh, after_latest1, after_latest100, include_tags=False
            )
        ] == []

    @m.describe(
        "Finding updated experiments, positions and tag identifiers by datetime"
    )
    @m.context("When query dates are provided")
    @m.it("Finds the correct experiment, slot, tag identifier tuples")
    def test_find_updated_components_tag(self, ont_synthetic_mlwh):
        before_latest = LATEST - timedelta(days=1)
        odd_positions = []

        # Odd slot multiplexed experiments were done at LATEST time
        for expt_name in ["multiplexed_experiment_001", "multiplexed_experiment_003"]:
            for slot in [1, 3, 5]:
                # Tag identifiers NB01 - NB12
                for tag_id in [ont_tag_identifier(i + 1) for i in range(12)]:
                    odd_positions.append(Component(expt_name, slot, tag_id))

        assert [
            c
            for c in find_updated_components(
                ont_synthetic_mlwh, before_latest, LATEST, include_tags=True
            )
        ] == odd_positions

        after_latest1 = LATEST + timedelta(days=1)
        after_latest100 = LATEST + timedelta(days=100)
        assert [
            c
            for c in find_updated_components(
                ont_synthetic_mlwh, after_latest1, after_latest100, include_tags=True
            )
        ] == []
