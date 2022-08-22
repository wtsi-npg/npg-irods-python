# -*- coding: utf-8 -*-
#
# Copyright © 2020, 2022 Genome Research Ltd. All rights reserved.
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

from datetime import timedelta, datetime

from pytest import mark as m

from conftest import BEGIN, EARLY, LATE, LATEST
from npg_irods.ont import find_recent_expt, find_recent_expt_slot
from npg_irods.metadata.illumina import illumina_recently_changed


@m.describe("Finding updated ONT experiments by datetime")
class TestONTMLWarehouseQueries(object):
    @m.context("When a query date is provided")
    @m.it("Finds the correct experiments")
    def test_find_recent_expt(self, mlwh_session):
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
        assert find_recent_expt(mlwh_session, EARLY) == all_expts

        # Odd-numbered experiments were done late or latest
        before_late = LATE - timedelta(days=1)
        odd_expts = [
            "simple_experiment_001",
            "simple_experiment_003",
            "simple_experiment_005",
            "multiplexed_experiment_001",
            "multiplexed_experiment_003",
        ]
        assert find_recent_expt(mlwh_session, before_late) == odd_expts

        after_latest = LATEST + timedelta(days=1)
        none = find_recent_expt(mlwh_session, after_latest)
        assert none == []

    @m.describe("Finding updated experiments and positions by datetime")
    @m.context("When a query date is provided")
    @m.it("Finds the correct experiment, slot tuples")
    def test_find_recent_expt_pos(self, mlwh_session):
        before_late = LATE - timedelta(days=1)
        odd_expts = [
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
        assert find_recent_expt_slot(mlwh_session, before_late) == odd_expts

        before_latest = LATEST - timedelta(days=1)
        odd_positions = [
            ("multiplexed_experiment_001", 1),
            ("multiplexed_experiment_001", 3),
            ("multiplexed_experiment_001", 5),
            ("multiplexed_experiment_003", 1),
            ("multiplexed_experiment_003", 3),
            ("multiplexed_experiment_003", 5),
        ]
        assert find_recent_expt_slot(mlwh_session, before_latest) == odd_positions

        after_latest = LATEST + timedelta(days=1)
        assert find_recent_expt_slot(mlwh_session, after_latest) == []


@m.describe("Finding illumina recently changed information in illumina tables")
class TestIlluminaMLWarehouseQueries(object):
    @m.context("When given a datetime")
    @m.it("Finds rows updated since that datetime")
    def test_illumina_recently_changed(self, mlwh_session):

        late_expected = [
            (
                "ST0000000001",
                "Recently Changed",
                "Recently changed study",
                "study_04",
                "SA000002",
                "SAMPLE_02",
                "Unchanged",
                "Unchanged",
                "Unchanged",
                "Unchanged_supplier",
                "cohort_02",
                "DONOR_02",
                0,
                "LIBRARY_01",
                0,
                "Primer_panel_01",
            ),
            (
                "ST0000000002",
                "Unchanged",
                "Unchanged study",
                "study_05",
                "SA000001",
                "SAMPLE_01",
                "Recently changed",
                "Recently changed",
                "Recently changed",
                "Recently_changed_supplier",
                "cohort_01",
                "DONOR_01",
                0,
                "LIBRARY_02",
                0,
                "Primer_panel_02",
            ),
            (
                "ST0000000002",
                "Unchanged",
                "Unchanged study",
                "study_05",
                "SA000002",
                "SAMPLE_02",
                "Unchanged",
                "Unchanged",
                "Unchanged",
                "Unchanged_supplier",
                "cohort_02",
                "DONOR_02",
                0,
                "LIBRARY_04",
                0,
                "Primer_panel_04",
            ),
            (
                "ST0000000002",
                "Unchanged",
                "Unchanged study",
                "study_05",
                "SA000002",
                "SAMPLE_02",
                "Unchanged",
                "Unchanged",
                "Unchanged",
                "Unchanged_supplier",
                "cohort_02",
                "DONOR_02",
                0,
                "LIBRARY_03",
                0,
                "Primer_panel_03",
            ),
        ]
        before_early_expected = late_expected + [
            (
                "ST0000000002",
                "Unchanged",
                "Unchanged study",
                "study_05",
                "SA000002",
                "SAMPLE_02",
                "Unchanged",
                "Unchanged",
                "Unchanged",
                "Unchanged_supplier",
                "cohort_02",
                "DONOR_02",
                0,
                "LIBRARY_05",
                0,
                "Primer_panel_05",
            )
        ]
        before_early = BEGIN - timedelta(days=1)
        after_latest = LATEST + timedelta(days=1)

        # only recently updated
        assert illumina_recently_changed(mlwh_session, LATE) == late_expected
        # all
        assert (
            illumina_recently_changed(mlwh_session, before_early)
            == before_early_expected
        )
        # none
        assert illumina_recently_changed(mlwh_session, after_latest) == []
