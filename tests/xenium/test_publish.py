# -*- coding: utf-8 -*-
#
# Copyright © 2026 Genome Research Ltd. All rights reserved.
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

from pathlib import Path

from partisan.irods import AVU, Collection
from pytest import mark as m

from npg_irods.common import PlatformNamespace
from npg_irods.xenium import publish_result_dir


class TestPublish:
    @m.context("When a local Xenium results directory is provided")
    @m.it("Publishes it to iRODS with correct collection metadata")
    def test_publish_results_directory(self, empty_collection):
        local_path = Path(
            "tests/data/xenium/synthetic/"
            "output-XETG00000__0000000__synthetic_region_001__20000101__000000"
        )
        remote_path = empty_collection / "xenium"

        Collection(remote_path).create()  # The root collection must exist
        coll = publish_result_dir(local_path, remote_path)

        instrument = "XETG00000"
        run_name = "synthetic_run_001"

        assert coll.exists()
        assert coll.path == remote_path / instrument / run_name / local_path.name

        expected_metadata = [
            avu.with_namespace(PlatformNamespace.XENIUM)
            for avu in [
                AVU("analysis_sw_version", "xenium-synthetic-0.0.0"),
                AVU("analysis_uuid", "00000000-0000-0000-0000-000000000001"),
                AVU("cassette_name", "SYNTHETIC_0001"),
                AVU("cassette_uuid", "00000000-0000-0000-0000-000000000003"),
                AVU("experiment_uuid", "00000000-0000-0000-0000-000000000004"),
                AVU("instrument_sn", "XETG00000"),
                AVU("instrument_sw_version", "0.0.0.0"),
                AVU("panel_design_id", "synthetic_design"),
                AVU("panel_name", "Synthetic Panel"),
                AVU("panel_organism", "Syntheticus"),
                AVU("panel_tissue_type", "Synthetic"),
                AVU("panel_type", "synthetic"),
                AVU("region_name", "synthetic_region_001"),
                AVU("roi_uuid", "00000000-0000-0000-0000-000000000005"),
                AVU("run_name", "synthetic_run_001"),
                AVU("run_start_time", "2000-01-01T00:00:00Z"),
                AVU("slide_id", "0000000"),
                AVU("well_uuid", "synthetic_well_001"),
            ]
        ]

        assert coll.metadata() == expected_metadata
