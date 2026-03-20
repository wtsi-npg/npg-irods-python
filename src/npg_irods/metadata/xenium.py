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

from enum import unique

from partisan.metadata import AsValueEnum, with_namespace

from npg_irods.common import PlatformNamespace

EXPERIMENT_FILENAME = "experiment.xenium"


@unique
class Instrument(AsValueEnum, metaclass=with_namespace(PlatformNamespace.XENIUM)):
    """Xenium platform metadata"""

    ANALYSIS_SW_VERSION = "analysis_sw_version"
    ANALYSIS_UUID = "analysis_uuid"
    CASSETTE_NAME = "cassette_name"
    CASSETTE_UUID = "cassette_uuid"
    EXPERIMENT_UUID = "experiment_uuid"
    INSTRUMENT_SN = "instrument_sn"
    INSTRUMENT_SW_VERSION = "instrument_sw_version"
    PANEL_DESIGN_ID = "panel_design_id"
    PANEL_NAME = "panel_name"
    PANEL_ORGANISM = "panel_organism"
    PANEL_TISSUE_TYPE = "panel_tissue_type"
    PANEL_TYPE = "panel_type"
    REGION_NAME = "region_name"
    ROI_UUID = "roi_uuid"
    RUN_NAME = "run_name"
    RUN_START_TIME = "run_start_time"
    SLIDE_ID = "slide_id"
    WELL_UUID = "well_uuid"

    def __repr__(self):
        return f"{Instrument.namespace}:{self.value}"
