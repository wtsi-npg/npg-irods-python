# -*- coding: utf-8 -*-
#
# Copyright © 2021, 2022, 2023 Genome Research Ltd. All rights reserved.
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

from enum import unique

from partisan.metadata import AsValueEnum, with_namespace


@unique
class Instrument(AsValueEnum, metaclass=with_namespace("ont")):
    """Oxford Nanopore platform metadata"""

    DEVICE_ID = "device_id"
    DEVICE_TYPE = "device_type"
    DISTRIBUTION_VERSION = "distribution_version"
    EXPERIMENT_NAME = "experiment_name"
    FLOWCELL_ID = "flowcell_id"
    GUPPY_VERSION = "guppy_version"
    HOSTNAME = "hostname"
    INSTRUMENT_SLOT = "instrument_slot"
    PROTOCOL_GROUP_ID = "protocol_group_id"
    RUN_ID = "run_id"
    SAMPLE_ID = "sample_id"
    TAG_IDENTIFIER = "tag_identifier"

    def __repr__(self):
        return f"{Instrument.namespace}:{self.value}"
