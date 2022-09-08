# -*- coding: utf-8 -*-
#
# Copyright © 2021, 2022 Genome Research Ltd. All rights reserved.
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

    EXPERIMENT_NAME = "experiment_name"
    INSTRUMENT_SLOT = "instrument_slot"
