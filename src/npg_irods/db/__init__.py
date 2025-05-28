# -*- coding: utf-8 -*-
#
# Copyright © 2023, 2024 Genome Research Ltd. All rights reserved.
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

"""Business logic API, schema-specific API and utilities for SQL databases."""

from dataclasses import dataclass, field
from urllib import parse


@dataclass
class Config:
    """A database configuration which falls back on environment variables if values are
    not set in the constructor.

    The database URL attribute is currently constructed in MySQL-specific format.
    """

    host: str
    port: str
    schema: str
    user: str
    password: str = field(repr=False, default=None)

    @property
    def url(self):
        return (
            f"mysql+pymysql://{self.user}:{parse.quote_plus(self.password)}"
            f"@{self.host}:{self.port}/{self.schema}?charset=utf8mb4"
        )
