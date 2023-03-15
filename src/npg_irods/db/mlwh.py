# -*- coding: utf-8 -*-
#
# Copyright © 2023 Genome Research Ltd. All rights reserved.
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

from abc import ABC
from dataclasses import dataclass, fields
from typing import List

from sqlalchemy import select

from npg_irods.db import DBHandle


@dataclass
class MLWHEntity(ABC):
    """A base class for all ML warehouse entities."""

    def field_names(self) -> List[str]:
        """Return a list of the dataclass field names."""
        return [f.name for f in fields(self)]


@dataclass
class Sample(MLWHEntity):
    """A minimal representation of a ML warehouse sample."""

    consent_withdrawn: bool = False
    id_sample_lims: str = ""
    name: str = ""
    sanger_sample_id: str = ""
    supplier_name: str = ""


def find_consent_withdrawn_samples(db: DBHandle) -> List[Sample]:
    """Return a list of all samples with consent withdrawn.

    Args:
        db: A database handle.

    Returns:
        All samples marked as having their consent withdrawn.
    """
    with db.engine.connect() as conn:
        table = db.table("sample")
        columns = [table.c[name] for name in Sample().field_names()]
        query = select(*columns).filter(table.c.consent_withdrawn == 1)

        return [Sample(*row) for row in conn.execute(query).all()]
