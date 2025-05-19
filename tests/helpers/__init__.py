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

"""Helper functions for testing."""
import os
from datetime import datetime
from pathlib import PurePath

import pytest
from partisan.icommands import (
    add_specific_sql,
    have_admin,
    imkdir,
    iquest,
    irm,
    mkgroup,
    remove_specific_sql,
    rmgroup,
)
from partisan.irods import AC, AVU, Collection, DataObject, Permission

# The following iRODS groups represent groups not managed by this API e.g. belonging to
# administrators or other teams
UNMANAGED_GROUPS = ["unmanaged"]

# The following iRODS groups manage permissions for data belonging to the corresponding
# study.
STUDY_GROUPS = ["ss_1000", "ss_2000", "ss_3000", "ss_4000", "ss_5000", "ss_888"]

# The following iRODS groups manage permissions for human contamination identified in
# data belonging to the corresponding study.
HUMAN_STUDY_GROUPS = [g + "_human" for g in STUDY_GROUPS]

# The following iRODS groups are created by the iRODS test fixture.
TEST_GROUPS = UNMANAGED_GROUPS + STUDY_GROUPS + HUMAN_STUDY_GROUPS

# Example dates, useful for fixtures for tests over date ranges.
CREATED = datetime(year=2019, month=12, day=30, hour=0, minute=0, second=0)
BEGIN = datetime(year=2020, month=1, day=1, hour=0, minute=0, second=0)
EARLY = datetime(year=2020, month=6, day=1, hour=0, minute=0, second=0)
LATE = datetime(year=2020, month=6, day=14, hour=0, minute=0, second=0)
LATEST = datetime(year=2020, month=6, day=30, hour=0, minute=0, second=0)

# iRODS aliases for canned SQL statements.
TEST_SQL_STALE_REPLICATE = "setObjectReplStale"
TEST_SQL_INVALID_CHECKSUM = "setObjectChecksumInvalid"


def is_running_in_github_ci():
    """Return True if running in GitHub CI, False otherwise.

    https://docs.github.com/en/actions/writing-workflows/choosing-what-your-workflow-does/store-information-in-variables
    """
    return "GITHUB_ACTIONS" in os.environ


def add_sql_test_utilities():
    """Add to iRODS canned SQL statements required for testing."""
    if have_admin():
        add_specific_sql(
            TEST_SQL_STALE_REPLICATE,
            "UPDATE r_data_main dm SET DATA_IS_DIRTY = 0 FROM r_coll_main cm "
            "WHERE dm.coll_id = cm.coll_id "
            "AND cm.COLL_NAME = ? "
            "AND dm.DATA_NAME = ? "
            "AND dm.DATA_REPL_NUM= ?",
        )
        add_specific_sql(
            TEST_SQL_INVALID_CHECKSUM,
            "UPDATE r_data_main dm SET DATA_CHECKSUM = 0 FROM r_coll_main cm "
            "WHERE dm.coll_id = cm.coll_id "
            "AND cm.COLL_NAME = ? "
            "AND dm.DATA_NAME = ? "
            "AND dm.DATA_REPL_NUM= ?",
        )


def remove_sql_test_utilities():
    """Remove from iRODS canned SQL statements required for testing."""
    if have_admin():
        remove_specific_sql(TEST_SQL_STALE_REPLICATE)
        remove_specific_sql(TEST_SQL_INVALID_CHECKSUM)


def set_replicate_invalid(obj: DataObject, replicate_num: int):
    """Set the replicate number of a data object to be invalid."""
    iquest(
        "--sql",
        TEST_SQL_STALE_REPLICATE,
        obj.path.as_posix(),
        obj.name,
        str(replicate_num),
    )


def set_checksum_invalid(obj: DataObject, replicate_num: int):
    """Set the checksum of a data object to be invalid."""
    iquest(
        "--sql",
        TEST_SQL_INVALID_CHECKSUM,
        obj.path.as_posix(),
        obj.name,
        str(replicate_num),
    )


def add_rods_path(root_path: PurePath, tmp_path: PurePath) -> PurePath:
    """Add a path to iRODS, returning the path in iRODS."""
    parts = PurePath(*tmp_path.parts[1:])
    rods_path = root_path / parts
    imkdir(rods_path, make_parents=True)

    return rods_path


def remove_rods_path(rods_path: PurePath):
    """Remove a path from iRODS."""
    coll = Collection(rods_path)
    if coll.exists():
        coll.add_permissions(
            AC(user="irods", perm=Permission.OWN, zone="testZone"), recurse=True
        )
        irm(rods_path, force=True, recurse=True)


def add_test_groups():
    """Add test iRODS groups to iRODS."""
    if have_admin():
        for g in TEST_GROUPS:
            mkgroup(g)


def remove_test_groups():
    """Remove test iRODS groups from iRODS."""
    if have_admin():
        for g in TEST_GROUPS:
            rmgroup(g)


def history_in_meta(history: AVU, metadata: list[AVU]):
    """Return true if the history AVU is present in metadata, using a comparator
    which ignores the timestamp portion of the AVU value, False otherwise.

    Args:
        history: An AVU created by the AVU.history method.
        metadata: The metadata list of an entity.

    Returns: bool
    """
    if not history.is_history():
        raise ValueError(f"{history} is not a history AVU")

    def compare_without_timestamp(val1, val2):
        return val1.split("]")[1] == val2.split("]")[1]

    for avu in metadata:
        if (
            avu.is_history()
            and history.attribute == avu.attribute
            and compare_without_timestamp(history.value, avu.value)
            and history.units == avu.units
        ):
            return True

    return False


tests_have_admin = pytest.mark.skipif(
    not have_admin(), reason="tests do not have iRODS admin access"
)
