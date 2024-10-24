# -*- coding: utf-8 -*-
#
# Copyright © 2020, 2022, 2023, 2024 Genome Research Ltd. All rights reserved.
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

# From the pytest docs:
#
# "The conftest.py file serves as a means of providing fixtures for an entire
# directory. Fixtures defined in a conftest.py can be used by any test in that
# package without needing to import them (pytest will automatically discover
# them)."

import logging
import os
from datetime import datetime, timezone
from pathlib import PurePath

import pytest
import structlog
from npg.conf import IniData
from partisan.icommands import (
    iput,
    irm,
)
from partisan.irods import AC, AVU, Collection, DataObject, Permission
from partisan.metadata import DublinCore
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy_utils import create_database, database_exists, drop_database

from helpers import (
    BEGIN,
    CREATED,
    add_rods_path,
    add_sql_test_utilities,
    add_test_groups,
    is_running_in_github_ci,
    remove_rods_path,
    remove_sql_test_utilities,
    remove_test_groups,
    set_replicate_invalid,
)
from npg_irods import db
from npg_irods.db import mlwh
from npg_irods.metadata.common import DataFile
from npg_irods.metadata.lims import Sample, Study, TrackedSample, TrackedStudy

logging.basicConfig(level=logging.ERROR)

structlog.configure(
    logger_factory=structlog.stdlib.LoggerFactory(),
    processors=[structlog.processors.JSONRenderer()],
)

TEST_INI = os.path.join(os.path.dirname(__file__), "testdb.ini")
INI_SECTION_LOCAL = "docker"
INI_SECTION_GITHUB = "github"


@pytest.fixture(scope="function")
def irods_groups():
    try:
        add_test_groups()
        yield
    finally:
        remove_test_groups()


@pytest.fixture(scope="session")
def sql_test_utilities():
    """Install SQL test utilities for the iRODS backend database."""
    try:
        add_sql_test_utilities()
        yield
    finally:
        remove_sql_test_utilities()


@pytest.fixture(scope="function")
def mlwh_session() -> Session:
    """Create an empty ML warehouse database fixture."""
    section = INI_SECTION_GITHUB if is_running_in_github_ci() else INI_SECTION_LOCAL

    dbconfig = IniData(db.Config).from_file(TEST_INI, section)
    engine = create_engine(dbconfig.url, echo=False)

    if database_exists(engine.url):
        drop_database(engine.url)

    create_database(engine.url)

    with engine.connect() as conn:
        # Workaround for invalid default values for dates.
        conn.execute(text("SET sql_mode = '';"))
        conn.commit()

    mlwh.Base.metadata.create_all(engine)
    session_maker = sessionmaker(bind=engine)
    sess: Session() = session_maker()

    try:
        yield sess
    finally:
        sess.close()

    # This is for the benefit of MySQL where we have a schema reused for
    # a number of tests. Without using sqlalchemy-utils, one would call:
    #
    #   for t in reversed(meta.sorted_tables):
    #       t.drop(engine)
    #
    # Dropping the database for SQLite deletes the SQLite file.
    drop_database(engine.url)


def initialize_mlwh_study_and_sample(session: Session):
    """
    Insert ML warehouse test data for synthetic runs.

    """
    default_timestamps = {
        "created": CREATED,
        "last_updated": BEGIN,
        "recorded_at": BEGIN,
    }

    study_x = Study(
        id_lims="LIMS_01",
        id_study_lims="1000",
        name="Study X",
        study_title="Test Study Title",
        accession_number="Test Accession",
        **default_timestamps,
    )
    session.add(study_x)

    sample_y = Sample(
        id_lims="LIMS_01",
        common_name="common_name1",
        donor_id="donor_id1",
        id_sample_lims="id_sample_lims1",
        accession_number="Test Accession",
        name="name1",
        public_name="public_name1",
        supplier_name="supplier_name1",
        **default_timestamps,
    )
    session.add(sample_y)

    session.commit()


@pytest.fixture(scope="function")
def simple_study_and_sample_mlwh(mlwh_session) -> Session:
    """An ML warehouse database fixture populated with test study records."""
    initialize_mlwh_study_and_sample(mlwh_session)
    yield mlwh_session


@pytest.fixture(scope="function")
def simple_collection(tmp_path):
    """A fixture providing an empty collection."""
    root_path = PurePath("/testZone/home/irods/test/simple_collection")
    coll_path = add_rods_path(root_path, tmp_path)

    try:
        yield coll_path
    finally:
        irm(coll_path, force=True, recurse=True)


@pytest.fixture(scope="function")
def simple_data_object(tmp_path):
    """A fixture providing a collection containing a single data object containing
    UTF-8 data."""
    root_path = PurePath("/testZone/home/irods/test/simple_data_object")
    rods_path = add_rods_path(root_path, tmp_path)

    obj_path = rods_path / "lorem.txt"
    iput("./tests/data/simple/data_object/lorem.txt", obj_path)

    try:
        yield obj_path
    finally:
        remove_rods_path(rods_path)


@pytest.fixture(scope="function")
def annotated_data_object(tmp_path):
    """A fixture providing a collection containing a single, annotated data object
    containing UTF-8 data."""

    root_path = PurePath("/testZone/home/irods/test/annotated_data_object")
    rods_path = add_rods_path(root_path, tmp_path)

    obj_path = rods_path / "lorem.txt"
    iput("./tests/data/simple/data_object/lorem.txt", obj_path)

    obj = DataObject(obj_path)
    obj.add_metadata(
        AVU(
            DublinCore.CREATED, datetime.now(timezone.utc).isoformat(timespec="seconds")
        ),
        AVU(DublinCore.CREATOR, "dummy creator"),
        AVU(DublinCore.PUBLISHER, "dummy publisher"),
        AVU(DataFile.TYPE, "txt"),
        AVU(DataFile.MD5, "39a4aa291ca849d601e4e5b8ed627a04"),
    )

    try:
        yield obj_path
    finally:
        remove_rods_path(rods_path)


@pytest.fixture(scope="function")
def simple_study_and_sample_data_object(tmp_path, irods_groups):
    """A fixture providing a collection containing a single data object with sample
    and study metadata"""
    root_path = PurePath(
        "/testZone/home/irods/test/simple_study_and_sample_data_object"
    )
    rods_path = add_rods_path(root_path, tmp_path)

    obj_path = rods_path / "lorem.txt"
    iput("./tests/data/simple/data_object/lorem.txt", obj_path)

    obj = DataObject(obj_path)
    obj.add_metadata(
        AVU(TrackedSample.ID, "id_sample_lims1"), AVU(TrackedStudy.ID, "1000")
    )

    try:
        yield obj_path
    finally:
        remove_rods_path(rods_path)


@pytest.fixture(scope="function")
def consent_withdrawn_gapi_data_object(tmp_path):
    root_path = PurePath("/testZone/home/irods/test/consent_withdrawn_gapi_data_object")
    rods_path = add_rods_path(root_path, tmp_path)

    obj_path = rods_path / "lorem.txt"
    iput("./tests/data/simple/data_object/lorem.txt", obj_path)

    obj = DataObject(obj_path)
    obj.add_metadata(AVU(TrackedSample.CONSENT, "0"))

    try:
        yield obj_path
    finally:
        remove_rods_path(rods_path)


@pytest.fixture(scope="function")
def consent_withdrawn_npg_data_object(tmp_path):
    root_path = PurePath("/testZone/home/irods/test/consent_withdrawn_npg_data_object")
    rods_path = add_rods_path(root_path, tmp_path)

    obj_path = rods_path / "lorem.txt"
    iput("./tests/data/simple/data_object/lorem.txt", obj_path)

    obj = DataObject(obj_path)
    obj.add_metadata(AVU(TrackedSample.CONSENT_WITHDRAWN, "1"))

    try:
        yield obj_path
    finally:
        remove_rods_path(rods_path)


@pytest.fixture(scope="function")
def invalid_replica_data_object(tmp_path, sql_test_utilities):
    """A fixture providing a data object with one of its two replicas marked invalid."""
    root_path = PurePath("/testZone/home/irods/test/invalid_replica_data_object")
    rods_path = add_rods_path(root_path, tmp_path)

    obj_path = rods_path / "invalid_replica.txt"
    iput("./tests/data/simple/data_object/lorem.txt", obj_path)
    set_replicate_invalid(DataObject(obj_path), 1)

    try:
        yield obj_path
    finally:
        remove_rods_path(rods_path)


@pytest.fixture(scope="function")
def annotated_collection_tree(tmp_path, irods_groups):
    """A fixture providing a tree of collections and data objects, with both
    collections and data objects having annotation."""

    root_path = PurePath("/testZone/home/irods/test/annotated_collection_tree")
    rods_path = add_rods_path(root_path, tmp_path)

    iput("./tests/data/tree", rods_path, recurse=True)
    tree_root = rods_path / "tree"

    group_ac = AC("ss_1000", Permission.READ, zone="testZone")
    public_ac = AC("public", Permission.READ, zone="testZone")

    coll = Collection(tree_root)

    # Create some empty collections
    c = Collection(coll.path / "c")
    c.create()
    for x in ["s", "t", "u"]:
        Collection(c.path / x).create()

    coll.add_metadata(AVU("path", str(coll)))
    coll.add_permissions(group_ac, public_ac)

    for item in coll.contents(recurse=True):
        item.add_metadata(AVU("path", str(item)))
        item.add_permissions(group_ac, public_ac)

    try:
        yield tree_root
    finally:
        remove_rods_path(rods_path)


@pytest.fixture(scope="function")
def challenging_paths_irods(tmp_path):
    """A fixture providing a collection of challengingly named paths which contain spaces
    and/or quotes."""
    root_path = PurePath("/testZone/home/irods/test/challenging_paths_irods")
    rods_path = add_rods_path(root_path, tmp_path)

    iput("./tests/data/challenging", rods_path, recurse=True)
    expt_root = rods_path / "challenging"

    try:
        yield expt_root
    finally:
        remove_rods_path(rods_path)
