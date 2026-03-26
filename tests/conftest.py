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
import shutil
from datetime import datetime, timezone
from pathlib import Path, PurePath
from typing import Any, Generator

import pytest
import structlog
from npg.conf import IniData
from partisan.irods import AC, AVU, Collection, DataObject, Permission
from partisan.metadata import DublinCore
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy_utils import create_database, database_exists, drop_database

from helpers import (
    BEGIN,
    CREATED,
    PUBLIC_AC,
    UNMANAGED_AC,
    add_rods_path,
    add_sql_test_utilities,
    add_test_groups,
    consume,
    enable_inheritance,
    is_running_in_github_ci,
    remove_sql_test_utilities,
    remove_test_groups,
    set_replicate_invalid,
)
from npg_irods import db
from npg_irods.db import mlwh
from npg_irods.metadata.common import DataFile
from npg_irods.metadata.lims import Sample, Study, TrackedSample, TrackedStudy

logging.basicConfig(level=logging.ERROR, encoding="utf-8")

log_processors = [
    # If log level is too low, abort pipeline and throw away log entry.
    structlog.stdlib.filter_by_level,
    # Add the name of the logger to event dict.
    structlog.stdlib.add_logger_name,
    # Add log level to event dict.
    structlog.stdlib.add_log_level,
    # Perform %-style formatting.
    structlog.stdlib.PositionalArgumentsFormatter(),
    # Add a timestamp in ISO 8601 format.
    structlog.processors.TimeStamper(fmt="iso", utc=True),  # UTC added by kdj
    # If some value is in bytes, decode it to a Unicode str.
    structlog.processors.UnicodeDecoder(),
    # Add call site parameters.
    structlog.processors.CallsiteParameterAdder(
        {
            structlog.processors.CallsiteParameter.FILENAME,
            structlog.processors.CallsiteParameter.FUNC_NAME,
            structlog.processors.CallsiteParameter.LINENO,
            structlog.processors.CallsiteParameter.THREAD_NAME,
        }
    ),
    structlog.processors.JSONRenderer(),
]

structlog.configure(
    processors=log_processors,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)


TEST_INI = os.path.join(os.path.dirname(__file__), "testdb.ini")
INI_SECTION_LOCAL = "docker"
INI_SECTION_GITHUB = "github"


@pytest.fixture(scope="session", autouse=True)
def irods_groups():
    try:
        add_test_groups()
        yield
    finally:
        remove_test_groups()


@pytest.fixture(scope="session", autouse=True)
def sql_test_utilities():
    """Install SQL test utilities for the iRODS backend database."""
    try:
        add_sql_test_utilities()
        yield
    finally:
        remove_sql_test_utilities()


@pytest.fixture(scope="function")
def mlwh_session() -> Generator[Session, Any, None]:
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
    sess: Session = session_maker()

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


def initialize_mlwh_study_and_samples(session: Session):
    """Insert ML warehouse test data for synthetic runs."""

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

    sample_1 = Sample(
        id_lims="LIMS_01",
        common_name="common_name1",
        donor_id="donor_id1",
        id_sample_lims="id_sample_lims1",
        accession_number="Test Accession",
        name="name1",
        public_name="public_name1",
        supplier_name="supplier_name1",
        uuid_sample_lims="82429892-0ab6-11ee-b5ba-fa163eac3ag7",
        **default_timestamps,
    )
    session.add(sample_1)

    sample_2 = Sample(
        id_lims="LIMS_01",
        common_name="common_name2",
        donor_id="donor_id2",
        id_sample_lims="id_sample_lims2",
        accession_number="Test Accession",
        name="name2",
        public_name="public_name2",
        supplier_name="supplier_name2",
        uuid_sample_lims="82429892-0ab6-11ee-b5ba-fa163eac3ag8",
        **default_timestamps,
    )
    session.add(sample_2)

    session.commit()


@pytest.fixture(scope="function")
def study_and_samples_mlwh(mlwh_session) -> Generator[Session, Any, None]:
    """An ML warehouse database fixture populated with a single test study and some
    sample records."""
    initialize_mlwh_study_and_samples(mlwh_session)
    yield mlwh_session


@pytest.fixture(scope="function")
def tmp_irods_collection_path(tmp_path) -> Generator[PurePath, Any, None]:
    """A fixture providing a temporary iRODS collection."""
    root_path = PurePath("/testZone/home/irods/tmp")
    coll_path = add_rods_path(root_path, tmp_path)
    coll = Collection(coll_path).create(exist_ok=True, parents=True)

    try:
        yield coll_path
    finally:
        coll.remove(recurse=True)


@pytest.fixture(scope="function")
def empty_collection_path(tmp_irods_collection_path) -> PurePath:
    """A fixture providing an empty collection."""
    coll_path = tmp_irods_collection_path / "empty_collection"
    Collection(coll_path).create(exist_ok=True, parents=True)

    return coll_path


@pytest.fixture(scope="function")
def populated_collection_path(tmp_irods_collection_path) -> PurePath:
    """A fixture providing a collection path containing a single data object and a
    sub-collection, also containing a single data object."""
    coll_path = tmp_irods_collection_path / "populated_collection"
    consume(
        Collection(coll_path).put(
            "./tests/data/simple/collection", recurse=True, verify_checksum=True
        )
    )

    return coll_path


@pytest.fixture(scope="function")
def simple_data_object_path(
    tmp_irods_collection_path,
) -> Generator[PurePath, Any, None]:
    """A fixture providing a collection containing a single data object containing
    UTF-8 data."""
    obj_path = tmp_irods_collection_path / "lorem.txt"
    obj = DataObject(obj_path).put(
        "./tests/data/simple/data_object/lorem.txt", verify_checksum=True
    )

    try:
        yield obj_path
    finally:
        obj.remove(force=True)


@pytest.fixture(scope="function")
def annotated_data_object_path(
    tmp_irods_collection_path,
) -> Generator[PurePath, Any, None]:
    """A fixture providing a collection containing a single, annotated data object
    containing UTF-8 data."""
    obj_path = tmp_irods_collection_path / "lorem.txt"

    obj = DataObject(obj_path).put(
        "./tests/data/simple/data_object/lorem.txt", verify_checksum=True
    )
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
        obj.remove(force=True)


@pytest.fixture(scope="function")
def single_study_single_sample_data_object_path(
    tmp_irods_collection_path,
) -> Generator[PurePath, Any, None]:
    """A fixture providing a collection containing a single data object with sample
    and study metadata."""
    obj_path = tmp_irods_collection_path / "lorem.txt"
    obj = DataObject(obj_path).put(
        "./tests/data/simple/data_object/lorem.txt", verify_checksum=True
    )
    obj.add_metadata(
        AVU(TrackedSample.ID, "id_sample_lims1"), AVU(TrackedStudy.ID, "1000")
    )

    try:
        yield obj_path
    finally:
        obj.remove(force=True)


@pytest.fixture(scope="function")
def single_study_multi_sample_data_object_path(
    tmp_irods_collection_path,
) -> Generator[PurePath, Any, None]:
    """A fixture providing a collection containing a single data object with multiple
    sample and single study metadata."""
    obj_path = tmp_irods_collection_path / "lorem.txt"
    obj = DataObject(obj_path).put(
        "./tests/data/simple/data_object/lorem.txt", verify_checksum=True
    )
    obj.add_metadata(
        AVU(TrackedSample.ID, "id_sample_lims1"),
        AVU(TrackedSample.ID, "id_sample_lims2"),
        AVU(TrackedStudy.ID, "1000"),
    )

    try:
        yield obj_path
    finally:
        obj.remove(force=True)


@pytest.fixture(scope="function")
def consent_withdrawn_gapi_data_object_path(
    tmp_irods_collection_path,
) -> Generator[PurePath, Any, None]:
    obj_path = tmp_irods_collection_path / "lorem.txt"
    obj = DataObject(obj_path).put(
        "./tests/data/simple/data_object/lorem.txt", verify_checksum=True
    )
    obj.add_metadata(AVU(TrackedSample.CONSENT, "0"))

    try:
        yield obj_path
    finally:
        obj.remove(force=True)


@pytest.fixture(scope="function")
def consent_withdrawn_npg_data_object_path(
    tmp_irods_collection_path,
) -> Generator[PurePath, Any, None]:
    obj_path = tmp_irods_collection_path / "lorem.txt"
    obj = DataObject(obj_path).put(
        "./tests/data/simple/data_object/lorem.txt", verify_checksum=True
    )
    obj.add_metadata(AVU(TrackedSample.CONSENT_WITHDRAWN, "1"))

    try:
        yield obj_path
    finally:
        obj.remove(force=True)


@pytest.fixture(scope="function")
def invalid_replica_data_object_path(
    tmp_irods_collection_path,
) -> Generator[PurePath, Any, None]:
    """A fixture providing a data object with one of its two replicas marked invalid."""
    obj_path = tmp_irods_collection_path / "invalid_replica.txt"
    obj = DataObject(obj_path).put(
        "./tests/data/simple/data_object/lorem.txt", verify_checksum=True
    )
    set_replicate_invalid(obj, 1)

    try:
        yield obj_path
    finally:
        obj.remove(force=True)


@pytest.fixture(scope="function")
def annotated_collection_tree_path(
    tmp_irods_collection_path,
) -> Generator[PurePath, Any, None]:
    """A fixture providing a tree of collections and data objects, with both
    collections and data objects having annotation."""
    tree_root = tmp_irods_collection_path / "annotated_collection_tree"

    group_ac = AC("ss_1000", Permission.READ, zone="testZone")
    public_ac = AC("public", Permission.READ, zone="testZone")

    coll = Collection(tree_root)
    consume(coll.put("./tests/data/tree", recurse=True, verify_checksum=True))

    # Create some empty collections
    c = Collection(coll.path / "c").create()
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
        coll.remove(recurse=True)


@pytest.fixture(scope="function")
def challenging_irods_paths(
    tmp_irods_collection_path,
) -> Generator[PurePath, Any, None]:
    """A fixture providing a collection of challengingly named paths which contain spaces
    and/or quotes."""
    expt_root = tmp_irods_collection_path / "challenging"

    coll = Collection(expt_root)
    consume(coll.put("./tests/data/challenging", recurse=True, verify_checksum=True))

    try:
        yield expt_root
    finally:
        coll.remove(recurse=True)


@pytest.fixture(scope="function")
def ultima_run(tmp_path) -> Generator[tuple[Path, Path], Any, None]:
    """A fixture providing an Ultima runs directory and a checksums file."""
    run_dir = (tmp_path / "minimal").absolute()
    shutil.copytree("./tests/data/ultima/minimal", run_dir)
    checksums_path = tmp_path / "minimal.md5"
    checksums_path.write_text(
        f"""ac06fd24fc0edc84761c799c975d73c3  {run_dir}/000001-a/000002-c.txt
f8c316034eaf9cd99e7346afa5e4a8e3  {run_dir}/000001-d/000001-d.txt
6e785a5236e0b5480025469d312b2aeb  {run_dir}/000001_a.txt
2b1c6c7095b550e86230c4996d1595e4  {run_dir}/b.txt"""
    )
    yield run_dir, checksums_path


@pytest.fixture(scope="function")
def public_unmanaged_inheritance_enabled_collection(
    tmp_path,
) -> Generator[PurePath, Any, None]:
    """A fixture providing a collection with public and unmanaged read permissions and
    inheritance enabled.

    Simplified version of how top level runs folder are configured for many
    instruments, e.g. `/seq/illumina/runs`.
    """
    yield from _collection_with_permissions_inheritance(
        tmp_path, [PUBLIC_AC, UNMANAGED_AC], True
    )


@pytest.fixture(scope="function")
def public_unmanaged_inheritance_disabled_collection_path(
    tmp_path,
) -> Generator[PurePath, Any, None]:
    """A fixture providing a collection with public and unmanaged read permissions and
    inheritance disabled."""
    yield from _collection_with_permissions_inheritance(
        tmp_path, [PUBLIC_AC, UNMANAGED_AC], False
    )


def _collection_with_permissions_inheritance(tmp_path, permissions: list[AC], inherit):
    root_path = PurePath(
        f"/testZone/home/irods/test/{"_".join(ac.user for ac in permissions)}_inherit_{inherit}_collection"
    )
    coll_path = add_rods_path(root_path, tmp_path)

    coll = Collection(coll_path)
    coll.add_permissions(*permissions)
    if inherit:
        enable_inheritance(coll_path)

    try:
        yield coll_path
    finally:
        coll.remove(recurse=True)
