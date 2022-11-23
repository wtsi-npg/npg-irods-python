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

# From the pytest docs:
#
# "The conftest.py file serves as a means of providing fixtures for an entire
# directory. Fixtures defined in a conftest.py can be used by any test in that
# package without needing to import them (pytest will automatically discover
# them)."

import configparser
import logging
import os
from datetime import datetime
from pathlib import PurePath

import pytest

import structlog
from ml_warehouse.schema import (
    Base,
    IseqFlowcell,
    IseqProductMetrics,
    OseqFlowcell,
    Sample,
    Study,
)

from partisan import icommands
from partisan.icommands import imkdir, iput, irm, mkgroup, rmgroup
from partisan.irods import (
    AC,
    AVU,
    Collection,
    DataObject,
    Permission,
)
from partisan.metadata import DublinCore
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy_utils import create_database, database_exists, drop_database

from npg_irods.metadata.common import DataFile
from npg_irods.metadata.ont import Instrument

test_ini = os.path.join(os.path.dirname(__file__), "testdb.ini")

logging.basicConfig(level=logging.ERROR)

structlog.configure(
    logger_factory=structlog.stdlib.LoggerFactory(),
    processors=[structlog.processors.JSONRenderer()],
)


@pytest.fixture(scope="session")
def config() -> configparser.ConfigParser:
    # Database credentials for the test MySQL instance are stored here. This
    # should be an instance in a container, discarded after each test run.
    test_config = configparser.ConfigParser()
    test_config.read(test_ini)
    yield test_config


def mysql_url(config: configparser.ConfigParser):
    """Returns a MySQL URL configured through an ini file.

    The keys and values are:

    [MySQL]
    user       = <database user, defaults to "mlwh">
    password   = <database password, defaults to empty i.e. "">
    ip_address = <database IP address, defaults to "127.0.0.1">
    port       = <database port, defaults to 3306>
    schema     = <database schema, defaults to "mlwh">
    """
    section = "MySQL"

    if section not in config.sections():
        raise configparser.Error(
            f"The {section} configuration section is missing. "
            "You need to fill this in before running "
            f"tests on a {section} database"
        )
    connection_conf = config[section]
    user = connection_conf.get("user", "mlwh")
    password = connection_conf.get("password", "")
    ip_address = connection_conf.get("ip_address", "127.0.0.1")
    port = connection_conf.get("port", "3306")
    schema = connection_conf.get("schema", "mlwh")

    return (
        f"mysql+pymysql://{user}:{password}@"
        f"{ip_address}:{port}/{schema}?charset=utf8mb4"
    )


@pytest.fixture(scope="function")
def mlwh_session(config: configparser.ConfigParser) -> Session:

    uri = mysql_url(config)
    engine = create_engine(uri, echo=False, future=True)

    if not database_exists(engine.url):
        create_database(engine.url)

    with engine.connect() as conn:
        # Workaround for invalid default values for dates.
        conn.execute(text("SET sql_mode = '';"))
        conn.commit()

    Base.metadata.create_all(engine)
    session_maker = sessionmaker(bind=engine)
    sess: Session() = session_maker()

    initialize_mlwh_ont(sess)
    initialize_mlwh_illumina(sess)

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


icommands_have_admin = pytest.mark.skipif(
    not icommands.have_admin(), reason="tests do not have iRODS admin access"
)

NUM_SIMPLE_EXPTS = 5
NUM_MULTIPLEXED_EXPTS = 3
NUM_INSTRUMENT_SLOTS = 5

TEST_GROUPS = ["ss_study_01", "ss_study_02", "ss_study_03"]


def add_test_groups():
    if icommands.have_admin():
        for g in TEST_GROUPS:
            mkgroup(g)


def remove_test_groups():
    if icommands.have_admin():
        for g in TEST_GROUPS:
            rmgroup(g)


def add_rods_path(root_path: PurePath, tmp_path: PurePath) -> PurePath:
    parts = PurePath(*tmp_path.parts[1:])
    rods_path = root_path / parts
    imkdir(rods_path, make_parents=True)

    return rods_path


@pytest.fixture(scope="function")
def simple_collection(tmp_path):
    """A fixture providing an empty collection"""
    root_path = PurePath("/testZone/home/irods/test/simple_collection")
    coll_path = add_rods_path(root_path, tmp_path)

    try:
        yield coll_path
    finally:
        irm(root_path, force=True, recurse=True)


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
        irm(root_path, force=True, recurse=True)


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
        AVU(DublinCore.CREATED, datetime.utcnow().isoformat(timespec="seconds")),
        AVU(DublinCore.CREATOR, "dummy creator"),
        AVU(DublinCore.PUBLISHER, "dummy publisher"),
        AVU(DataFile.TYPE, "txt"),
        AVU(DataFile.MD5, "39a4aa291ca849d601e4e5b8ed627a04"),
    )

    try:
        yield obj_path
    finally:
        irm(root_path, force=True, recurse=True)


@pytest.fixture(scope="function")
def annotated_tree(tmp_path):
    """A fixture providing a tree of collections and data objects, with both
    collections and data objects having annotation."""

    root_path = PurePath("/testZone/home/irods/test/annotated_tree")
    rods_path = add_rods_path(root_path, tmp_path)

    iput("./tests/data/tree", rods_path, recurse=True)
    tree_root = rods_path / "tree"

    add_test_groups()
    ac = AC("ss_study_01", Permission.READ, zone="testZone")

    coll = Collection(tree_root)

    # Create some empty collections
    c = Collection(coll.path / "c")
    c.create()
    for x in ["s", "t", "u"]:
        Collection(c.path / x).create()

    coll.add_metadata(AVU("path", str(coll)))
    coll.add_permissions(ac)

    for item in coll.contents(recurse=True):
        item.add_metadata(AVU("path", str(item)))
        item.add_permissions(ac)

    try:
        yield tree_root
    finally:
        irm(root_path, force=True, recurse=True)
        remove_test_groups()


@pytest.fixture(scope="function")
def ont_gridion(tmp_path):
    """A fixture providing a set of files based on output from an ONT GridION
    instrument. This dataset provides an example of file and directory naming
    conventions. The file contents are dummy values."""
    root_path = PurePath("/testZone/home/irods/test/ont_gridion")
    rods_path = add_rods_path(root_path, tmp_path)

    iput("./tests/data/ont/gridion", rods_path, recurse=True)
    expt_root = rods_path / "gridion"
    add_test_groups()

    try:
        yield expt_root
    finally:
        irm(root_path, force=True, recurse=True)
        remove_test_groups()


@pytest.fixture(scope="function")
def ont_synthetic(tmp_path):
    """A fixture providing a synthetic set of files and metadata based on output
    from an ONT GridION instrument, modified to represent the way simple and
    multiplexed experiments are laid out. The file contents are dummy values."""
    root_path = PurePath("/testZone/home/irods/test/ont_synthetic")
    rods_path = add_rods_path(root_path, tmp_path)

    expt_root = PurePath(rods_path, "synthetic")
    add_test_groups()

    for expt in range(1, NUM_SIMPLE_EXPTS + 1):
        for slot in range(1, NUM_INSTRUMENT_SLOTS + 1):
            expt_name = f"simple_experiment_{expt :0>3}"
            id_flowcell = f"flowcell{slot + 10 :0>3}"
            run_folder = f"20190904_1514_G{slot}00000_{id_flowcell}_69126024"

            coll = Collection(expt_root / expt_name / run_folder)
            coll.create(parents=True)
            meta = [
                avu.with_namespace(Instrument.namespace)
                for avu in [
                    AVU(Instrument.EXPERIMENT_NAME, expt_name),
                    AVU(Instrument.INSTRUMENT_SLOT, f"{slot}"),
                ]
            ]
            coll.add_metadata(*meta)

    for expt in range(1, NUM_MULTIPLEXED_EXPTS + 1):
        for slot in range(1, NUM_INSTRUMENT_SLOTS + 1):
            expt_name = f"multiplexed_experiment_{expt :0>3}"
            id_flowcell = f"flowcell{slot + 100 :0>3}"
            run_folder = f"20190904_1514_GA{slot}0000_{id_flowcell}_cf751ba1"

            coll = Collection(expt_root / expt_name / run_folder)
            coll.create(parents=True)
            meta = [
                avu.with_namespace(Instrument.namespace)
                for avu in [
                    AVU(Instrument.EXPERIMENT_NAME, expt_name),
                    AVU(Instrument.INSTRUMENT_SLOT, f"{slot}"),
                ]
            ]
            coll.add_metadata(*meta)

    # We have synthetic data only for simple_experiment_001 and
    # multiplexed_experiment_001
    iput("./tests/data/ont/synthetic", rods_path, recurse=True)

    try:
        yield expt_root
    finally:
        irm(root_path, force=True, recurse=True)
        remove_test_groups()


@pytest.fixture(scope="function")
def illumina_products(tmp_path):
    root_path = PurePath("/testZone/home/irods/test/illumina_products")
    rods_path = add_rods_path(root_path, tmp_path)

    Collection(rods_path).create(parents=True)

    metadata = {
        "12345/12345#1.cram": (
            AVU("id_product", "31a3d460bb3c7d98845187c716a30db81c44b615"),
            AVU("component", "{'id_run': 12345, 'position': 1, 'tag_index': 1}"),
            AVU("component", "{'id_run': 12345, 'position': 2, 'tag_index': 1}"),
            AVU("reference", "Any/other/reference"),
            AVU("tag_index", 1),
        ),
        "12345/12345#2.cram": (
            AVU("id_product", "0b3bd00f1d186247f381aa87e213940b8c7ab7e5"),
            AVU("component", "{'id_run': 12345, 'position': 1, 'tag_index': 2"),
            AVU("component", "{'id_run': 12345, 'position': 2, 'tag_index': 2"),
            AVU("tag_index", 2),
            AVU("alt_process", "Alternative Process"),
        ),
        "12345/12345#1_phix.cram": (
            AVU("id_product", "31a3d460bb3c7d98845187c716a30db81c44b615"),
            AVU(
                "component",
                "{'id_run': 12345, 'position': 1, 'subset': 'phix', tag_index': 1}",
            ),
            AVU(
                "component",
                "{'id_run': 12345, 'position': 2, 'subset': 'phix', tag_index': 1}",
            ),
            AVU("tag_index", 1),
        ),
        "12345/12345#888.cram": (
            AVU("id_product", "5e67fc5c63b7ceb4e63bbb8e62ab58dcc57b6e64"),
            AVU("component", "{'id_run': 12345, 'position': 1, 'tag_index': 888"),
            AVU("component", "{'id_run': 12345, 'position': 2, 'tag_index': 888"),
            AVU("reference", "A/reference/with/PhiX/present"),
            AVU("tag_index", 888),
        ),
        "12345/12345#0.cram": (
            AVU("id_product", "f54f4a5c3eba5bdf302c1ce4a7c18add33a04315"),
            AVU("component", "{'id_run': 12345, 'position': 1, 'tag_index': 0"),
            AVU("component", "{'id_run': 12345, 'position': 2, 'tag_index': 0"),
            AVU("tag_index", 0),
        ),
        "12345/cellranger/12345.cram": (),
        "54321/54321#1.bam": (
            AVU("id_product", "1a08a7027d9f9c20d01909989370ea6b70a5bccc"),
            AVU("component", "{'id_run': 54321, 'position': 1, 'tag_index': 1}"),
            AVU("component", "{'id_run': 54321, 'position': 2, 'tag_index': 1}"),
            AVU("tag_index", 1),
        ),
        "67890/67890#1.cram": (
            AVU("component", "{'id_run': 54321, 'position': 1, 'tag_index': 1}"),
            AVU("component", "{'id_run': 54321, 'position': 2, 'tag_index': 1}"),
            AVU("tag_index", 1),
        ),
    }

    iput("./tests/data/illumina/mlwh_locations", rods_path, recurse=True)
    for path in metadata.keys():
        obj = DataObject(rods_path / "mlwh_locations" / path)
        for avu in metadata[path]:
            obj.add_metadata(avu)
    try:
        yield rods_path / "mlwh_locations"
    finally:
        irm(root_path, force=True, recurse=True)


BEGIN = datetime(year=2020, month=1, day=1, hour=0, minute=0, second=0)
EARLY = datetime(year=2020, month=6, day=1, hour=0, minute=0, second=0)
LATE = datetime(year=2020, month=6, day=14, hour=0, minute=0, second=0)
LATEST = datetime(year=2020, month=6, day=30, hour=0, minute=0, second=0)


def initialize_mlwh_ont(session: Session):
    """Create test data for all synthetic simple and multiplexed experiments.

    This is a superset of the experiments represented by the files in
    ./tests/data/ont/synthetic
    """
    instrument_name = "instrument_01"
    pipeline_id_lims = "Ligation"
    req_data_type = "Basecalls and raw data"
    default_timestamps = {"last_updated": BEGIN, "recorded_at": BEGIN}

    study_x = Study(
        id_lims="LIMS_01",
        id_study_lims="study_01",
        name="Study X",
        **default_timestamps,
    )
    study_y = Study(
        id_lims="LIMS_01",
        id_study_lims="study_02",
        name="Study Y",
        **default_timestamps,
    )
    study_z = Study(
        id_lims="LIMS_01",
        id_study_lims="study_03",
        name="Study Z",
        **default_timestamps,
    )
    session.add_all([study_x, study_y, study_z])
    session.flush()

    samples = []
    flowcells = []

    num_samples = 200
    for s in range(1, num_samples + 1):
        sid = f"sample{s}"
        name = f"sample {s}"
        samples.append(
            Sample(
                id_lims="LIMS_01", id_sample_lims=sid, name=name, **default_timestamps
            )
        )
    session.add_all(samples)
    session.flush()

    sample_idx = 0
    for expt in range(1, NUM_SIMPLE_EXPTS + 1):
        for slot in range(1, NUM_INSTRUMENT_SLOTS + 1):
            expt_name = f"simple_experiment_{expt :0>3}"
            id_flowcell = f"flowcell{slot + 10 :0>3}"

            # All the even experiments have the early datetime
            # All the odd experiments have the late datetime
            when_expt = EARLY if expt % 2 == 0 else LATE

            flowcells.append(
                OseqFlowcell(
                    sample=samples[sample_idx],
                    study=study_y,
                    instrument_name=instrument_name,
                    instrument_slot=slot,
                    experiment_name=expt_name,
                    id_lims=f"Example LIMS ID {sample_idx}",
                    id_flowcell_lims=id_flowcell,
                    pipeline_id_lims=pipeline_id_lims,
                    requested_data_type=req_data_type,
                    last_updated=when_expt,
                    recorded_at=BEGIN,
                )
            )
            sample_idx += 1

    barcodes = [
        "CACAAAGACACCGACAACTTTCTT",
        "ACAGACGACTACAAACGGAATCGA",
        "CCTGGTAACTGGGACACAAGACTC",
        "TAGGGAAACACGATAGAATCCGAA",
        "AAGGTTACACAAACCCTGGACAAG",
        "GACTACTTTCTGCCTTTGCGAGAA",
        "AAGGATTCATTCCCACGGTAACAC",
        "ACGTAACTTGGTTTGTTCCCTGAA",
        "AACCAAGACTCGCTGTGCCTAGTT",
        "GAGAGGACAAAGGTTTCAACGCTT",
        "TCCATTCCCTCCGATAGATGAAAC",
        "TCCGATTCTGCTTCTTTCTACCTG",
    ]

    msample_idx = 0
    for expt in range(1, NUM_MULTIPLEXED_EXPTS + 1):
        for slot in range(1, NUM_INSTRUMENT_SLOTS + 1):
            expt_name = f"multiplexed_experiment_{expt :0>3}"
            id_flowcell = f"flowcell{slot + 100 :0>3}"

            # All the even experiments have the early datetime
            when = EARLY

            # All the odd experiments have the late datetime
            if expt % 2 == 1:
                when = LATE
                # Or latest if they have an odd instrument position
                if slot % 2 == 1:
                    when = LATEST

            for barcode_idx, barcode in enumerate(barcodes):
                tag_id = f"ONT_EXP-012-{ barcode_idx + 1 :02d}"

                flowcells.append(
                    OseqFlowcell(
                        sample=samples[msample_idx],
                        study=study_z,
                        instrument_name=instrument_name,
                        instrument_slot=slot,
                        experiment_name=expt_name,
                        id_lims=f"Example LIMS ID m{msample_idx}",
                        id_flowcell_lims=id_flowcell,
                        tag_set_id_lims="ONT_12",
                        tag_set_name="ONT library barcodes x12",
                        tag_sequence=barcode,
                        tag_identifier=tag_id,
                        pipeline_id_lims=pipeline_id_lims,
                        requested_data_type=req_data_type,
                        last_updated=when,
                        recorded_at=BEGIN,
                    )
                )
                msample_idx += 1

    session.add_all(flowcells)
    session.commit()


def initialize_mlwh_illumina(sess: Session):

    changed_study = Study(
        id_lims="LIMS_05",
        id_study_lims="study_04",
        name="Recently Changed",
        study_title="Recently changed study",
        accession_number="ST0000000001",
        last_updated=BEGIN,
        recorded_at=LATEST,
    )
    unchanged_study = Study(
        id_lims="LIMS_05",
        id_study_lims="study_05",
        name="Unchanged",
        study_title="Unchanged study",
        accession_number="ST0000000002",
        last_updated=BEGIN,
        recorded_at=BEGIN,
    )
    sess.add_all([changed_study, unchanged_study])
    sess.commit()
    sess.refresh(changed_study)
    sess.refresh(unchanged_study)

    changed_sample = Sample(
        id_lims="LIMS_05",
        id_sample_lims="SAMPLE_01",
        name="Recently changed",
        accession_number="SA000001",
        public_name="Recently changed",
        common_name="Recently changed",
        supplier_name="Recently_changed_supplier",
        cohort="cohort_01",
        donor_id="DONOR_01",
        consent_withdrawn=0,
        last_updated=BEGIN,
        recorded_at=LATEST,
    )
    unchanged_sample = Sample(
        id_lims="LIMS_05",
        id_sample_lims="SAMPLE_02",
        name="Unchanged",
        accession_number="SA000002",
        public_name="Unchanged",
        common_name="Unchanged",
        supplier_name="Unchanged_supplier",
        cohort="cohort_02",
        donor_id="DONOR_02",
        consent_withdrawn=0,
        last_updated=BEGIN,
        recorded_at=BEGIN,
    )
    sess.add_all([changed_sample, unchanged_sample])
    sess.commit()
    sess.refresh(changed_sample)
    sess.refresh(unchanged_sample)

    study_changed_flowcell = IseqFlowcell(
        id_lims="LIMS_05",
        id_flowcell_lims="FLOWCELL_01",
        id_library_lims="LIBRARY_01",
        primer_panel="Primer_panel_01",
        position=1,
        last_updated=BEGIN,
        recorded_at=BEGIN,
        id_study_tmp=changed_study.id_study_tmp,
        id_sample_tmp=unchanged_sample.id_sample_tmp,
        entity_type="library_indexed",
        entity_id_lims="ENTITY_01",
        id_pool_lims="ABC1234",
    )
    sample_changed_flowcell = IseqFlowcell(
        id_lims="LIMS_05",
        id_flowcell_lims="FLOWCELL_02",
        id_library_lims="LIBRARY_02",
        primer_panel="Primer_panel_02",
        position=2,
        last_updated=BEGIN,
        recorded_at=BEGIN,
        id_study_tmp=unchanged_study.id_study_tmp,
        id_sample_tmp=changed_sample.id_sample_tmp,
        entity_type="library_indexed",
        entity_id_lims="ENTITY_01",
        id_pool_lims="ABC1234",
    )
    product_changed_flowcell = IseqFlowcell(
        id_lims="LIMS_05",
        id_flowcell_lims="FLOWCELL_03",
        id_library_lims="LIBRARY_03",
        primer_panel="Primer_panel_03",
        position=1,
        last_updated=BEGIN,
        recorded_at=LATEST,
        id_study_tmp=unchanged_study.id_study_tmp,
        id_sample_tmp=unchanged_sample.id_sample_tmp,
        entity_type="library_indexed",
        entity_id_lims="ENTITY_01",
        id_pool_lims="ABC1234",
    )
    self_changed_flowcell = IseqFlowcell(
        id_lims="LIMS_05",
        id_flowcell_lims="FLOWCELL_04",
        id_library_lims="LIBRARY_04",
        primer_panel="Primer_panel_04",
        position=2,
        last_updated=BEGIN,
        recorded_at=LATEST,
        id_study_tmp=unchanged_study.id_study_tmp,
        id_sample_tmp=unchanged_sample.id_sample_tmp,
        entity_type="library_indexed",
        entity_id_lims="ENTITY_01",
        id_pool_lims="ABC1234",
    )
    no_change_flowcell = IseqFlowcell(
        id_lims="LIMS_05",
        id_flowcell_lims="FLOWCELL_05",
        id_library_lims="LIBRARY_05",
        primer_panel="Primer_panel_05",
        position=2,
        last_updated=BEGIN,
        recorded_at=BEGIN,
        id_study_tmp=unchanged_study.id_study_tmp,
        id_sample_tmp=unchanged_sample.id_sample_tmp,
        entity_type="library_indexed",
        entity_id_lims="ENTITY_01",
        id_pool_lims="ABC1234",
    )
    flowcells = [
        study_changed_flowcell,
        sample_changed_flowcell,
        self_changed_flowcell,
        product_changed_flowcell,
        no_change_flowcell,
    ]
    sess.add_all(flowcells)
    sess.commit()
    for flowcell in flowcells:
        sess.refresh(flowcell)

    study_changed_product_metrics = IseqProductMetrics(
        id_iseq_product="PRODUCT_01",
        id_run=12111,
        last_changed=BEGIN,
        id_iseq_flowcell_tmp=study_changed_flowcell.id_iseq_flowcell_tmp,
        qc="study_changed_qc",
    )
    sample_changed_product_metrics = IseqProductMetrics(
        id_iseq_product="PRODUCT_02",
        id_run=12111,
        last_changed=BEGIN,
        id_iseq_flowcell_tmp=sample_changed_flowcell.id_iseq_flowcell_tmp,
        qc="sample_changed_qc",
    )
    flowcell_changed_product_metrics = IseqProductMetrics(
        id_iseq_product="PRODUCT_03",
        id_run=12111,
        last_changed=BEGIN,
        id_iseq_flowcell_tmp=self_changed_flowcell.id_iseq_flowcell_tmp,
        qc="flowcell_changed_qc",
    )
    self_changed_product_metrics = IseqProductMetrics(
        id_iseq_product="PRODUCT_04",
        id_run=12111,
        last_changed=LATEST,
        id_iseq_flowcell_tmp=product_changed_flowcell.id_iseq_flowcell_tmp,
        qc="self_changed_qc",
    )

    no_change_product_metrics = IseqProductMetrics(
        id_iseq_product="PRODUCT_05",
        id_run=12111,
        last_changed=BEGIN,
        id_iseq_flowcell_tmp=no_change_flowcell.id_iseq_flowcell_tmp,
        qc="no_change_qc",
    )
    sess.add_all(
        [
            study_changed_product_metrics,
            sample_changed_product_metrics,
            flowcell_changed_product_metrics,
            self_changed_product_metrics,
            no_change_product_metrics,
        ]
    )
    sess.commit()
