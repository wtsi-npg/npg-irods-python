# -*- coding: utf-8 -*-
#
# Copyright © 2024 Genome Research Ltd. All rights reserved.
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

from pathlib import PurePath

import pytest
from partisan.icommands import iput
from partisan.irods import AVU, Collection, DataObject
from sqlalchemy.orm import Session

from helpers import (
    BEGIN,
    CREATED,
    LATEST,
    add_rods_path,
    remove_rods_path,
)
from npg_irods.db.mlwh import IseqFlowcell, IseqProductMetrics, Sample, Study
from npg_irods.illumina import EntityType
from npg_irods.metadata import illumina
from npg_irods.metadata.common import SeqConcept


def initialize_mlwh_illumina_synthetic(session: Session):
    """Insert ML warehouse test data for synthetic simple and multiplexed Illumina runs.

    This is represented by the files in ./tests/data/illumina/synthetic
    """
    default_timestamps = {
        "created": CREATED,
        "last_updated": BEGIN,
        "recorded_at": BEGIN,
    }
    default_run = 12345

    study_a = Study(
        id_lims="LIMS_01", id_study_lims="4000", name="Study A", **default_timestamps
    )
    study_b = Study(
        id_lims="LIMS_01", id_study_lims="5000", name="Study B", **default_timestamps
    )
    control_study = Study(
        id_lims="LIMS_888",
        id_study_lims="888",
        name="Control Study",
        **default_timestamps,
    )
    session.add_all([study_a, study_b, control_study])

    def make_sample(n):
        return Sample(
            common_name=f"common_name{n}",
            donor_id=f"donor_id{n}",
            id_lims="LIMS_01",
            id_sample_lims=f"id_sample_lims{n}",
            name=f"name{n}",
            public_name=f"public_name{n}",
            supplier_name=f"supplier_name{n}",
            uuid_sample_lims=f"52429892-0ab6-11ee-b5ba-fa163eac3af{n}",
            **default_timestamps,
        )

    samples = [make_sample(n) for n in range(1, 4)]
    control_sample = Sample(
        id_lims="LIMS_888",
        id_sample_lims="phix",
        name="Phi X",
        uuid_sample_lims="42429892-0ab6-11ee-b5ba-fa163eac3af3",
        **default_timestamps,
    )
    session.add_all([*samples, control_sample])

    sample_info = [
        # Not multiplexed
        {
            "study": study_a,
            "sample": samples[0],
            "position": 1,
            "tag_index": None,
            "entity_type": EntityType.LIBRARY.value,
        },
        # Multiplexed, samples from the same study
        {
            "study": study_a,
            "sample": samples[0],
            "position": 1,
            "tag_index": 1,
            "entity_type": EntityType.LIBRARY_INDEXED.value,
        },
        {
            "study": study_a,
            "sample": samples[1],
            "position": 1,
            "tag_index": 2,
            "entity_type": EntityType.LIBRARY_INDEXED.value,
        },
        {
            "study": study_a,
            "sample": samples[0],
            "position": 2,
            "tag_index": 1,
            "entity_type": EntityType.LIBRARY_INDEXED.value,
        },
        {
            "study": study_a,
            "sample": samples[1],
            "position": 2,
            "tag_index": 2,
            "entity_type": EntityType.LIBRARY_INDEXED.value,
        },
        # Multiplexed, samples from different studies
        {
            "study": study_a,
            "sample": samples[0],
            "position": 2,
            "tag_index": 1,
            "entity_type": EntityType.LIBRARY_INDEXED.value,
        },
        {
            "study": study_b,
            "sample": samples[2],
            "position": 2,
            "tag_index": 2,
            "entity_type": EntityType.LIBRARY_INDEXED.value,
        },
        # Phi X
        {
            "study": control_study,
            "sample": control_sample,
            "position": 1,
            "tag_index": 888,
            "entity_type": EntityType.LIBRARY_INDEXED_SPIKE.value,
        },
        {
            "study": control_study,
            "sample": control_sample,
            "position": 2,
            "tag_index": 888,
            "entity_type": EntityType.LIBRARY_INDEXED_SPIKE.value,
        },
    ]

    default_fc_timestamps = {
        "last_updated": BEGIN,
        "recorded_at": BEGIN,
    }

    flowcells = [
        IseqFlowcell(
            entity_id_lims=f"ENTITY_01",
            entity_type=info["entity_type"],
            id_flowcell_lims=f"FLOWCELL{i}",
            id_lims="LIMS_01",
            id_pool_lims=f"POOL_01",
            position=info["position"],
            sample=info["sample"],
            study=info["study"],
            tag_index=info["tag_index"],
            **default_fc_timestamps,
        )
        for i, info in enumerate(sample_info)
    ]
    session.add_all(flowcells)

    product_metrics = [
        IseqProductMetrics(
            id_iseq_product=f"product{i}",
            id_run=default_run,
            iseq_flowcell=fc,
            last_changed=BEGIN,
            position=fc.position,
            tag_index=fc.tag_index,
        )
        for i, fc in enumerate(flowcells)
    ]
    session.add_all(product_metrics)
    session.commit()


def initialize_mlwh_illumina_backfill(sess: Session):
    """Insert ML warehouse test data for Illumina product iRODS paths."""
    changed_study = Study(
        id_lims="LIMS_05",
        id_study_lims="4000",
        name="Recently Changed",
        study_title="Recently changed study",
        accession_number="ST0000000001",
        created=CREATED,
        last_updated=BEGIN,
        recorded_at=LATEST,
    )
    unchanged_study = Study(
        id_lims="LIMS_05",
        id_study_lims="5000",
        name="Unchanged",
        study_title="Unchanged study",
        accession_number="ST0000000002",
        created=CREATED,
        last_updated=BEGIN,
        recorded_at=BEGIN,
    )
    sess.add_all([changed_study, unchanged_study])
    sess.flush()

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
        created=CREATED,
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
        created=CREATED,
        last_updated=BEGIN,
        recorded_at=BEGIN,
    )
    sess.add_all([changed_sample, unchanged_sample])
    sess.flush()

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
    sess.flush()

    study_changed_product_metrics = IseqProductMetrics(
        id_iseq_product="PRODUCT_01",
        id_run=12111,
        last_changed=BEGIN,
        id_iseq_flowcell_tmp=study_changed_flowcell.id_iseq_flowcell_tmp,
        qc=0,
    )
    sample_changed_product_metrics = IseqProductMetrics(
        id_iseq_product="PRODUCT_02",
        id_run=12111,
        last_changed=BEGIN,
        id_iseq_flowcell_tmp=sample_changed_flowcell.id_iseq_flowcell_tmp,
        qc=0,
    )
    flowcell_changed_product_metrics = IseqProductMetrics(
        id_iseq_product="PRODUCT_03",
        id_run=12111,
        last_changed=BEGIN,
        id_iseq_flowcell_tmp=self_changed_flowcell.id_iseq_flowcell_tmp,
        qc=0,
    )
    self_changed_product_metrics = IseqProductMetrics(
        id_iseq_product="PRODUCT_04",
        id_run=12111,
        last_changed=LATEST,
        id_iseq_flowcell_tmp=product_changed_flowcell.id_iseq_flowcell_tmp,
        qc=0,
    )

    no_change_product_metrics = IseqProductMetrics(
        id_iseq_product="PRODUCT_05",
        id_run=12111,
        last_changed=BEGIN,
        id_iseq_flowcell_tmp=no_change_flowcell.id_iseq_flowcell_tmp,
        qc=0,
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


@pytest.fixture(scope="function")
def illumina_synthetic_mlwh(mlwh_session) -> Session:
    """An ML warehouse database fixture populated with Illumina-related records."""
    initialize_mlwh_illumina_synthetic(mlwh_session)
    yield mlwh_session


@pytest.fixture(scope="function")
def illumina_backfill_mlwh(mlwh_session) -> Session:
    """An ML warehouse database fixture populated with Illumina iRODS path backfill
    records."""
    initialize_mlwh_illumina_backfill(mlwh_session)
    yield mlwh_session


@pytest.fixture(scope="function")
def illumina_synthetic_irods(tmp_path, irods_groups):
    root_path = PurePath("/testZone/home/irods/test/illumina_synthetic_irods")
    rods_path = add_rods_path(root_path, tmp_path)

    Collection(rods_path).create(parents=True)

    run = illumina.Instrument.RUN
    pos = illumina.Instrument.LANE
    tag = SeqConcept.TAG_INDEX
    idp = SeqConcept.ID_PRODUCT
    cmp = SeqConcept.COMPONENT
    ref = SeqConcept.REFERENCE

    run_pos = [AVU(run, 12345), AVU(pos, 1), AVU(pos, 2)]

    metadata = {
        "12345/12345.cram": (
            AVU(cmp, '{"id_run":12345, "position":1}'),
            AVU(ref, "Any/other/reference"),
            AVU(run, 12345),
            AVU(pos, 1),
        ),
        "12345/12345#1.cram": (
            AVU(idp, "31a3d460bb3c7d98845187c716a30db81c44b615"),
            AVU(cmp, '{"id_run":12345, "position":1, "tag_index":1}'),
            AVU(cmp, '{"id_run":12345, "position":2, "tag_index":1}'),
            AVU(ref, "Any/other/reference"),
            *run_pos,
            AVU(tag, 1),
        ),
        "12345/12345#1_human.cram": (
            AVU(idp, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            AVU(cmp, '{"id_run":12345, "position":1, "tag_index":1, "subset":"human"}'),
            AVU(cmp, '{"id_run":12345, "position":2, "tag_index":1, "subset":"human"}'),
            *run_pos,
            AVU(tag, 1),
        ),
        "12345/12345#1_xahuman.cram": (
            AVU(idp, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
            AVU(
                cmp, '{"id_run":12345, "position":1, "tag_index":1, "subset":"xahuman"}'
            ),
            AVU(
                cmp, '{"id_run":12345, "position":2, "tag_index":1, "subset":"xahuman"}'
            ),
            *run_pos,
            AVU(tag, 1),
        ),
        "12345/12345#2.cram": (
            AVU(idp, "0b3bd00f1d186247f381aa87e213940b8c7ab7e5"),
            AVU(cmp, '{"id_run":12345, "position":1, "tag_index":2}'),
            AVU(cmp, '{"id_run":12345, "position":2, "tag_index":2}'),
            AVU(run, 12345),
            AVU(pos, 1),
            AVU(pos, 2),
            AVU(tag, 2),
            AVU(SeqConcept.ALT_PROCESS, "Alternative Process"),
        ),
        "12345/12345#1_phix.cram": (
            AVU(idp, "31a3d460bb3c7d98845187c716a30db81c44b615"),
            AVU(cmp, '{"id_run":12345, "position":1, "subset":"phix", "tag_index":1}'),
            AVU(cmp, '{"id_run":12345, "position":2, "subset":"phix", "tag_index":1}'),
            *run_pos,
            AVU(tag, 1),
        ),
        "12345/12345#888.cram": (
            AVU(idp, "5e67fc5c63b7ceb4e63bbb8e62ab58dcc57b6e64"),
            AVU(cmp, '{"id_run":12345, "position":1, "tag_index":888}'),
            AVU(cmp, '{"id_run":12345, "position":2, "tag_index":888}'),
            AVU(ref, "A/reference/with/PhiX/present"),
            *run_pos,
            AVU(tag, 888),
        ),
        "12345/12345#0.cram": (
            AVU(idp, "f54f4a5c3eba5bdf302c1ce4a7c18add33a04315"),
            AVU(cmp, '{"id_run":12345, "position":1, "tag_index":0}'),
            AVU(cmp, '{"id_run":12345, "position":2, "tag_index":0}'),
            *run_pos,
            AVU(tag, 0),
        ),
        "12345/cellranger/12345.cram": (),
        "54321/54321#1.bam": (
            AVU(idp, "1a08a7027d9f9c20d01909989370ea6b70a5bccc"),
            AVU(cmp, '{"id_run":54321, "position":1, "tag_index":1}'),
            AVU(cmp, '{"id_run":54321, "position":2, "tag_index":1}'),
            AVU(run, 54321),
            AVU(tag, 1),
        ),
        "67890/67890#1.cram": (
            AVU(cmp, '{"id_run":54321, "position":1, "tag_index":1}'),
            AVU(cmp, '{"id_run":54321, "position":2, "tag_index":1}'),
            AVU(run, 67890),
            AVU(tag, 1),
        ),
    }

    iput("./tests/data/illumina/synthetic", rods_path, recurse=True)
    for path in metadata.keys():
        obj = DataObject(rods_path / "synthetic" / path)
        for avu in metadata[path]:
            obj.add_metadata(avu)

    try:
        yield rods_path / "synthetic"
    finally:
        remove_rods_path(rods_path)
