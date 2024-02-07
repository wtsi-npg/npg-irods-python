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
from npg_id_generation.pac_bio import PacBioEntity
from partisan.icommands import iput
from partisan.irods import AVU, Collection, DataObject
from sqlalchemy.orm import Session

from helpers import (
    BEGIN,
    EARLY,
    LATE,
    add_rods_path,
    add_test_groups,
    remove_rods_path,
    remove_test_groups,
)
from npg_irods.metadata.common import SeqConcept
from npg_irods.metadata.pacbio import Instrument, remove_well_padding
from npg_irods.db.mlwh import (
    PacBioProductMetrics,
    PacBioRun,
    PacBioRunWellMetrics,
    Sample,
    Study,
)


def make_product_id(run: str, well: str, plate=None, tag=None):
    # Remove padding from well label; e.g. A01 -> A1
    well_depad = remove_well_padding(well)
    return PacBioEntity(
        run_name=run, well_label=well_depad, plate_number=plate, tags=tag
    ).hash_product_id()


def make_pacbio_fixture(session, num_runs=2, num_wells=2, num_tags=2, is_revio=True):
    """Insert ML warehouse test data for synthetic PacBio data.

    Even-numbered runs were done EARLY. Odd-numbered runs were done LATE.

    NB. If you change the default values for num_runs, num_wells or num_tags, you will
    need to add more tags to the make_tag function.
    """
    id_lims = "LIMS_01"
    default_timestamps = {"last_updated": BEGIN, "recorded_at": BEGIN}

    study_x = Study(
        id_lims="LIMS_01", id_study_lims="1000", name="Study X", **default_timestamps
    )
    session.add(study_x)

    def make_sample(n):
        return Sample(
            accession_number=f"ACC{n}",
            common_name=f"common_name{n}",
            donor_id=f"donor_id{n}",
            id_lims=id_lims,
            id_sample_lims=f"id_sample_lims{n}",
            name=f"name{n}",
            public_name=f"public_name{n}",
            supplier_name=f"supplier_name{n}",
            **default_timestamps,
        )

    def make_tag(n):
        """Return a tuple of (tag_id, tag_sequence) for the nth tag."""
        if n < 0 or n > 15:
            raise ValueError(f"n must be in the range 0 to 15 but was: {n}")

        tags = [
            "AAAAAAAAAA",
            "GGGGGGGGGG",
            "CCCCCCCCCC",
            "TTTTTTTTTT",
            "AAAGGGGGGG",
            "AAACCCCCCC",
            "AAATTTTTTT",
            "GGGAAAAAAA",
            "GGGCCCCCCC",
            "GGGTTTTTTT",
            "CCCAAAAAAA",
            "CCCGGGGGGG",
            "CCCTTTTTTT",
            "TTTAAAAAAA",
            "TTTGGGGGGG",
            "TTTCCCCCCC",
        ]
        return f"bc{n:0>4}", tags[n]

    samples = []
    runs = []
    num_plates = 2 if is_revio else 1

    sample_idx = 1
    for r in range(1, num_runs + 1):
        when = EARLY if r % 2 == 0 else LATE

        for p in range(1, num_plates + 1):
            for w in range(1, num_wells + 1):
                for t in range(1, num_tags + 1):
                    sample = make_sample(sample_idx)
                    samples.append(sample)

                    tag_id, tag_seq = make_tag(sample_idx - 1)
                    sample_idx += 1

                    plate_number = p if is_revio else None
                    run = PacBioRun(
                        id_lims=id_lims,
                        sample=sample,
                        study=study_x,
                        id_pac_bio_run_lims=f"id_run_lims{r}",
                        pac_bio_run_name=f"run{r}",
                        well_label=f"A{w}",  # Well label x-coordinates are unpadded
                        plate_number=plate_number,
                        tag_identifier=tag_id,
                        tag_sequence=tag_seq,
                        last_updated=when,
                        recorded_at=when,
                    )
                    runs.append(run)

    session.add_all(samples)
    session.add_all(runs)

    rmw_added = {}
    for run in runs:
        key = (run.pac_bio_run_name, run.well_label, run.plate_number)
        # Make only one PacBioRunWellMetrics for each (run, well and plate) tuple
        # because the database has a unique constraint on these columns.
        if key not in rmw_added:
            rwm = PacBioRunWellMetrics(
                pac_bio_run_name=run.pac_bio_run_name,
                well_label=run.well_label,
                plate_number=run.plate_number,
                id_pac_bio_product=make_product_id(
                    run.pac_bio_run_name,
                    run.well_label,
                    run.plate_number,
                ),  # Well product ID
                last_changed=LATE,
            )
            rmw_added[key] = rwm
            session.add(rwm)

        pm = PacBioProductMetrics(
            pac_bio_run=run,
            pac_bio_run_well_metrics=rmw_added[key],
            id_pac_bio_product=make_product_id(
                run.pac_bio_run_name, run.well_label, run.plate_number, run.tag_sequence
            ),  # Tag product ID
            qc=True,
            last_changed=LATE,
        )
        session.add(pm)
    session.commit()


def initialize_mlwh_pacbio_synthetic(session: Session):
    """Insert ML warehouse test data for synthetic PacBio pre-style data.

    PacBio instruments prior to Revio use a single plate and consequently no plate
    number was recorded (plate_number in the ML warehouse is null).

    Synthetic PacBio run data for 2 runs, each run having one plate, with 2 wells and
    each well having 2 tags. Each combination of run, well and tag has a sample.
    All samples are in study X.
    """
    make_pacbio_fixture(session, is_revio=False)


def initialize_mlwh_revio_synthetic(session: Session):
    """Insert ML warehouse test data for synthetic PacBio Revio-style data.

    Revio differs from earlier PacBio instruments in that it allows two plates to be
    loaded, therefore the plate number, well and tag are required to uniquely identify
    a sample.

    Synthetic PacBio run data for 2 runs, each run having two plates, each plate
    having 2 wells and each well having 2 tags. Each combination of run, plate, well
    and tag has a sample. All samples are in study X.
    """
    make_pacbio_fixture(session, is_revio=True)


@pytest.fixture(scope="function")
def pacbio_synthetic_mlwh(mlwh_session) -> Session:
    """An ML warehouse database fixture populated with PacBio-related records."""
    initialize_mlwh_pacbio_synthetic(mlwh_session)
    yield mlwh_session


@pytest.fixture(scope="function")
def revio_synthetic_mlwh(mlwh_session) -> Session:
    """An ML warehouse database fixture populated with PacBio Revio-related records."""
    initialize_mlwh_revio_synthetic(mlwh_session)
    yield mlwh_session


@pytest.fixture(scope="function")
def pacbio_synthetic_irods(tmp_path):
    root_path = PurePath("/testZone/home/irods/test/pacbio_synthetic_irods")
    rods_path = add_rods_path(root_path, tmp_path)

    Collection(rods_path).create(parents=True)
    add_test_groups()
    run = "run1"
    plate = 1
    well = "A01"
    tag_id = "bc0001"
    tag = "AAAAAAAAAA"
    source = "production"

    common_avus = [
        AVU(Instrument.RUN_NAME, run),
        AVU(Instrument.PLATE_NUMBER, plate),
        AVU(Instrument.WELL_LABEL, well),
        AVU(Instrument.TAG_IDENTIFIER, tag_id),
        AVU(Instrument.TAG_SEQUENCE, tag),
        AVU(Instrument.SOURCE, source),
    ]

    # Any subset of reads which are of insufficient quality or cannot be assigned to
    # a tag are given the same products ID as the well. They are given same permissions
    # as the good quality reads.
    well_product_avus = [
        *common_avus,
        AVU(SeqConcept.ID_PRODUCT, make_product_id(run, well, plate=plate)),
    ]
    tag_product_avus = [
        *common_avus,
        AVU(SeqConcept.ID_PRODUCT, make_product_id(run, well, plate=plate, tag=tag)),
    ]

    metadata = {
        "m12345_246789_987655_s3.hifi_reads.bc1000.bam": tag_product_avus,
        "m12345_246789_987655_s3.fail_reads.bc1000.bam": well_product_avus,
        "m12345_246789_987655_s3.hifi_reads.unassigned.bam": well_product_avus,
        "m12345_246789_987655_s3.fail_reads.unassigned.bam": well_product_avus,
    }

    iput("./tests/data/pacbio/synthetic", rods_path, recurse=True)
    for path in metadata.keys():
        obj = DataObject(
            rods_path / PurePath("synthetic", "r12345_20246789_98765", "1_A01", path)
        )
        for avu in metadata[path]:
            obj.add_metadata(avu)

    try:
        yield rods_path / "synthetic"
    finally:
        remove_rods_path(rods_path)
        remove_test_groups()


@pytest.fixture(scope="function")
def pacbio_requires_id(tmp_path):
    """A fixture providing a data object which requires a product ID."""
    root_path = PurePath("/testZone/home/irods/test")
    rods_path = add_rods_path(root_path, tmp_path)

    obj_path = rods_path / "pb.bam"
    iput("./tests/data/simple/data_object/lorem.txt", obj_path)

    try:
        yield obj_path
    finally:
        remove_rods_path(rods_path)


@pytest.fixture(scope="function")
def pacbio_has_id(pacbio_requires_id):
    """A fixture providing a data object that has a product ID in its metadata"""

    obj = DataObject(pacbio_requires_id)
    obj.add_metadata(AVU(SeqConcept.ID_PRODUCT, "abcde12345"))

    yield pacbio_requires_id
