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

from pathlib import PurePath

import pytest
from partisan.icommands import iput
from partisan.irods import AVU, Collection
from sqlalchemy.orm import Session

from helpers import (
    BEGIN,
    CREATED,
    EARLY,
    LATE,
    LATEST,
    add_rods_path,
    add_test_groups,
    remove_rods_path,
    remove_test_groups,
)
from npg_irods.db.mlwh import OseqFlowcell, Sample, Study
from npg_irods.metadata import ont

# Counts of test fixture experiments
NUM_SIMPLE_EXPTS = 5
NUM_MULTIPLEXED_EXPTS = 3
NUM_INSTRUMENT_SLOTS = 5


def ont_tag_identifier(tag_index: int) -> str:
    """Return an ONT tag identifier in tag set EXP-NBD104, given a tag index."""
    return f"NB{tag_index :02d}"


def initialize_mlwh_ont_synthetic(session: Session):
    """Insert ML warehouse test data for all synthetic simple and multiplexed
    ONT experiments.

    Even-numbered experiments were done EARLY. Odd-numbered experiments were done LATE
    if they were on an even instrument position, or LATEST if they were on an odd
    instrument position.

    This is a superset of the experiments represented by the files in
    ./tests/data/ont/synthetic
    """
    instrument_name = "instrument_01"
    default_timestamps = {
        "created": CREATED,
        "last_updated": BEGIN,
        "recorded_at": BEGIN,
    }

    study_x = Study(
        id_lims="LIMS_01", id_study_lims="1000", name="Study X", **default_timestamps
    )
    study_y = Study(
        id_lims="LIMS_01", id_study_lims="2000", name="Study Y", **default_timestamps
    )
    study_z = Study(
        id_lims="LIMS_01", id_study_lims="3000", name="Study Z", **default_timestamps
    )
    session.add_all([study_x, study_y, study_z])

    def make_sample(n):
        return Sample(
            accession_number=f"ACC{n}",
            common_name=f"common_name{n}",
            donor_id=f"donor_id{n}",
            id_lims="LIMS_01",
            id_sample_lims=f"id_sample_lims{n}",
            name=f"name{n}",
            public_name=f"public_name{n}",
            supplier_name=f"supplier_name{n}",
            **default_timestamps,
        )

    def make_simple_flowcell(ex, sl, n):
        """Make a non-multiplexed flowcell given an experiment number, instrument slot
        and sample index."""
        # All the even experiments have the early datetime
        # All the odd experiments have the late datetime
        when = EARLY if ex % 2 == 0 else LATE

        return OseqFlowcell(
            sample=samples[n],
            study=study_y,
            instrument_name=instrument_name,
            instrument_slot=sl,
            experiment_name=f"simple_experiment_{ex :0>3}",
            id_lims=f"Example LIMS ID {n}",
            id_flowcell_lims=f"flowcell{sl + 10 :0>3}",
            last_updated=when,
            recorded_at=when,
        )

    num_samples = 200
    samples = [make_sample(n) for n in range(1, num_samples + 1)]
    session.add_all(samples)

    flowcells = []

    sample_idx = 0
    for expt in range(1, NUM_SIMPLE_EXPTS + 1):
        for slot in range(1, NUM_INSTRUMENT_SLOTS + 1):
            flowcells.append(make_simple_flowcell(expt, slot, sample_idx))
        sample_idx += 1

    def make_mplex_flowcell(ex_name, ex_n, fc_start, sl, tid, bc, n):
        """Make a multiplexed flowcell given an experiment name, experiment number,
        flowcell start idx, instrument slot, tag identifier, barcode and sample index.
        """

        when = EARLY  # All the even experiments have the early datetime
        if ex_n % 2 == 1:
            when = LATE  # All the odd experiments have the late datetime
            if sl % 2 == 1:
                when = LATEST  # Or latest if they have an odd instrument position

        return OseqFlowcell(
            sample=samples[n],
            study=study_z,
            instrument_name=instrument_name,
            instrument_slot=sl,
            experiment_name=f"{ex_name}_{ex_n :0>3}",
            id_lims=f"Example LIMS ID {n}",
            id_flowcell_lims=f"flowcell{sl + fc_start :0>3}",
            tag_set_id_lims="Example LIMS tag set ID",
            tag_set_name="EXP-NBD104",
            tag_sequence=bc,
            tag_identifier=tid,
            last_updated=when,
            recorded_at=when,
        )

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
            for barcode_idx, barcode in enumerate(barcodes):
                # The tag_id format and tag_set_name  are taken from the Guppy barcode
                # arrangement file barcode_arrs_nb12.toml distributed with Guppy and
                # MinKNOW.
                tag_id = ont_tag_identifier(barcode_idx + 1)
                flowcells.append(
                    make_mplex_flowcell(
                        "multiplexed_experiment",
                        expt,
                        100,
                        slot,
                        tag_id,
                        barcode,
                        msample_idx,
                    )
                )
                msample_idx += 1

    msample_idx = 0
    for expt in range(1, NUM_MULTIPLEXED_EXPTS + 1):
        for slot in range(1, NUM_INSTRUMENT_SLOTS + 1):
            for barcode_idx, barcode in enumerate(barcodes[:5]):
                tag_id = ont_tag_identifier(barcode_idx + 1)
                flowcells.extend(
                    [
                        make_mplex_flowcell(
                            "old_rebasecalled_multiplexed_experiment",
                            expt,
                            200,
                            slot,
                            tag_id,
                            barcode,
                            msample_idx,
                        ),
                        make_mplex_flowcell(
                            "rebasecalled_multiplexed_experiment",
                            expt,
                            300,
                            slot,
                            tag_id,
                            barcode,
                            msample_idx,
                        ),
                    ]
                )
                msample_idx += 1

    session.add_all(flowcells)  # Simple and multiplexed
    session.commit()


@pytest.fixture(scope="function")
def ont_synthetic_mlwh(mlwh_session) -> Session:
    """An ML warehouse database fixture populated with ONT-related records."""
    initialize_mlwh_ont_synthetic(mlwh_session)
    yield mlwh_session


@pytest.fixture(scope="function")
def ont_gridion_irods(tmp_path):
    """A fixture providing a set of files based on output from an ONT GridION
    instrument. This dataset provides an example of file and directory naming
    conventions. The file contents are dummy values."""
    root_path = PurePath("/testZone/home/irods/test/ont_gridion_irods")
    rods_path = add_rods_path(root_path, tmp_path)

    iput("./tests/data/ont/gridion", rods_path, recurse=True)
    expt_root = rods_path / "gridion"
    add_test_groups()

    try:
        yield expt_root
    finally:
        remove_rods_path(rods_path)
        remove_test_groups()


@pytest.fixture(scope="function")
def ont_synthetic_irods(tmp_path):
    """A fixture providing a synthetic set of files and metadata based on output
    from an ONT GridION instrument, modified to represent the way simple and
    multiplexed experiments are laid out. The file contents are dummy values."""
    root_path = PurePath("/testZone/home/irods/test/ont_synthetic_irods")
    rods_path = add_rods_path(root_path, tmp_path)

    expt_root = rods_path / "synthetic"
    add_test_groups()

    for expt in range(1, NUM_SIMPLE_EXPTS + 1):
        for slot in range(1, NUM_INSTRUMENT_SLOTS + 1):
            expt_name = f"simple_experiment_{expt :0>3}"
            id_flowcell = f"flowcell{slot + 10 :0>3}"
            run_folder = f"20190904_1514_G{slot}00000_{id_flowcell}_69126024"

            coll = Collection(expt_root / expt_name / run_folder)
            coll.create(parents=True)
            meta = [
                AVU(ont.Instrument.EXPERIMENT_NAME, expt_name),
                AVU(ont.Instrument.INSTRUMENT_SLOT, f"{slot}"),
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
                AVU(ont.Instrument.EXPERIMENT_NAME, expt_name),
                AVU(ont.Instrument.INSTRUMENT_SLOT, f"{slot}"),
            ]
            coll.add_metadata(*meta)

    for expt in range(1, NUM_MULTIPLEXED_EXPTS + 1):
        for slot in range(1, NUM_INSTRUMENT_SLOTS + 1):
            expt_name = f"old_rebasecalled_multiplexed_experiment_{expt :0>3}"
            id_flowcell = f"flowcell{slot + 200 :0>3}"
            run_folder = f"20190904_1514_GA{slot}0000_{id_flowcell}_cf751ba1"

            coll = Collection(expt_root / expt_name / run_folder)
            coll.create(parents=True)
            meta = [
                AVU(ont.Instrument.EXPERIMENT_NAME, expt_name),
                AVU(ont.Instrument.INSTRUMENT_SLOT, f"{slot}"),
            ]
            coll.add_metadata(*meta)

    for expt in range(1, NUM_MULTIPLEXED_EXPTS + 1):
        for slot in range(1, NUM_INSTRUMENT_SLOTS + 1):
            expt_name = f"rebasecalled_multiplexed_experiment_{expt :0>3}"
            id_flowcell = f"flowcell{slot + 300 :0>3}"
            run_folder = f"20190904_1514_GA{slot}0000_{id_flowcell}_cf751ba1"

            coll = Collection(expt_root / expt_name / run_folder)
            coll.create(parents=True)
            meta = [
                AVU(ont.Instrument.EXPERIMENT_NAME, expt_name),
                AVU(ont.Instrument.INSTRUMENT_SLOT, f"{slot}"),
            ]
            coll.add_metadata(*meta)

    # We have synthetic data only for simple_experiment_001 and
    # multiplexed_experiment_001
    iput("./tests/data/ont/synthetic", rods_path, recurse=True)

    try:
        yield expt_root
    finally:
        remove_rods_path(rods_path)
        remove_test_groups()
