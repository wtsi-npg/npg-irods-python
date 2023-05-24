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

"""API common to all analysis instruments and processes."""

import json
import re
from collections import defaultdict
from enum import Enum, unique

from partisan.irods import AVU, RodsItem

from npg_irods.metadata import illumina
from npg_irods.metadata.lims import SeqConcept


@unique
class Organisation(Enum):
    BIONANO = 1
    FLUIDIGM = 2
    GENOMICS_10x = 3
    ILLUMINA = 4
    OXFORD_NANOPORE_TECHNOLOGIES = 5
    PACBIO = 6
    SEQUENOM = 7
    ULTIMA_GENOMICS = 8


@unique
class AnalysisType(Enum):
    GENE_EXPRESSION = 1
    GENOTYPING = 2
    NUCLEIC_ACID_SEQUENCING = 3
    OPTICAL_MAPPING = 4


class Component:
    @classmethod
    def from_avu(cls, avu: AVU):
        if avu.attribute != SeqConcept.COMPONENT.value:
            raise ValueError(
                f"Cannot create a Component from metadata {avu}; "
                f"invalid attribute {avu.attribute}"
            )
        try:
            compval = json.loads(avu.value)

            return Component(
                compval[illumina.Instrument.RUN.value],
                compval[illumina.Instrument.LANE.value],
                subset=compval.get(SeqConcept.SUBSET.value, None),
                tag_index=compval.get(SeqConcept.TAG_INDEX.value, None),
            )
        except Exception as e:
            raise ValueError(
                f"Failed to create a Component from metadata {avu}; {e}",
            ) from e

    def __init__(
        self, run_id: int, position: int, subset: str = None, tag_index: int = None
    ):
        self.run_id = run_id
        self.position = position
        self.subset = subset
        self.tag_index = tag_index

    def __hash__(self):
        return (
            hash(self.run_id)
            + hash(self.position)
            + hash(self.tag_index)
            + hash(self.subset)
        )

    def __eq__(self, other):
        if not isinstance(other, Component):
            return False

        return (
            self.run_id == other.run_id
            and self.position == other.position
            and (
                (self.tag_index is None and other.tag_index is None)
                or (
                    self.tag_index is not None
                    and other.tag_index is not None
                    and self.tag_index == other.tag_index
                )
            )
            and (
                (self.subset is None and other.subset is None)
                or (
                    self.subset is not None
                    and other.subset is not None
                    and self.subset == other.subset
                )
            )
        )

    def __repr__(self):
        c = {
            illumina.Instrument.RUN.value: self.run_id,
            illumina.Instrument.LANE.value: self.position,
        }
        if self.tag_index is not None:
            c[SeqConcept.TAG_INDEX.value] = self.tag_index
        if self.subset is not None:
            c[SeqConcept.SUBSET.value] = self.subset

        return json.dumps(c)


# There are further tests we can do, aside from inspecting the path, such as looking
# at metadata or neighbouring data objects, but this will suffice to start with.
# Having the test wrapped in a function means it can be changed in one place.


def is_illumina(path: RodsItem) -> bool:
    illumina_legacy_patt = r"/seq/\d+\b"
    illumina_patt = r"/seq/illumina/runs/\d+\b"

    p = str(path)
    return (
        re.match(illumina_legacy_patt, p) is not None
        or re.match(illumina_patt, p) is not None
    )


def is_bionano(path: RodsItem) -> bool:
    return re.match(r"/seq/bionano\b", str(path)) is not None


def is_fluidigm(path: RodsItem) -> bool:
    return re.match(r"/seq/fluidigm\b", str(path)) is not None


def is_10x(path: RodsItem) -> bool:
    return re.match(r"/seq/illumina/(cell|long|space)ranger", str(path)) is not None


def is_oxford_nanopore(path: RodsItem) -> bool:
    return re.match(r"/seq/ont\b", str(path)) is not None


def is_pacbio(path: RodsItem) -> bool:
    return re.match(r"/seq/pacbio\b", str(path)) is not None


def is_sequenom(path: RodsItem) -> bool:
    return re.match(r"/seq/sequenom\b", str(path)) is not None


def is_ultima_genomics(path: RodsItem) -> bool:
    return re.match(r"/seq/ug\b", str(path)) is not None


def infer_data_source(path: RodsItem):
    if is_bionano(path):
        return Organisation.BIONANO, AnalysisType.OPTICAL_MAPPING
    if is_fluidigm(path):
        return Organisation.FLUIDIGM, AnalysisType.GENOTYPING
    if is_10x(path):
        return Organisation.GENOMICS_10x, AnalysisType.GENE_EXPRESSION
    if is_illumina(path):
        return Organisation.ILLUMINA, AnalysisType.NUCLEIC_ACID_SEQUENCING
    if is_oxford_nanopore(path):
        return (
            Organisation.OXFORD_NANOPORE_TECHNOLOGIES,
            AnalysisType.NUCLEIC_ACID_SEQUENCING,
        )
    if is_pacbio(path):
        return Organisation.PACBIO, AnalysisType.NUCLEIC_ACID_SEQUENCING
    if is_sequenom(path):
        return Organisation.SEQUENOM, AnalysisType.GENOTYPING
    if is_ultima_genomics(path):
        return Organisation.ULTIMA_GENOMICS, AnalysisType.NUCLEIC_ACID_SEQUENCING

    raise ValueError(f"Failed to infer a data source for iRODS path '{path}'")
