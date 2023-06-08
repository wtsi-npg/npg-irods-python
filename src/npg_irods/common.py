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
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, unique
from typing import Any

from partisan.irods import AC, AVU, Permission, RodsItem

from npg_irods.db.mlwh import Sample, Study
from npg_irods.metadata import illumina
from npg_irods.metadata.common import SeqConcept, SeqSubset
from npg_irods.metadata.lims import make_sample_acl


@unique
class Platform(Enum):
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
        try:
            if avu.attribute != SeqConcept.COMPONENT.value:
                raise ValueError(
                    f"Cannot create a Component from metadata {avu}; "
                    f"invalid attribute {avu.attribute}"
                )

            avu_value = json.loads(avu.value)

            if illumina.Instrument.RUN.value not in avu_value:
                raise ValueError(
                    f"Cannot create a Component from metadata {avu}; "
                    "only Illumina metadata is supported in iRODS AVUs"
                )

            subset_name = avu_value.get(SeqConcept.SUBSET.value, None)
            match subset_name:
                case "human":
                    subset = SeqSubset.HUMAN
                case "xahuman":
                    subset = SeqSubset.XAHUMAN
                case "yhuman":
                    subset = SeqSubset.YHUMAN
                case "phix":
                    subset = SeqSubset.PHIX
                case None:
                    subset = None
                case _:
                    raise ValueError(
                        f"Cannot create a Component from metadata {avu}; "
                        f"invalid subset '{subset_name}'"
                    )

            return Component(
                avu_value[illumina.Instrument.RUN.value],
                avu_value[illumina.Instrument.LANE.value],
                subset=subset,
                tag_index=avu_value.get(SeqConcept.TAG_INDEX.value, None),
                platform=Platform.ILLUMINA,
            )
        except Exception as e:
            raise ValueError(
                f"Failed to create a Component from metadata {avu}; {e}",
            ) from e

    def __init__(
        self,
        suid: Any,
        position: int,
        subset: str = None,
        tag_index: int = None,
        platform: Platform = None,
    ):
        self.suid = suid
        self.position = position
        self.subset = subset
        self.tag_index = tag_index
        self.platform = platform

    def __hash__(self):
        return (
            hash(self.suid)
            + hash(self.position)
            + hash(self.tag_index)
            + hash(self.subset)
        )

    def __eq__(self, other):
        if not isinstance(other, Component):
            return False

        return (
            self.suid == other.suid
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
        match self.platform:
            case Platform.ILLUMINA:
                c = {
                    illumina.Instrument.RUN.value: self.suid,
                    illumina.Instrument.LANE.value: self.position,
                }
            case _:
                c = {"suid": self.suid, "position": self.position}

        if self.tag_index is not None:
            c[SeqConcept.TAG_INDEX.value] = self.tag_index
        if self.subset is not None:
            c[SeqConcept.SUBSET.value] = self.subset

        return json.dumps(c)


# Access could be decided for a set of samples, set of studies, each sample's data
# having its own subset information
#
# Components can source all the above info, but they need to do the MLWH queries
# Making secondary metadata need to do the same MLWH queries


@dataclass
class SeqUnit:
    sample: Sample
    study: Study
    component: Component


class AccessPolicy(ABC):
    @abstractmethod
    def acl(self):
        pass


class IlluminaAccessPolicy(AccessPolicy):
    def acl(self, *seq_units: SeqUnit) -> list[AC]:
        def revoke(ac: AC):
            ac.perm = Permission.NULL

        non_consented_human = False

        acl = []
        for su in seq_units:
            sample_acl = make_sample_acl(su.sample, su.study)
            match su.component.subset:
                case SeqSubset.HUMAN | SeqSubset.XAHUMAN:
                    non_consented_human = True
                case SeqSubset.YHUMAN:
                    pass
                case SeqSubset.PHIX:
                    pass
                case _:
                    raise ValueError(f"Invalid subset in component of {su}")
            acl.extend(sample_acl)

        if non_consented_human or any([ac.perm == Permission.NULL for ac in acl]):
            map(revoke, acl)

        return acl


class ONTAccessPolicy(AccessPolicy):
    def acl(self, *seq_units: SeqUnit) -> list[AC]:
        studies = {su.study for su in seq_units}
        if len(studies) > 1:
            raise ValueError(
                "Invalid sequence units; more than one study is "
                f"represented: {studies}"
            )

        acl = []
        for su in seq_units:
            acl.extend(make_sample_acl(su.sample, su.study))

        return acl


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
        return Platform.BIONANO, AnalysisType.OPTICAL_MAPPING
    if is_fluidigm(path):
        return Platform.FLUIDIGM, AnalysisType.GENOTYPING
    if is_10x(path):
        return Platform.GENOMICS_10x, AnalysisType.GENE_EXPRESSION
    if is_illumina(path):
        return Platform.ILLUMINA, AnalysisType.NUCLEIC_ACID_SEQUENCING
    if is_oxford_nanopore(path):
        return (
            Platform.OXFORD_NANOPORE_TECHNOLOGIES,
            AnalysisType.NUCLEIC_ACID_SEQUENCING,
        )
    if is_pacbio(path):
        return Platform.PACBIO, AnalysisType.NUCLEIC_ACID_SEQUENCING
    if is_sequenom(path):
        return Platform.SEQUENOM, AnalysisType.GENOTYPING
    if is_ultima_genomics(path):
        return Platform.ULTIMA_GENOMICS, AnalysisType.NUCLEIC_ACID_SEQUENCING

    raise ValueError(f"Failed to infer a data source for iRODS path '{path}'")
