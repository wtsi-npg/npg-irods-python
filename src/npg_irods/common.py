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

import re
from enum import Enum, unique
from os import PathLike
from typing import Tuple

from structlog import get_logger

log = get_logger(__package__)


@unique
class Platform(Enum):
    """An analysis platform (instrument or analysis technology), named by manufacturing
    company."""

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
    """A crude classification of bioinformatic analysis types."""

    GENE_EXPRESSION = 1
    GENOTYPING = 2
    NUCLEIC_ACID_SEQUENCING = 3
    OPTICAL_MAPPING = 4


# There are further tests we can do, aside from inspecting the path, such as looking
# at metadata or neighbouring data objects, but this will suffice to start with.
# Having the test wrapped in a function means it can be changed in one place.


def is_illumina(path: PathLike | str) -> bool:
    """Test whether the argument should be data derived from an Illumina instrument.

    Args:
        path: An iRODS path.

    Returns:
        True if Illumina data.
    """
    illumina_legacy_patt = r"/seq/\d+\b"
    illumina_patt = r"/seq/illumina/runs/\d+\b"
    p = str(path)
    return (
        re.match(illumina_legacy_patt, p) is not None
        or re.match(illumina_patt, p) is not None
    )


def is_bionano(path: PathLike | str) -> bool:
    """Test whether the argument should be data derived from a BioNano instrument.

    Args:
        path: An iRODS path.

    Returns:
        True if BioNano data.
    """
    return re.match(r"/seq/bionano\b", str(path)) is not None


def is_fluidigm(path: PathLike | str) -> bool:
    """Test whether the argument should be data derived from a Fluidigm instrument.

    Args:
        path: An iRODS path.

    Returns:
        True if Fluidigm data.
    """
    return re.match(r"/seq/fluidigm\b", str(path)) is not None


def is_10x(path: PathLike | str) -> bool:
    """Test whether the argument should be data derived from a 10x Genomics analysis.

    Args:
        path: An iRODS path.

    Returns:
        True if 10x data.
    """
    return re.match(r"/seq/illumina/(cell|long|space)ranger", str(path)) is not None


def is_oxford_nanopore(path: PathLike | str) -> bool:
    """Test whether the argument should be data derived from an Oxford Nanopore
    Technologies instrument.

    Args:
        path: An iRODS path.

    Returns:
        True if ONT data.
    """
    return re.match(r"/seq/ont\b", str(path)) is not None


def is_pacbio(path: PathLike | str) -> bool:
    """Test whether the argument should be data derived from a PacBio instrument.

    Args:
        path: An iRODS path.

    Returns:
        True if PacBio data.
    """
    return re.match(r"/seq/pacbio\b", str(path)) is not None


def is_sequenom(path: PathLike | str) -> bool:
    """Test whether the argument should be data derived from a Sequenom (Agena)
    instrument.

    Args:
        path: An iRODS path.

    Returns:
        True if Sequenom data.
    """
    return re.match(r"/seq/sequenom\b", str(path)) is not None


def is_ultima_genomics(path: PathLike | str) -> bool:
    """Test whether the argument should be data derived from an Ultima Genomics
    instrument.

    Args:
        path: An iRODS path.

    Returns:
        True if Ultime data.
    """
    return re.match(r"/seq/ug\b", str(path)) is not None


def infer_data_source(path: PathLike | str) -> Tuple[Platform, AnalysisType]:
    """Infer the analysis platform and analysis type of data, given its iRODS path.

    Args:
        path: An iRODS path.

    Returns:
        A tuple of platform and analysis type.
    """
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
