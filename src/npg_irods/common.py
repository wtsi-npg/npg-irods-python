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

"""API common to all analysis instruments and processes."""

import re
from enum import Enum, unique
from os import PathLike
from pathlib import PurePath
from typing import Tuple

from partisan.irods import AC, AVU, Collection, DataObject, Permission, rods_user
from sqlalchemy.orm import Session
from structlog import get_logger

from npg_irods.db.mlwh import find_sample_by_sample_id, find_study_by_study_id
from npg_irods.metadata.lims import (
    TrackedSample,
    TrackedStudy,
    ensure_consent_withdrawn,
    has_consent_withdrawn_metadata,
    has_mixed_ownership,
    is_managed_access,
    is_public_access,
    make_sample_acl,
    make_sample_metadata,
    make_study_acl,
    make_study_metadata,
)

log = get_logger(__package__)


@unique
class Platform(Enum):
    """An analysis platform (instrument or analysis technology), named by manufacturing
    company."""

    OTHER = 0
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

    OTHER = 0
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


def is_infinium(path: PathLike | str) -> bool:
    """Test whether the argument should be data derived from an Infinium instrument.

    Args:
        path: An iRODS path.

    Returns:
        True if Infinium data.
    """
    return re.match(r"/infinium\b", str(path)) is not None


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
        True if Ultima data.
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
    if is_infinium(path):
        return Platform.ILLUMINA, AnalysisType.GENOTYPING
    if is_sequenom(path):
        return Platform.SEQUENOM, AnalysisType.GENOTYPING
    if is_ultima_genomics(path):
        return Platform.ULTIMA_GENOMICS, AnalysisType.NUCLEIC_ACID_SEQUENCING

    return Platform.OTHER, AnalysisType.OTHER


def update_metadata(item: Collection | DataObject, avus: list[AVU]) -> bool:
    """Update metadata on an iRODS path, removing existing metadata and replacing with
    the given AVUs and adding history of changes.

    Args:
        item: iRODS path to update.
        avus: Metadata to apply.

    Returns:
        True if any changes were made, False if the desired metadata were already
        present.
    """
    avus = sorted(set(avus))  # Ensure no duplicates, sort for reproducibility

    log.info("Updating metadata", path=item, meta=avus)
    num_removed, num_added = item.supersede_metadata(*avus, history=True)
    log.info(
        "Updated metadata",
        path=item,
        num_added=num_added,
        num_removed=num_removed,
    )
    return num_removed > 0 or num_added > 0


def update_permissions(
    item: Collection | DataObject, acl: list[AC], recurse=False
) -> bool:
    """Update permissions on an iRODS path, removing existing permissions and replacing
    with the given ACL. If the ACL contains multiple, conflicting, managed permissions
    then it will issue a warning and revoke access.

    Args:
        item: iRODS path to update.
        acl: ACL to apply.
        recurse: If True, recursively apply the ACL.

    Returns:
        True if changes any changes were made, False if the ACL(s) of the target path(s)
    were already in the desired state. This applies recursively, so to return False for
    a recursive operation, neither the target path, nor any contained path will have
    been updated.
    """
    if item.rods_type == DataObject and recurse:
        raise ValueError(
            f"Cannot recursively update permissions on a data object: {item}"
        )

    acl = sorted(set(acl))  # Ensure no duplicates, sort for reproducibility

    if has_mixed_ownership(acl):
        log.warn("Mixed-study data", path=item, acl=acl)
        for ac in acl:
            if is_managed_access(ac):
                ac.perm = Permission.NULL

    # Gather some of the current permissions that we want to keep, while we supersede
    # all the rest with our new ACL:

    # Don't try to remove current admin permissions (this would fail)
    admin_acl = item.permissions(user_type="rodsadmin")

    # Don't remove current group permissions, other than those we manage
    not_managed_acl = [
        ac
        for ac in item.permissions(user_type="rodsgroup")
        if not is_managed_access(ac) or is_public_access(ac)
    ]

    # Don't remove current permissions of ourselves (service user)
    user = rods_user()
    user_acl = [ac for ac in item.permissions() if ac.user == user.name]

    preserve_set = set(admin_acl + not_managed_acl + user_acl)

    # But do remove public access if the object has an ACL with managed access
    if any(is_managed_access(ac) for ac in acl):
        public_acl = [ac for ac in item.permissions() if is_public_access(ac)]
        log.info("Removing public access", path=item, acl=public_acl)
        preserve_set.difference_update(public_acl)

    preserve = sorted(preserve_set)  # Ensure no duplicates, sort for reproducibility
    log.debug(
        "Found permissions to preserve",
        path=item,
        user=user_acl,
        admin=admin_acl,
        not_managed=not_managed_acl,
    )

    log.info("Updating permissions", path=item, preserve=preserve, acl=acl)
    kwargs = {"recurse": recurse} if recurse else {}
    num_removed, num_added = item.supersede_permissions(*preserve, *acl, **kwargs)
    log.info(
        "Updated permissions",
        path=item,
        num_added=num_added,
        num_removed=num_removed,
    )
    return num_removed > 0 or num_added > 0


def infer_zone(path: Collection | DataObject) -> str:
    """Infer the iRODS zone from an iRODS path.

    Args:
        path: An absolute iRODS path.

    Returns:
        The zone.
    """
    parts = PurePath(path).parts
    if len(parts) < 2:
        raise ValueError(f"Invalid iRODS path {path}; no zone component")
    return parts[1]


def ensure_secondary_metadata_updated(
    item: Collection | DataObject, mlwh_session: Session
) -> bool:
    """Updates secondary metadata for a iRODS path using data from MLWH.

    Args:
        item: A Collection or DataObject.
        mlwh_session: An open SQL session.
    Returns:
       True if updated.
    """
    secondary_metadata, acl = [], []
    sample, study = None, None

    if item.metadata(TrackedStudy.ID):
        study_id = item.avu(TrackedStudy.ID).value
        study = find_study_by_study_id(mlwh_session, study_id)

    if item.metadata(TrackedSample.ID):
        sample_id = item.avu(TrackedSample.ID).value
        sample = find_sample_by_sample_id(mlwh_session, sample_id)

    if study is None and sample is None:
        raise ValueError(
            f"Failed to update metadata on {item}. No {TrackedStudy.ID} or "
            f"{TrackedSample.ID} or metadata are present"
        )

    if sample is None:
        secondary_metadata.extend(make_study_metadata(study))
        acl.extend(make_study_acl(study))
    elif study is None:
        secondary_metadata.extend(make_sample_metadata(sample))
        log.warn(
            f"Item has no {TrackedStudy.ID} metadata to allow permissions to be set. "
            "It will be set as unreadable by default.",
            path=item,
        )
    else:
        secondary_metadata.extend(make_study_metadata(study))
        secondary_metadata.extend(make_sample_metadata(sample))
        acl.extend(make_sample_acl(sample, study))

    secondary_metadata = sorted(set(secondary_metadata))
    acl = sorted(set(acl))

    meta_update = update_metadata(item, secondary_metadata)

    cons_update = perm_update = False
    if has_consent_withdrawn_metadata(item):
        log.info("Consent withdrawn", path=item)
        cons_update = ensure_consent_withdrawn(item)
    else:
        perm_update = update_permissions(item, acl)

    return any([meta_update, cons_update, perm_update])
