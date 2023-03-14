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

"""Support for LIMS platform metadata added to iRODS by NPG."""

import re
from enum import unique
from itertools import starmap
from typing import List

from ml_warehouse.schema import Sample, Study
from partisan.irods import AC, AVU, DataObject, Permission
from partisan.metadata import AsValueEnum
from structlog import get_logger

from npg_irods.metadata.common import _ensure_avus_present, avu_if_value

STUDY_IDENTIFIER_GROUP = "study_id"
STUDY_IDENTIFIER_REGEX = re.compile(r"^ss_(?P<study_id>\d+)$")

log = get_logger(__name__)


@unique
class TrackedSample(AsValueEnum):
    """SequenceScape Sample metadata."""

    ACCESSION_NUMBER = "sample_accession_number"
    COHORT = "sample_cohort"
    COMMON_NAME = "sample_common_name"
    CONSENT = "sample_consent"
    CONSENT_WITHDRAWN = "sample_consent_withdrawn"
    CONTROL = "sample_control"
    DONOR_ID = "sample_donor_id"
    ID = "sample_id"
    NAME = "sample"
    PUBLIC_NAME = "sample_public_name"
    SUPPLIER_NAME = "sample_supplier_name"


@unique
class TrackedStudy(AsValueEnum):
    """SequenceScape Study metadata."""

    ACCESSION_NUMBER = "study_accession_number"
    ID = "study_id"
    NAME = "study"
    TITLE = "study_title"


@unique
class SeqConcept(AsValueEnum):
    """Sequencing terminology."""

    TAG_INDEX = "tag_index"
    REFERENCE = "reference"
    COMPONENT = "component"
    ID_PRODUCT = "id_product"
    ALT_PROCESS = "alt_process"


def make_sample_metadata(sample: Sample) -> List[AVU]:
    """Return standard iRODS metadata for a Sample:

     - sample ID
     - sample name
     - sample accession
     - sample donor ID
     - sample supplier name
     - sample consent withdrawn

    Args:
        sample: An ML warehouse schema Sample.

    Returns: List[AVU]
    """
    av = [
        [TrackedSample.ID, sample.sanger_sample_id],
        [TrackedSample.NAME, sample.name],
        [TrackedSample.ACCESSION_NUMBER, sample.accession_number],
        [TrackedSample.DONOR_ID, sample.donor_id],
        [TrackedSample.SUPPLIER_NAME, sample.supplier_name],
        [
            TrackedSample.CONSENT_WITHDRAWN,
            1 if sample.consent_withdrawn else None,
        ],
    ]

    return list(filter(lambda avu: avu is not None, starmap(avu_if_value, av)))


def make_study_metadata(study: Study):
    av = [
        [TrackedStudy.ID, study.id_study_lims],
        [TrackedStudy.NAME, study.name],
        [TrackedStudy.ACCESSION_NUMBER, study.accession_number],
    ]

    return list(filter(lambda avu: avu is not None, starmap(avu_if_value, av)))


def make_sample_acl(sample: Sample, study: Study) -> List[AC]:
    irods_group = f"ss_{study.id_study_lims}"
    perm = Permission.NULL if sample.consent_withdrawn else Permission.READ

    return [AC(irods_group, perm)]


def has_consent_withdrawn_metadata(obj: DataObject) -> bool:
    """Return True if the data object is annotated in iRODS as having donor consent
    withdrawn.

    This is defined as having either of these AVUs:

        - sample_consent = 0 (data managed by the GAPI codebase)
        - sample_consent_withdrawn = 1 (data managed by the NPG codebase)

    The GAPI codebase has additional behaviour where consent is denoted by the AVU
    sample_consent = 1 and a data object missing the sample_consent AVU is considered
    as not consented. The AVU should be present, but given that iRODS does not guarantee
    AVU integrity, this behaviour means that if the AVU is missing, the data
    "fails closed" (unreadable), rather than "fails open" (readable).

    Args:
        obj: The data object to check.

    Returns:
        True if consent was withdrawn.
    """
    meta = obj.metadata()

    return (
        AVU(TrackedSample.CONSENT, 0) in meta
        or AVU(TrackedSample.CONSENT_WITHDRAWN, 1) in meta
    )


def ensure_consent_withdrawn_metadata(obj: DataObject) -> bool:
    """Ensure that consent withdrawn metadata are on the data object.

    Args:
        obj: The data object to check.

    Returns:
        True if metadata were added.
    """
    return _ensure_avus_present(obj, AVU(TrackedSample.CONSENT_WITHDRAWN, 1))


def has_consent_withdrawn_permissions(obj: DataObject) -> bool:
    """Return True if the object has permissions expected for data with consent
    withdrawn.

    Args:
        obj: The data object to check.

    Returns:
        True if the permissions were as expected.
    """
    # Alternatively, we could keep a list of rodsadmin users who should have continued
    # access e.g. in order to redact the data, and check that no other users are in the
    # ACL. Using a list of rodsadmins would mean we don't need to use regex.
    study_acl = [
        ac for ac in obj.permissions() if STUDY_IDENTIFIER_REGEX.match(ac.user)
    ]

    return not study_acl


def has_consent_withdrawn(obj: DataObject) -> bool:
    """Return True if the data object has metadata and permissions for data with consent
    withdrawn.

    Args:
        obj: The data object to check.

    Returns:
        True if the metadata and permissions were as expected.
    """
    return has_consent_withdrawn_metadata(obj) and has_consent_withdrawn_permissions(
        obj
    )


def ensure_consent_withdrawn(obj: DataObject) -> bool:
    """Ensure that a data object has its metadata and permissions in the correct state
    for having consent withdrawn.

    Args:
        obj: The data object to check.

    Returns:
        True if the metadata and/or permissions were updated.
    """
    if has_consent_withdrawn(obj):
        return False

    if ensure_consent_withdrawn_metadata(obj):
        log.info(
            "Updated metadata",
            path=obj,
            has_withdrawn_meta=has_consent_withdrawn_metadata(obj),
        )

    null_perms = [
        AC(ac.user, Permission.NULL)
        for ac in obj.permissions()
        if STUDY_IDENTIFIER_REGEX.match(ac.user)
    ]

    num_removed, num_added = obj.supersede_permissions(*null_perms)
    log.info(
        "Updated permissions",
        path=obj,
        num_removed=num_removed,
        num_added=num_added,
        has_withdrawn_perm=has_consent_withdrawn_permissions(obj),
    )
    return True
