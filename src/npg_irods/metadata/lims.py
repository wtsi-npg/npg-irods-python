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

from partisan.irods import (
    AC,
    AVU,
    Collection,
    DataObject,
    Permission,
    current_user,
    rods_user,
)
from partisan.metadata import AsValueEnum
from structlog import get_logger

from npg_irods.db.mlwh import Sample, Study
from npg_irods.metadata.common import (
    SeqConcept,
    SeqSubset,
    ensure_avus_present,
    avu_if_value,
)

STUDY_IDENTIFIER_PREFIX = "ss_"
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


def make_sample_metadata(sample: Sample) -> list[AVU]:
    """Return standard iRODS metadata for a Sample:

     - sample accession
     - sample common name
     - sample consent withdrawn
     - sample donor ID
     - sample ID
     - sample name
     - sample public name
     - sample supplier name

    Args:
        sample: An ML warehouse schema Sample.

    Returns:
        AVUs
    """
    av = [
        [TrackedSample.ACCESSION_NUMBER, sample.accession_number],
        [TrackedSample.COMMON_NAME, sample.common_name],
        [
            TrackedSample.CONSENT_WITHDRAWN,
            1 if sample.consent_withdrawn else None,
        ],
        [TrackedSample.DONOR_ID, sample.donor_id],
        [TrackedSample.ID, sample.id_sample_lims],
        [TrackedSample.NAME, sample.name],
        [TrackedSample.PUBLIC_NAME, sample.public_name],
        [TrackedSample.SUPPLIER_NAME, sample.supplier_name],
    ]

    return list(filter(lambda avu: avu is not None, starmap(avu_if_value, av)))


def make_reduced_sample_metadata(sample: Sample) -> list[AVU]:
    """Return reduced iRODS metadata for a Sample:

     - sample accession
     - sample ID
     - sample name

    Args:
        sample: An ML warehouse schema Sample.

    Returns:
        AVUs
    """
    if sample.consent_withdrawn:
        return [avu_if_value(TrackedSample.CONSENT_WITHDRAWN, 1)]

    return []


def make_study_metadata(study: Study) -> list[AVU]:
    """Return standard iRODS metadata for a Study:

    - study ID
    - study name
    - study accession
    - study title.

    Args:
        study: An ML warehouse schema Study.

    Returns:
        AVUs
    """
    av = [
        [TrackedStudy.ACCESSION_NUMBER, study.accession_number],
        [TrackedStudy.ID, study.id_study_lims],
        [TrackedStudy.NAME, study.name],
        [TrackedStudy.TITLE, study.study_title],
    ]

    return list(filter(lambda avu: avu is not None, starmap(avu_if_value, av)))


def make_reduced_study_metadata(study: Study) -> list[AVU]:
    """Return reduced iRODS metadata for a Study:

    - study ID
    - study name

    Args:
        study: An ML warehouse schema Study.

    Returns:
        AVUs
    """
    return [avu_if_value(TrackedStudy.ID, study.id_study_lims)]


def make_sample_acl(
    sample: Sample, study: Study, subset: SeqSubset = None, zone=None
) -> list[AC]:
    """Returns an ACL for a given Sample in a Study.

    This method takes into account all factors influencing access control, which are:

    From the Sample:

        - The statue of the per-sample consent withdrawn flag.

    From the subset:

        - The NPG business logic for how each subset of reads should be treated.

    Note that this function does not check that the sample is in the study.

    Args:
        subset: Subset of sequence reads
        sample: A sample, which will be used to confirm consent, which modifies the
                ACL.
        study: A study, which will provide permissions for the ACL.
        zone: The iRODS zone.

    Returns:
        An ACL
    """
    if subset is not None and subset is subset.XAHUMAN:
        return []

    if subset is not None and subset is subset.HUMAN:
        irods_group = f"{STUDY_IDENTIFIER_PREFIX}{study.id_study_lims}_human"
    else:
        irods_group = f"{STUDY_IDENTIFIER_PREFIX}{study.id_study_lims}"
    perm = Permission.NULL if sample.consent_withdrawn else Permission.READ

    return [AC(irods_group, perm, zone=zone)]


def make_public_read_acl() -> list[AC]:
    """Returns an ACL allowing public reads.

    Returns:
        An ACL
    """
    return [AC("public", Permission.READ)]


def has_consent_withdrawn_metadata(
    item: Collection | DataObject, recurse=False
) -> bool:
    """Return True if the collection or data object is annotated in iRODS as having
    donor consent withdrawn.

    This is defined as having either of these AVUs:

        - sample_consent = 0 (data managed by the GAPI codebase)
        - sample_consent_withdrawn = 1 (data managed by the NPG codebase)

    The GAPI codebase has additional behaviour where consent is denoted by the AVU
    sample_consent = 1 and a collection or data object missing the sample_consent AVU is
    considered as not consented. The AVU should be present, but given that iRODS does
    not guarantee AVU integrity, this behaviour means that if the AVU is missing,
    the collection or data object "fails closed" (unreadable), rather than "fails open"
    (readable).

    A collection having consent withdrawn metadata may imply that its contents
    should have permissions removed recursively. This can be checked with the recurse keyword.

    Args:
        item: The collection or data object to check.
        recurse: Include a check of collection contents recursively. Defaults to False.
            If True and item is a DataObject, raises a ValueError.
    Returns:
        True if consent was withdrawn.
    """
    if item.rods_type == DataObject and recurse:
        raise ValueError(f"Cannot recursively check metadata on a data object: {item}")

    gapi_withdrawn = AVU(TrackedSample.CONSENT, 0)
    npg_withdrawn = AVU(TrackedSample.CONSENT_WITHDRAWN, 1)

    def is_withdrawn(x):
        meta = x.metadata()
        return gapi_withdrawn in meta or npg_withdrawn in meta

    withdrawn_root = is_withdrawn(item)
    withdrawn_child = True

    if item.rods_type == Collection and recurse:
        for it in item.iter_contents(recurse=True):
            if not is_withdrawn(it):
                withdrawn_child = False
                break

    return withdrawn_root and withdrawn_child


def ensure_consent_withdrawn_metadata(
    item: Collection | DataObject, recurse=False
) -> bool:
    """Ensure that consent withdrawn metadata are on the collection or data object.

    Args:
        item: The collection or data object to update.
        recurse: Apply to collection contents recursively. Defaults to False. If True
            and item is a DataObject, raises a ValueError.

    Returns:
        True if metadata were added.
    """
    if item.rods_type == DataObject and recurse:
        raise ValueError(f"Cannot recursively update metadata on a data object: {item}")

    withdrawn_avu = AVU(TrackedSample.CONSENT_WITHDRAWN, 1)
    updated_root = ensure_avus_present(item, withdrawn_avu)
    updated_child = False

    if item.rods_type == Collection and recurse:
        for it in item.iter_contents(recurse=True):
            if ensure_avus_present(it, withdrawn_avu):
                updated_child = True

    return updated_root or updated_child


def is_managed_access(ac: AC):
    """Return True if the access control is managed by this API and can safely be added
    or removed.

    Args:
        ac: The access control to test.

    Returns:
        True if managed by this API.
    """
    return STUDY_IDENTIFIER_REGEX.match(ac.user)


def has_mixed_ownership(acl: list[AC]):
    """Return True if the ACL has managed access controls for more than one iRODS user
    or iRODS group. An example of this is where data belong to more than one study. As
    access controls are managed per study, this indicates possibly conflicting iRODS
    permissions (it isn't possible to open the data to the owners of one study
    while simultaneously denying access to the owners of the other).

    Args:
        acl: An access control list.

    Returns:
        True if mixed ownership.
    """
    return len({ac.user for ac in acl if is_managed_access(ac)}) > 1


def has_consent_withdrawn_permissions(
    item: Collection | DataObject, recurse=False
) -> bool:
    """Return True if the collection or data object has permissions expected for data
    with consent withdrawn.

    Args:
        item: The collection or data object to check.
        recurse: Include a check of collection contents recursively. Defaults to False.
            If True and item is a DataObject, raises a ValueError.
    Returns:
        True if the permissions were as expected.
    """
    if item.rods_type == DataObject and recurse:
        raise ValueError(
            f"Cannot recursively check permissions on a data object: {item}"
        )

    # Alternatively, we could keep a list of rodsadmin users who should have continued
    # access e.g. in order to redact the data, and check that no other users are in the
    # ACL. Using a list of rodsadmins would mean we don't need to use regex.
    def has_study_perms(x):
        return any([ac for ac in x.permissions() if is_managed_access(ac)])

    withdrawn_root = not has_study_perms(item)
    withdrawn_child = True

    if item.rods_type == Collection and recurse:
        for it in item.iter_contents(recurse=True):
            if has_study_perms(it):
                withdrawn_child = False
                break

    return withdrawn_root and withdrawn_child


def has_consent_withdrawn(item: Collection | DataObject, recurse=False) -> bool:
    """Return True if the data object has metadata and permissions for data with consent
    withdrawn.

    Args:
        item: The collection or data object to check.
        recurse: Include a check of the collection contents recursively. Defaults to
        False. If True and item is a DataObject, raises a ValueError.

    Returns:
        True if the metadata and permissions were as expected.
    """
    return has_consent_withdrawn_metadata(
        item, recurse=recurse
    ) and has_consent_withdrawn_permissions(item, recurse=recurse)


def ensure_consent_withdrawn(item: Collection | DataObject, recurse=False) -> bool:
    """Ensure that a data object or collection has its metadata and permissions in the
    correct state for having consent withdrawn. All read permissions are withdrawn
    except for:

    - The current user making these changes.
    - Any rodsadmin.

    Args:
        item: The collection or data object to check.
        recurse: Apply to collection contents recursively. Defaults to False. If True
            and item is a DataObject, raises a ValueError.

    Returns:
        True if the metadata and/or permissions were updated.
    """
    if item.rods_type == DataObject and recurse:
        raise ValueError(
            f"Cannot recursively withdraw permissions on a data object: {item}"
        )

    if has_consent_withdrawn(item, recurse=recurse):
        return False

    updated_meta = ensure_consent_withdrawn_metadata(item, recurse=recurse)
    if updated_meta:
        log.info(
            "Updated consent withdrawn metadata",
            path=item,
            has_withdrawn_meta=has_consent_withdrawn_metadata(item, recurse=recurse),
        )

    curr_user = current_user()

    def withdraw(x):  # Withdraw perms from a single path, return True if changes made
        to_remove = []

        for ac in x.permissions():
            u = rods_user(ac.user)

            if u is None:
                log.info("Removing permissions (non-local user)", path=x, ac=ac)
                to_remove.append(ac)
                continue
            if u == curr_user:
                log.info(
                    "Not removing permissions for self", path=x, user=str(u), ac=ac
                )
                continue
            if u.is_rodsadmin():
                log.info(
                    "Not removing permissions for rodsadmin", path=x, user=str(u), ac=ac
                )
                continue

            log.info("Removing permissions", path=x, user=str(u), ac=ac)
            to_remove.append(ac)

        num_removed = item.remove_permissions(*to_remove)
        log.info(
            "Removed permissions",
            path=x,
            num_removed=num_removed,
            has_withdrawn_perm=has_consent_withdrawn_permissions(x),
        )

        return num_removed > 0

    updated_perms_root = withdraw(item)
    updated_perms_child = False

    if item.rods_type == Collection and recurse:
        for it in item.iter_contents(recurse=True):
            if withdraw(it):
                updated_perms_child = True

    return updated_meta or updated_perms_root or updated_perms_child


def has_id_product_metadata(obj: DataObject):
    """Return True if the data object has id product metadata.

    Args:
        obj: The data object to check

    Returns:
        True if the object has id product metadata, False otherwise.
    """
    return len(obj.metadata(SeqConcept.ID_PRODUCT)) > 0
