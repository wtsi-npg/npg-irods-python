# -*- coding: utf-8 -*-
#
# Copyright © 2022, 2023, 2025 Genome Research Ltd. All rights reserved.
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

"""Support for metadata common to all data added to iRODS by NPG."""

from datetime import datetime
from enum import unique
from pathlib import PurePath
from typing import List

from partisan.irods import AVU, DataObject, Replica, RodsItem
from partisan.metadata import AsValueEnum, DublinCore, with_namespace
from structlog import get_logger

from npg_irods.exception import ChecksumError

log = get_logger(__name__)

"""A fallback value for dcterms:creator for use when the original process or user 
that created the data object is the Wellcome Sanger Institute."""
WSI_CREATOR = "http://www.sanger.ac.uk"


"""The iRODS group that is used to control access to public data."""
PUBLIC_IRODS_GROUP = "public"

"""A timeout in seconds for short iRODS operations,such as listing single items,
and non-recursive metadata and ACL operations."""
SHORT_IRODS_TIMEOUT: float = 10

"""A lookup table of the file suffixes which will be recognised for iRODS "type" 
metadata."""
RECOGNISED_FILE_SUFFIXES = {
    elt: True
    for elt in [
        "_samhaplotag_clear_bc",
        "_samhaplotag_missing_bc_qt_tags",
        "_samhaplotag_unclear_bc",
        "bai",
        "bam",
        "bam_stats",
        "bamcheck",
        "bcfstats",
        "bed",
        "bin",
        "bqsr_table",
        "crai",
        "cram",
        "csv",
        "fasta",
        "flagstat",
        "gtc",
        "h5",
        "hops",
        "idat",
        "json",
        "pbi",
        "quant",
        "seqchksum",
        "stats",
        "tab",
        "tar",
        "tbi",
        "tgz",
        "tif",
        "tsv",
        "txt",
        "xls",
        "xlsx",
        "xml",
    ]
}


@unique
class SeqConcept(AsValueEnum):
    """Sequencing terminology."""

    ALT_PROCESS = "alt_process"
    COMPONENT = "component"
    ID_PRODUCT = "id_product"
    POSITION = "position"
    REFERENCE = "reference"
    SUBSET = "subset"
    TAG_INDEX = "tag_index"


@unique
class SeqSubset(AsValueEnum):
    """Categorised subsets of sequence reads which are separated from each other after
    sequencing."""

    HUMAN = "human"
    """Human DNA."""

    PHIX = "phix"
    """Phi X control reads."""

    XAHUMAN = "xahuman"
    """Human X chromosome and autosome reads."""

    YHUMAN = "yhuman"
    """Human Y chromosome reads separated from X and autosomal data."""

    @staticmethod
    def from_string(s: str):
        """Return a SeqSubset from a string.

        Args:
            s: The string to parse.

        Returns:
            A SeqSubset, or None if no match.
        """
        if s is None:
            return None

        for subset in SeqSubset:
            if subset.value == s:
                return subset

        raise ValueError(f"Invalid subset '{s}'")


@unique
class DataFile(AsValueEnum):
    """Data file metadata."""

    MD5 = "md5"
    TYPE = "type"
    TARGET = "target"


@unique
class CompressSuffix(AsValueEnum):
    """File suffixes used to indicate compressed data."""

    BZ2 = "bz2"
    GZ = "gz"
    XZ = "xz"
    ZIP = "zip"


@unique
class SqyrrlApp(AsValueEnum, metaclass=with_namespace("sqyrrl")):
    """Sqyrrl application metadata.

    See https://github.com/wtsi-npg/sqyrrl
    """

    index = "index"
    category = "category"

    def __repr__(self):
        return f"{SqyrrlApp.namespace}:{self.value}"


# Checksums are not metadata in the sense of iRODS AVUs, but are nevertheless metadata
def has_complete_checksums(obj: DataObject) -> bool:
    """Return True if the data object has all required checksums.

    This is defined as having complete checksum coverage i.e. that every valid
    replica has a checksum. These checksums do not necessarily have to agree (which
    would indicate a data integrity problem), but simply be present.

    Args:
        obj: The data object to check.

    Returns:
        True if there is full checksum coverage, or False otherwise.
    """

    if len(obj.replicas()) == 0:
        raise ValueError(f"The replica list of {obj} is empty")

    for r in obj.replicas():
        if r.valid and r.checksum is None:
            log.debug("Valid replica has no checksum", path=obj, number=r.number)
            return False

    return True


def has_matching_checksums(obj: DataObject) -> bool:
    """Return True if the data object has the same checksum for every valid replica.

    If the data object does not have complete checksums, this function returns False.

    Args:
        obj: The data object to check.

    Returns:
        True if all the valid replicas share the same checksum, or False otherwise.
    """
    if not has_complete_checksums(obj):
        return False

    checksum = obj.checksum()
    for r in obj.replicas():
        if r.valid and r.checksum != checksum:
            log.debug(
                "Valid replica has non-matching checksum",
                path=obj,
                number=r.number,
                expected=checksum,
                observed=r.checksum,
            )
            return False

    return True


def has_matching_checksum_metadata(obj: DataObject) -> bool:
    """Return True if the data object has complete, matching checksums and checksum
    metadata, and all of these concur.

    Note that function does not check whether checksum metadata are required to be on
    the data object.

    Args:
        obj: The data object to check.

    Returns:
        True if all checksums and metadata concur, or False otherwise.
    """
    # It's possible, technically, for there to be multiple checksum AVUs in an
    # object's metadata because iRODS is permissive on this. If we find more than
    # one, we consider that the checksum metadata do not match.
    checksum_meta = obj.metadata(DataFile.MD5)
    if len(checksum_meta) > 1:
        return False

    if not has_matching_checksums(obj):
        return False

    if not has_checksum_metadata(obj):
        return False

    return AVU(DataFile.MD5, obj.checksum()) in checksum_meta


def ensure_matching_checksum_metadata(obj: DataObject) -> bool:
    """Ensure that a data object has checksum metadata that matches its iRODS checksums.

    This check implies that the data object has full checksum coverage of all its
    valid replicas, and that those checksums all match each other. This function first
    attempts to ensure that this is the case. If this cannot be achieved, a
    ChecksumError is raised.

    If there is no checksum metadata on the data object, a new AVU is added and this
    function returns True. If there is already checksum metadata on the data object
    that concurs with the current checksum, this function does nothing and returns
    False.

    If there is already checksum metadata that does not concur with the current
    checksum, the checksum metadata value is updated and the previous value added
    to metadata history.

    Args:
        obj: The data object to repair.

    Returns:
        True if a repair was done.
    """
    if has_matching_checksum_metadata(obj):
        return False

    valid_replicas = [r for r in obj.replicas() if r.valid]
    expected_checksums = [obj.checksum()] * len(valid_replicas)
    observed_checksums = [r.checksum for r in valid_replicas]

    if not has_complete_checksums(obj):
        raise ChecksumError(
            "Failed to ensure that the data object has matching checksum metadata "
            "because not all of its valid replicas have a checksum",
            path=obj,
            expected=expected_checksums,
            observed=observed_checksums,
        )

    if not has_matching_checksums(obj):
        raise ChecksumError(
            "Failed to ensure that the data object has matching checksum metadata "
            "because its valid replica checksums do not match each other",
            path=obj,
            expected=expected_checksums,
            observed=observed_checksums,
        )

    if not has_checksum_metadata(obj):
        obj.add_metadata(AVU(DataFile.MD5, obj.checksum()))
        return True

    expected_avu = AVU(DataFile.MD5, obj.checksum())
    observed_avus = obj.metadata(DataFile.MD5)

    num_added, num_removed = obj.supersede_metadata(expected_avu, history=True)
    if num_added and expected_avu in obj.metadata():
        delta = set(obj.metadata()).difference(observed_avus)
        log.info(
            "Updated checksum metadata",
            path=obj,
            expected=expected_avu,
            observed=observed_avus,
            num_added=num_added,
            num_removed=num_removed,
            delta=delta,
        )
        return True

    if expected_avu not in obj.metadata():
        raise ChecksumError(
            "Existing checksum metadata did not match the iRODS checksum",
            path=obj,
            expected=expected_avu,
            observed=observed_avus,
        )

    return False


def has_complete_replicas(obj: DataObject, num_replicas=2) -> bool:
    """Return True if the data object has all required replicas.

    This is defined as having at least the expected number of valid replicas, with
    matching checksums on those that are valid.

    Args:
        obj: The data object to check.
        num_replicas: The expected number of valid replicas. Defaults to 2.

    Returns:
        True if there are complete replicas, or False otherwise.
    """
    if num_replicas < 1:
        raise ValueError(
            f"The num_replicas argument may not be less than 1: {num_replicas}"
        )

    num_valid = len([r for r in obj.replicas() if r.valid])

    return num_valid >= num_replicas and has_matching_checksums(obj)


def trimmable_replicas(
    obj: DataObject, num_replicas=2
) -> (List[Replica], List[Replica]):
    """Return tuple of lists of valid and invalid replicas that are trimmable.

    Trimmable replicas are any valid replicas in excess of the expected number
    and any invalid replicas (invalid replicas are always trimmable).

    Args:
        obj: The data object to check.
        num_replicas: The expected number of valid replicas. Defaults to 2.

    Returns:
        A tuple of lists of replicas, those valid first.
    """
    if num_replicas < 1:
        raise ValueError(
            f"The num_replicas argument may not be less than 1: {num_replicas}"
        )

    valid = []
    invalid = []
    for r in obj.replicas():
        if r.valid:
            valid.append(r)
        else:
            invalid.append(r)

    return valid[num_replicas:], invalid


def has_trimmable_replicas(obj: DataObject, num_replicas=2) -> bool:
    """Return True if the data object has replicas that may be trimmed.

    Trimmable replicas are any valid replicas in excess of the expected number
    and any invalid replicas (invalid replicas are always trimmable).

    Args:
        obj: The data object to check.
        num_replicas: The number of valid replicas that should be present.

    Returns:
        True if there are any replicas to trim.
    """
    valid, invalid = trimmable_replicas(obj, num_replicas=num_replicas)
    return valid or invalid


def requires_creation_metadata(obj: DataObject) -> bool:
    """Return True if the data object should have these metadata.

    Args:
        obj: The data object to check.

    Returns:
        True if the metadata are required, or False otherwise.
    """
    return True


def has_creation_metadata(obj: DataObject) -> bool:
    """Return True if the data object has the expected creation metadata.

    Args:
        obj: The data object to check.

    Returns:
        True if the metadata are present, or False otherwise.
    """
    expected = [str(attr) for attr in [DublinCore.CREATED, DublinCore.CREATOR]]
    observed = [avu.attribute for avu in obj.metadata()]

    return set(expected).issubset(set(observed))


def make_creation_metadata(creator: str, created: datetime) -> list[AVU]:
    """Return standard iRODS metadata for data creation:

      - dcterms:creator
      - dcterms:created

    Args:
        creator: Name of user or service creating data.
        created: Creation timestamp.

    Returns: List[AVU]
    """
    return [
        AVU(DublinCore.CREATOR, creator),
        AVU(DublinCore.CREATED, created.isoformat(timespec="seconds")),
    ]


def ensure_creation_metadata(obj: DataObject, creator=None) -> bool:
    """Ensure that an object has creation metadata, if it should need any. Otherwise,
    do nothing.

    Args:
        obj: The data object to repair.
        creator: The creator name string. Optional, defaults to the WSI_CREATOR
        placeholder.

    Returns:
        True if one or more AVUs required adding.
    """
    if not requires_creation_metadata(obj):
        return False

    if has_creation_metadata(obj):
        return False

    c = creator if creator is not None else WSI_CREATOR
    t = obj.timestamp()

    return ensure_avus_present(obj, *make_creation_metadata(c, t))


def requires_modification_metadata(obj: DataObject) -> bool:
    """Return True if the data object should have these metadata.

    Args:
        obj: The data object to check.

    Returns:
        True if the metadata are required, or False otherwise.
    """
    return False


def has_modification_metadata(obj: DataObject) -> bool:
    """Return True if the data object has the expected modification metadata.

    Args:
        obj: The data object to check.

    Returns:
        True if the metadata are present, or False otherwise.
    """
    return len(obj.metadata(str(DublinCore.MODIFIED))) > 0


def make_modification_metadata(modified: datetime) -> list[AVU]:
    """Return standard iRODS metadata for data modification:

      - dcterms:modified

    Args:
        modified: Modification timestamp.

    Returns: List[AVU]
    """
    return [AVU(DublinCore.MODIFIED, modified.isoformat(timespec="seconds"))]


def requires_checksum_metadata(obj: DataObject) -> bool:
    """Return True if the data object should have these metadata.

    Args:
        obj: The data object to check.

    Returns:
        True if the metadata are required, or False otherwise.
    """
    return True


def has_checksum_metadata(obj: DataObject) -> bool:
    """Return True if the data object has the expected checksum metadata. This
    function does not check that the checksum is valid, only that it is present.

    Args:
        obj: The data object to check.

    Returns:
        True if the metadata are present, or False otherwise.
    """
    return len(obj.metadata(DataFile.MD5)) > 0


def make_checksum_metadata(checksum: str) -> list[AVU]:
    """Return standard iRODS checksum metadata:

      - md5

    Args:
        checksum: The current checksum.

    Returns: List[AVU]
    """
    return [AVU(DataFile.MD5, checksum)]


def ensure_checksum_metadata(obj: DataObject) -> bool:
    """Ensure that an object has checksum metadata, if it should need any.
    Otherwise, do nothing.

    Args:
        obj: The data object to repair.

    Returns:
        True if one or more AVUs required adding.
    """
    if not requires_checksum_metadata(obj):
        return False

    c = obj.checksum()
    if not c:
        raise ValueError(f"Empty checksum returned from iRODS for {obj}")

    return ensure_avus_present(obj, *make_checksum_metadata(c))


def requires_type_metadata(obj: DataObject) -> bool:
    """Return True if the data object should have these metadata.

    Args:
        obj: The data object to check.

    Returns:
        True if the metadata are required, or False otherwise.
    """
    t = parse_object_type(obj)
    return RECOGNISED_FILE_SUFFIXES.get(t, False)


def has_type_metadata(obj: DataObject) -> bool:
    """Return True if the data object has the expected data type metadata.

    Args:
        obj: The data object to check.

    Returns:
        True if the metadata are present, or False otherwise.
    """
    return len(obj.metadata(DataFile.TYPE)) > 0


def make_type_metadata(obj: DataObject) -> list[AVU]:
    """Return standard iRODS data type metadata:

       - type

    The type value is determined from the path suffix, after removing and suffix
    that indicates compression (e.g. "gz", "bz2"). Case is ignored and the result is
    folded to lower case.

    This function will return metadata regardless of whether it is required for the
    data object, or not. See requires_type_metadata.

     Args:
         obj: A data object whose type is to be described.

     Returns: List[AVU]
    """
    t = parse_object_type(obj)
    if t:
        return [AVU(DataFile.TYPE, t)]
    return []


def ensure_type_metadata(obj: DataObject) -> bool:
    """Ensure that an object has type metadata, if it should need any. Otherwise, do
    nothing.

    Args:
        obj: The data object to repair.

    Returns:
        True if one or more AVUs required adding.
    """
    if not requires_type_metadata(obj):
        return False

    return ensure_avus_present(obj, *make_type_metadata(obj))


def parse_object_type(obj: DataObject):
    """Return a data "type" parsed from the data object path.

    The type value is determined from the path suffix, after removing any suffix
    that indicates compression (e.g. "gz", "bz2"). Case is ignored and the result is
    folded to lower case.

     Args:
         obj: A data object whose type is to be described.

    Returns: A type string, if one can be parsed, or None.
    """
    suffixes = [s.lstrip(".") for s in PurePath(obj).suffixes]
    compress_suffixes = [s.value for s in CompressSuffix]

    for s in reversed(suffixes):
        if s in compress_suffixes:
            continue
        return s
    return None


def has_common_metadata(obj: DataObject) -> bool:
    """Return True if the data object has the metadata that should be common to any
    file in the iRODS system. These are

       - Creation metadata
       - Checksum metadata
       - File data type metadata

    Args:
        obj: The data object to check.

    Returns:
        True if metadata are present, or False otherwise.
    """
    checks = [has_creation_metadata, has_checksum_metadata]
    if requires_type_metadata(obj):
        checks.append(has_type_metadata)

    return all(fn(obj) for fn in checks)


def ensure_common_metadata(obj: DataObject, creator=None) -> bool:
    """Ensure that an object has any common metadata that it needs. If it does not
    need any, or the metadata are present, do nothing.

    This function doesn't handle cases where invalid checksum metadata are present
    e.g. if the object was overwritten with a new file but the checksum metadata was
    not updated. For that, see utilities.ensure_matching_checksum_metadata.

    Args:
        obj: The data object to repair.
        creator: The creator name string. Optional, defaults to the WSI_CREATOR
        placeholder.

    Returns:
        True if one or more AVUs required adding.
    """
    changed = [
        ensure_creation_metadata(obj, creator=creator),
        ensure_checksum_metadata(obj),
        ensure_type_metadata(obj),
    ]

    return any(changed)


def make_sqyrrl_metadata(category: str = None) -> list[AVU]:
    """Make Sqyrrl metadata AVUs for a data object to be indexed
    Args:
        category:

    Returns:

    """
    if category is None:
        return [AVU(SqyrrlApp.index, "1")]

    return [AVU(SqyrrlApp.category, category), AVU(SqyrrlApp.index, "1")]


def ensure_sqyrrl_metadata(obj: DataObject, category: str = None) -> bool:
    """Ensure that an object has Sqyrrl metadata.

    Args:
        obj: The data object to repair.
        category: The Sqyrrl category.

    Returns:
        True if one or more AVUs required adding.
    """
    return ensure_avus_present(obj, *make_sqyrrl_metadata(category))


def avu_if_value(attribute, value):
    """Return an AVU if value is not None, otherwise return None.

    Args:
        attribute: An iRODS AVU attribute.
        value: An iRODS AVU value.

    Returns:
        A new AVU instance.
    """
    return AVU(attribute, value) if value is not None else None


def ensure_avus_present(item: RodsItem, *avus: AVU) -> bool:
    """Ensure that an item in iRODS has the specified metadata.

    Args:
        item: An item to update.
        avus: AVUs to ensure are present.

    Returns:
        True if one or more AVUs required adding.
    """

    current = set(item.metadata())
    candidates = set(avus)

    missing = candidates.difference(current)
    item.add_metadata(*missing)

    return True if missing else False


def has_target_metadata(obj: DataObject) -> bool:
    """Return true if the object has 'target' metadata.

    Args:
        obj: The data object to check.

    Returns:
        True if the object has 'target' metadata.
    """
    return len(obj.metadata(DataFile.TARGET.value)) > 0
