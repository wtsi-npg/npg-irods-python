# -*- coding: utf-8 -*-
#
# Copyright © 2022 Genome Research Ltd. All rights reserved.
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

"""Support for metadata common to all data objects added to iRODS by NPG."""

from datetime import datetime
from enum import unique
from pathlib import PurePath

from partisan.irods import AVU, DataObject, RodsItem
from partisan.metadata import AsValueEnum, DublinCore
from structlog import get_logger

log = get_logger(__name__)


"""A fallback value for dcterms:creator for use when the original process or user 
that created the data object is not known """
UNKNOWN_CREATOR = "unknown"

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
        "txt",
        "xls",
        "xlsx",
        "xml",
        "xml",
    ]
}


@unique
class DataFile(AsValueEnum):
    """Data file metadata."""

    MD5 = "md5"
    TYPE = "type"


@unique
class CompressSuffix(AsValueEnum):
    """File suffixes used to indicate compressed data."""

    BZ2 = "bz2"
    GZ = "gz"
    XZ = "xz"
    ZIP = "zip"


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
        AVU(DublinCore.CREATOR.value, creator, namespace=DublinCore.namespace),
        AVU(
            DublinCore.CREATED.value,
            created.isoformat(timespec="seconds"),
            namespace=DublinCore.namespace,
        ),
    ]


def ensure_creation_metadata(obj: DataObject, creator=None) -> bool:
    """Ensure that an object has creation metadata, if it should need any. Otherwise,
    do nothing.

    Args:
        obj: The data object to repair.
        creator: The creator name string. Optional, defaults to the UNKNOWN_CREATOR
        placeholder.

    Returns:
        True if one or more AVUs required adding.
    """
    if not requires_creation_metadata(obj):
        return False

    c = creator if creator is not None else UNKNOWN_CREATOR
    t = obj.timestamp()
    return _ensure_avus_present(obj, *make_creation_metadata(c, t))


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
    return any(avu.attribute == DublinCore.MODIFIED.value for avu in obj.metadata())


def make_modification_metadata(modified: datetime) -> list[AVU]:
    """Return standard iRODS metadata for data modification:

      - dcterms:modified

    Args:
        modified: Modification timestamp.

    Returns: List[AVU]
    """
    return [
        AVU(
            DublinCore.MODIFIED.value,
            modified.isoformat(timespec="seconds"),
            namespace=DublinCore.namespace,
        )
    ]


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
    return any(avu.attribute == DataFile.MD5.value for avu in obj.metadata())


def make_checksum_metadata(checksum: str) -> list[AVU]:
    """Return standard iRODS checksum metadata:

      - md5

    Args:
        checksum: The current checksum.

    Returns: List[AVU]
    """
    return [AVU(DataFile.MD5, checksum)]


def ensure_checksum_metadata(obj: DataObject) -> bool:
    """Ensure that an object has checksum metadata, if it should need have any.
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

    return _ensure_avus_present(obj, *make_checksum_metadata(c))


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
    return any(avu.attribute == DataFile.TYPE.value for avu in obj.metadata())


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

    return _ensure_avus_present(obj, *make_type_metadata(obj))


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
    """Ensure that an object has any common metadata that it needs. Otherwise,
    do nothing.

    Args:
        obj: The data object to repair.
        creator: The creator name string. Optional, defaults to the UNKNOWN_CREATOR
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


def avu_if_value(attribute, value):
    """Return an AVU if value is not None, otherwise return None.

    Args:
        attribute: An iRODS AVU attribute.
        value: An iRODS AVU value.

    Returns:
        A new AVU instance.
    """
    if value is not None:
        return AVU(attribute, value)


def _ensure_avus_present(item: RodsItem, *avus: AVU) -> bool:
    """Ensure that an item in iRODS has the specified metadata.

    NB: this function is fore case where we only have a single AVU for each
    attribute. Do not use it for cases where there are multiple AVUs sharing the same
    attribute.

    Args:
        item: An item to update.
        avu: AVUs to ensure are present.

    Returns:
        True if one or more AVUs required adding.
    """
    missing = []

    # Note: this uses the knowledge that we only have a single AVU for each attribute
    meta = {avu.attribute: avu for avu in avus}
    for attr, avu in meta.items():
        if not any(a.attribute == attr for a in item.metadata()):
            missing.append(avu)

    item.add_metadata(*missing)

    return True if missing else False
