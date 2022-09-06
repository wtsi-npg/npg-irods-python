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

import re
from datetime import datetime
from enum import unique

from partisan.irods import AVU, DataObject
from partisan.metadata import AsValueEnum, DublinCore


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


def requires_creation_metadata(obj: DataObject) -> bool:
    """Return True if the data object should have these metadata.

    Args:
        obj: The data object to check.

    Returns:
        True if the metadata are required, or False otherwise.
    """
    return True


def has_creation_metadata(obj: DataObject) -> bool:
    """Return True if the data object has all expected creation metadata.

    Args:
        obj: The data object to check.

    Returns:
        True if all metadata are present, or False otherwise.
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


def requires_modification_metadata(obj: DataObject) -> bool:
    """Return True if the data object should have these metadata.

    Args:
        obj: The data object to check.

    Returns:
        True if the metadata are required, or False otherwise.
    """
    return False


def has_modification_metadata(obj: DataObject) -> bool:
    """Return True if the data object has all expected modification metadata.

    Args:
        obj: The data object to check.

    Returns:
        True if all metadata are present, or False otherwise.
    """
    return any(avu.attribute == str(DublinCore.MODIFIED) for avu in obj.metadata())


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
    """Return True if the data object has all expected checksum metadata. This function
    does not check that the checksum is valid, only that it is present .

    Args:
        obj: The data object to check.

    Returns:
        True if all metadata are present, or False otherwise.
    """
    return any(avu.attribute == str(DataFile.MD5) for avu in obj.metadata())


def make_checksum_metadata(checksum: str) -> list[AVU]:
    """Return standard iRODS checksum metadata:

      - md5

    Args:
        checksum: The current checksum.


    Returns: List[AVU]
    """
    return [AVU(DataFile.MD5, checksum)]


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
    """Return True if the data object has all expected data type metadata.

    Args:
        obj: The data object to check.

    Returns:
        True if all metadata are present, or False otherwise.
    """

    return any(avu.attribute == str(DataFile.TYPE) for avu in obj.metadata())


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


def parse_object_type(obj: DataObject):
    """Return a data "type" parsed from the data object path.

    The type value is determined from the path suffix, after removing any suffix
    that indicates compression (e.g. "gz", "bz2"). Case is ignored and the result is
    folded to lower case.

     Args:
         obj: A data object whose type is to be described.

    Returns: A type string, if one can be parsed, or None.
    """
    compress = "|".join([s.value for s in CompressSuffix])

    # All the groups are named for documentation and debug purposes, even though
    # only one is used:
    #
    # path: The file path/name, including the suffix.
    # suffix: The file suffix we are interested in.
    # dotcsuffix: The compression suffix, including dot.
    # csuffix: The compression suffix, without dot.
    regex = re.compile(
        "(?P<path>[^.]*[.](?P<suffix>[^.]*))"
        f"(?P<dotcsuffix>[.](?P<csuffix>{compress}))*",
        re.IGNORECASE,
    )

    m = regex.match(obj.name)
    if m:
        if m.group("suffix"):
            return m.group("suffix").lower()

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
        True if all metadata are present, or False otherwise.
    """
    checks = [has_checksum_metadata, has_checksum_metadata]
    if requires_type_metadata(obj):
        checks.append(has_type_metadata)

    return all(fn(obj) for fn in checks)
