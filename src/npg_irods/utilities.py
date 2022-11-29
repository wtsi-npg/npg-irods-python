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

import subprocess
import threading
from multiprocessing.pool import ThreadPool
from pathlib import PurePath

import partisan
from partisan.exception import RodsError
from partisan.irods import Collection, DataObject, RodsItem, client_pool, make_rods_item
from structlog import get_logger

from npg_irods.exception import ChecksumError
from npg_irods.metadata.common import (
    DataFile,
    ensure_common_metadata,
    ensure_matching_checksum_metadata,
    has_checksum_metadata,
    has_common_metadata,
    has_complete_checksums,
    has_creation_metadata,
    has_matching_checksum_metadata,
    has_matching_checksums,
    has_type_metadata,
    requires_creation_metadata,
    requires_type_metadata,
)

log = get_logger(__name__)

print_lock = threading.Lock()


def _print(path, writer):
    with print_lock:
        print(path, file=writer)


def check_checksums(
    reader, writer, num_threads=1, num_clients=1, print_pass=True, print_fail=False
) -> (int, int, int):
    """Read iRODS data object paths from a file and check that each one has correct
    checksums and checksum metadata, printing the results to a writer.

    The conditions for checksums and checksum metadata of a data object to be correct
    are:

    - The data object must have a checksum set for each valid replica.
    - The checksums for all replicas must have the same value.
    - The data object must have one, and only one, checksum AVU in its metadata.
    - The checksum AVU must have the same value as the replica checksums.

    Args:
        reader: A file supplying iRODS data object paths to check, one per line.
        writer: A file where checked paths will be written, one per line.
        num_threads: The number of Python threads to use. Defaults to 1.
        num_clients: The number of baton clients to use, Defaults to 1.
        print_pass: Print the paths of objects passing the check. Defaults to True.
        print_fail: Print the paths of objects failing the check. Defaults to False.

    Returns:
        A tuple of the number of paths checked, the number of paths found to be correct
        and the number of errors (paths with incorrect checksums and/or paths that
        failed to be checked because of an exception).
    """
    with client_pool(num_clients) as bp:

        def fn(i: int, path: str) -> bool:
            success = False

            p = path.strip()
            try:
                obj = DataObject(p, pool=bp)
                if has_matching_checksum_metadata(obj):
                    success = True
                    log.info("Checksums correct", item=i, path=obj)
                    if print_pass:
                        _print(p, writer)
                else:
                    checksums = [
                        avu.value
                        for avu in obj.metadata()
                        if avu.attribute == DataFile.MD5.value
                    ]
                    checksums.sort()
                    log.warn(
                        "Checksum metadata do not match",
                        item=i,
                        path=obj,
                        checksum=obj.checksum(),
                        metadata=checksums,
                    )

                    if print_fail:
                        _print(p, writer)

            except RodsError as re:
                log.error(re.message, item=i, code=re.code)
                if print_fail:
                    _print(p, writer)
            except ChecksumError as ce:
                log.error(ce, item=i, expected=ce.expected, observed=ce.observed)
                if print_fail:
                    _print(p, writer)
            except Exception as e:
                log.error(e)
                if print_fail:
                    _print(p, writer)

            return success

        with ThreadPool(num_threads) as tp:
            results = tp.starmap(fn, enumerate(reader))
            num_succeeded = results.count(True)

        return len(results), num_succeeded, len(results) - num_succeeded


def repair_checksums(
    reader,
    writer,
    num_threads=1,
    num_clients=1,
    print_repair=True,
    print_fail=False,
) -> (int, int, int):
    """Read iRODS data object paths from a file and ensure that each one has correct
    checksums and checksum metadata by making any necessary repairs, printing the
    results to a writer.

    The possible repairs are:

    - Data object checksums: valid replicas that have no checksum are updated to have
      their current checksum according to their state on disk, using the iRODS API.

    - Data object metadata: if all valid replicas have the same checksum and there is
      no checksum metadata AVU, then one is added.

    The following states are not repaired automatically because they require an
    assessment on which, if any, replicas are correct.

    - The checksums across all valid replicas are not identical.
    - The checksum metadata AVU does not concur with the checksum(s) of the valid
      replicas.

    Args:
        reader: A file supplying iRODS data object paths to repair, one per line.
        writer: A file where repaired paths will be written, one per line.
        num_threads: The number of Python threads to use. Defaults to 1.
        num_clients: The number of baton clients to use, Defaults to 1.
        print_repair: Print the paths of objects that required repair and were
            repaired successfully. Defaults to True.
        print_fail: Print the paths of objects that required repair and the repair
            failed. Defaults to False.

    Returns:
        A tuple of the number of paths checked, the number of paths with a checksum
        repaired and the number of errors (paths with incorrect checksums that could
        not be fixed and/or paths that failed to be fixed because of an exception).
    """
    with client_pool(num_clients) as bp:

        def fn(i: int, path: str) -> (bool, bool):
            success = False
            repair = False

            p = path.strip()
            try:
                obj = DataObject(p, pool=bp)
                if has_matching_checksum_metadata(obj):
                    success = True
                    log.info("Checksum metadata matches", item=i, path=obj)
                else:
                    log.info(
                        "Checksum metadata incomplete; repairing",
                        item=i,
                        path=obj,
                        has_compl_checksums=has_complete_checksums(obj),
                        has_match_checksums=has_matching_checksums(obj),
                        has_checksum_meta=has_checksum_metadata(obj),
                    )
                    if ensure_matching_checksum_metadata(obj):
                        success = repair = True
                        if print_repair:
                            _print(p, writer)

            except RodsError as re:
                log.error(re.message, item=i, code=re.code)
                if print_fail:
                    _print(p, writer)
            except ChecksumError as ce:
                log.error(ce, item=i, expected=ce.expected, observed=ce.observed)
                if print_fail:
                    _print(p, writer)
            except Exception as e:
                log.error(e, item=i)
                if print_fail:
                    _print(p, writer)

            return success, repair

        with ThreadPool(num_threads) as tp:
            results, repaired = zip(*tp.starmap(fn, enumerate(reader)))
            num_succeeded = results.count(True)

        return len(results), repaired.count(True), len(results) - num_succeeded


def check_common_metadata(
    reader, writer, num_threads=1, num_clients=1, print_pass=True, print_fail=False
) -> bool:
    """Read iRODS data object paths from a file and check that each one has correct
    common metadata, printing the results to a writer.

    Tests show that the fastest performance will be obtained with ~4 each of threads
    and clients.

    Args:
        reader: A file supplying iRODS data object paths to check, one per line.
        writer: A file where checked paths will be written, one per line.
        num_threads: The number of Python threads to use. Defaults to 1.
        num_clients: The number of baton clients to use, Defaults to 1.
        print_pass: Print the paths of objects passing the check. Defaults to True.
        print_fail: Print the paths of objects failing the check. Defaults to False.

    Returns:
        True if all checks are done.
    """
    with client_pool(num_clients) as bp:

        def fn(i: int, path: str):
            success = False

            p = path.strip()
            try:
                obj = DataObject(p, pool=bp)
                if has_common_metadata(obj):
                    log.info("Common metadata complete", item=i, path=obj)
                    if print_pass:
                        _print(p, writer)
                else:
                    log.warn(
                        "Common metadata incomplete",
                        item=i,
                        path=obj,
                        req_checksum_meta=requires_creation_metadata(obj),
                        has_checksum_meta=has_checksum_metadata(obj),
                        req_creation_meta=requires_creation_metadata(obj),
                        has_creation_meta=has_creation_metadata(obj),
                        req_type_meta=requires_type_metadata(obj),
                        has_type_meta=has_type_metadata(obj),
                    )

                    if print_fail:
                        _print(p, writer)

                success = True

            except RodsError as re:
                log.error(re.message, item=i, code=re.code)
                if print_fail:
                    _print(p, writer)
            except Exception as e:
                log.error(e, item=i)
                if print_fail:
                    _print(p, writer)

            return success

        with ThreadPool(num_threads) as tp:
            succeeded = tp.starmap(fn, enumerate(reader))

        return all(succeeded)


def repair_common_metadata(
    reader,
    writer,
    creator=None,
    num_threads=1,
    num_clients=1,
    print_repair=True,
    print_fail=False,
) -> bool:
    """Read iRODS data object paths from a file and ensure that each one has correct
    common metadata by making any necessary repairs, printing the results to a writer.

    The possible repairs are:

    - Creation metadata: the creation time is estimated from iRODS' internal record of
      creation time of one of the object's replicas. The creator is set to the current
      user.
    - Checksum metadata: the checksum is taken from the object's replicas.
    - Type metadata: the type is taken from the object's path.

    Args:
        reader: A file supplying iRODS data object paths to repair, one per line.
        writer: A file where repaired paths will be written, one per line.
        creator: The name of a data creator to use in any metadata generated.
            Optional, defaults to a placeholder value.
        num_threads: The number of Python threads to use. Defaults to 1.
        num_clients: The number of baton clients to use, Defaults to 1.
        print_repair: Print the paths of objects that required repair and were
            repaired successfully. Defaults to True.
        print_fail: Print the paths of objects that required repair and the repair
            failed. Defaults to False.

    Returns:
        True if all repairs were done.
    """
    with client_pool(num_clients) as bp:

        def fn(i: int, path: str) -> bool:
            success = False

            p = path.strip()
            try:
                obj = DataObject(p, pool=bp)
                if not has_common_metadata(obj):
                    log.info(
                        "Common metadata incomplete; repairing",
                        item=i,
                        path=obj,
                        req_checksum=requires_creation_metadata(obj),
                        has_checksum=has_checksum_metadata(obj),
                        req_creation=requires_creation_metadata(obj),
                        has_creation=has_creation_metadata(obj),
                        req_type=requires_type_metadata(obj),
                        has_type=has_type_metadata(obj),
                    )

                    if ensure_common_metadata(obj, creator=creator):
                        if print_repair:
                            _print(p, writer)
                    success = True
            except RodsError as re:
                log.error(re.message, item=i, code=re.code)
                if print_fail:
                    _print(p, writer)
            except Exception as e:
                log.error(e, item=i)
                if print_fail:
                    _print(p, writer)

            return success

        with ThreadPool(num_threads) as tp:
            succeeded = tp.starmap(fn, enumerate(reader))

        return all(succeeded)


def copy(src, dest, acl=False, avu=False, exist_ok=False, recurse=False) -> (int, int):
    """Copy a collection or data object from one location to another, optionally
    including metadata and permissions.

    Args:
        src: A DataObject, Collection, PurePath or str path to copy from.
        dest: A DataObject, Collection, PurePath or str path to copy to.
        acl: If True, also copy any permissions.
        avu: If True, also copy any metadata.
        exist_ok: If True, check for existing collections and data objects at the
            destination. If they exist and are identical to what would be the result of
            copying, do not raise an error.
        recurse: If True, recurse into collections when copying.

    Returns:
        A tuple of the number of items (collections and data objects) processed, the
        number of items copied.

        If exist_ok is True, this number copied does not include counts of collections
        and data objects which were not copied because they already existed at the
        destination.
    Raises:
        ChecksumError if checksums are inconsistent.
    """
    num_processed, num_copied = 0, 0

    def _cp_avu_acl(s, d):
        if avu:
            n = d.add_metadata(*s.metadata())
            log.info(f"Added {n} AVUs", path=d)
        if acl:
            n = d.add_permissions(*s.permissions())
            log.info(f"Added {n} permissions", path=d)

    def _maybe_copy_obj(s: DataObject, d: DataObject) -> int:
        if exist_ok and d.exists():
            if s.checksum() != d.checksum():
                raise ChecksumError(
                    "A data object with a different checksum exists at the destination",
                    path=d,
                    observed=d.checksum(),
                    expected=s.checksum(),
                )
            if not has_matching_checksums(s):
                raise ChecksumError(
                    "The source data object does not have matching checksums",
                    path=s,
                    observed=[r.checksum for r in s.replicas()],
                )
            if not has_matching_checksums(d):
                raise ChecksumError(
                    "The destination data object does not have matching checksums",
                    path=d,
                    observed=[r.checksum for r in d.replicas()],
                )
            log.info(
                "Skipping copy of data object, destination object exists",
                src=s,
                dest=d,
                checksum=d.checksum(),
            )

            return 0

        log.info("Copying data object", src=s, dest=d)
        _icp(str(s), str(d), verify_checksum=True)
        return 1

    def _maybe_copy_coll(s: Collection, d: Collection) -> int:
        if exist_ok and d.exists():
            log.info(
                "Skipping copy of collection, destination collection exists",
                src=s,
                dest=d,
            )
            return 0

        log.info("Copying collection", src=s, dest=d)

        coll.create(exist_ok=exist_ok)
        return 1

    if not isinstance(src, RodsItem):
        src = make_rods_item(src)
    if not isinstance(dest, RodsItem):
        dest = make_rods_item(dest)

    match (src.rods_type, dest.rods_type):
        case (partisan.irods.Collection, partisan.irods.DataObject):
            raise ValueError(
                f"Cannot copy a collection {src} into a data object {dest}"
            )

        case (partisan.irods.Collection, partisan.irods.Collection) | (
            partisan.irods.Collection,
            None,
        ):
            coll = Collection(PurePath(dest.path, src.path.name))
            num_processed += 1
            num_copied += _maybe_copy_coll(src, coll)
            _cp_avu_acl(src, coll)

            if recurse:
                for item in src.contents():
                    np, nc = copy(
                        item,
                        Collection(coll.path),
                        avu=avu,
                        acl=acl,
                        exist_ok=exist_ok,
                        recurse=True,
                    )
                    num_processed += np
                    num_copied += nc

        case (partisan.irods.DataObject, partisan.irods.DataObject) | (
            partisan.irods.DataObject,
            None,
        ):
            num_processed += 1
            num_copied += _maybe_copy_obj(src, dest)
            _cp_avu_acl(src, dest)

        case (partisan.irods.DataObject, partisan.irods.Collection):
            obj = DataObject(PurePath(dest.path, src.name))
            num_processed += 1
            num_copied += _maybe_copy_obj(src, obj)
            _cp_avu_acl(src, obj)

        case _:
            raise ValueError(
                f"Invalid iRODS path type combination src: {src}: "
                f"src type: {src.rods_type}, "
                f"dest: {dest}, dest type: {dest.rods_type}"
            )

    return num_processed, num_copied


def _icp(src, dest, force=False, verify_checksum=True):
    cmd = ["icp"]

    if force:
        cmd.append("-f")
    if verify_checksum:
        cmd.append("-K")

    cmd.append(src)
    cmd.append(dest)
    log.debug("Running command", cmd=cmd)
    log.info("Copying data object", src=src, dest=dest, force=force)

    completed = subprocess.run(cmd, capture_output=True)
    if completed.returncode == 0:
        return

    raise RodsError(completed.stderr.decode("utf-8").strip(), 0)
