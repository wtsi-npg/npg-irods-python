# -*- coding: utf-8 -*-
#
# Copyright © 2022, 2023 Genome Research Ltd. All rights reserved.
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

import io
import os
import shlex
import threading
from multiprocessing.pool import ThreadPool
from pathlib import PurePath

import partisan
from partisan.exception import RodsError
from partisan.icommands import icp
from partisan.irods import (
    AC,
    Collection,
    DataObject,
    RodsItem,
    client_pool,
    make_rods_item,
    rods_path_type,
)
from structlog import get_logger

from npg_irods import illumina, ont
from npg_irods.common import AnalysisType, Platform, infer_data_source
from npg_irods.exception import ChecksumError
from npg_irods.metadata.common import (
    DataFile,
    ensure_common_metadata,
    ensure_matching_checksum_metadata,
    has_checksum_metadata,
    has_common_metadata,
    has_complete_checksums,
    has_complete_replicas,
    has_creation_metadata,
    has_matching_checksum_metadata,
    has_matching_checksums,
    has_trimmable_replicas,
    has_type_metadata,
    requires_creation_metadata,
    requires_type_metadata,
    trimmable_replicas,
)
from npg_irods.metadata.lims import (
    ensure_consent_withdrawn,
    has_consent_withdrawn,
    has_consent_withdrawn_metadata,
    has_consent_withdrawn_permissions,
)
from npg_irods.version import version

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
                log.exception(e)
                if print_fail:
                    _print(p, writer)

            return success

        with ThreadPool(num_threads) as tp:
            results = tp.starmap(fn, enumerate(reader))

        return len(results), results.count(True), results.count(False)


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

    - Data object metadata: if all valid replicas have the same checksum and current
      checksum metadata are incorrect, a new AVU is added and any previous metadata
      moved to history.

    The following states are not repaired automatically because they require an
    assessment on which, if any, replicas are correct:

    - The checksums across all valid replicas are not identical.

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
                log.exception(e, item=i)
                if print_fail:
                    _print(p, writer)

            return success, repair

        with ThreadPool(num_threads) as tp:
            results, repaired = zip(*tp.starmap(fn, enumerate(reader)))

        return len(results), repaired.count(True), results.count(False)


def check_replicas(
    reader,
    writer,
    num_replicas=2,
    num_threads=1,
    num_clients=1,
    print_pass=True,
    print_fail=False,
) -> (int, int, int):
    """Read iRODS data objects paths from a file and check that each one has correct
      replicas, printing the results to a writer.

      The conditions for replicas of a data object to be correct are:

      - The data object has the number of replicas expected for its location in the
        iRODS resource tree. This is typically 2 replicas, but some trees may be
        unreplicated, so there will be 1 "replica" in those cases.
      - All replicas must be in the "valid" state.
      - The conditions for correct checksums must apply for all replicas (see
        `check_checksums`).

    Args:
          reader: A file supplying iRODS data object paths to check, one per line.
          writer: A file where checked paths will be written, one per line.
          num_replicas: The number of replicas expected. Defaults to 2.
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
                comp = has_complete_replicas(obj, num_replicas=num_replicas)
                trim = has_trimmable_replicas(obj, num_replicas=num_replicas)

                if comp and not trim:
                    success = True
                    log.info("Replicas are complete", item=i, path=obj)
                    if print_pass:
                        _print(p, writer)
                else:
                    nv = len([r for r in obj.replicas() if r.valid])
                    ni = len([r for r in obj.replicas() if not r.valid])

                    log.warn(
                        "Replicas are incomplete",
                        item=i,
                        path=obj,
                        num_valid=nv,
                        num_invalid=ni,
                        has_compl_replicas=comp,
                        has_trim_replicas=trim,
                        has_compl_checksums=has_complete_checksums(obj),
                        has_match_checksums=has_matching_checksums(obj),
                    )

                    if print_fail:
                        _print(p, writer)

            except RodsError as re:
                log.error(re.message, item=i, code=re.code)
                if print_fail:
                    _print(p, writer)
            except Exception as e:
                log.exception(e, item=i)
                if print_fail:
                    _print(p, writer)

            return success

        with ThreadPool(num_threads) as tp:
            results = tp.starmap(fn, enumerate(reader))

        return len(results), results.count(True), results.count(False)


def repair_replicas(
    reader,
    writer,
    num_replicas=2,
    num_threads=1,
    num_clients=1,
    print_repair=True,
    print_fail=False,
) -> (int, int, int):
    """Read iRODS data object paths from a file and ensure that each one has correct
    replicas by making any necessary repairs, printing the results to a writer.

    The possible repairs are:

    - Invalid replicas: if the data object has invalid replicas, these are trimmed.
      This is the most common type of repair.

    - Valid replicas: if the data object has more valid replicas than the number
      required, the excess replicas are trimmed.

    Args:
        reader: A file supplying iRODS data object paths to repair, one per line.
        writer: A file where repaired paths will be written, one per line.
        num_replicas: The number of replicas expected. Defaults to 2.
        num_threads: The number of Python threads to use. Defaults to 1.
        num_clients: The number of baton clients to use, Defaults to 1.
        print_repair: Print the paths of objects that required repair and were
            repaired successfully. Defaults to True.
        print_fail: Print the paths of objects that required repair and the repair
            failed. Defaults to False.

    Returns:
        A tuple of the number of paths checked, the number of paths with a replica
        repaired and the number of errors (paths with incorrect replicas that could
        not be fixed and/or paths that failed to be fixed because of an exception).
    """
    with client_pool(num_clients) as bp:

        def fn(i: int, path: str) -> (bool, bool):
            success = False
            repair = False

            p = path.strip()
            try:
                obj = DataObject(p, pool=bp)
                comp = has_complete_replicas(obj, num_replicas=num_replicas)
                trim = has_trimmable_replicas(obj, num_replicas=num_replicas)

                if trim:
                    valid, invalid = trimmable_replicas(obj, num_replicas=num_replicas)
                    if valid:
                        nv, ni = obj.trim_replicas(valid=True, invalid=False)
                        log.info(
                            "Trimmed valid replicas",
                            item=i,
                            path=obj,
                            has_compl_replicas=comp,
                            num_trimmed=nv,
                        )
                    if invalid:
                        nv, ni = obj.trim_replicas(valid=False, invalid=True)
                        log.info(
                            "Trimmed invalid replicas",
                            item=i,
                            path=obj,
                            has_compl_replicas=comp,
                            num_trimmed=ni,
                        )

                    repair = success = True
                    if print_repair:
                        _print(p, writer)
                else:
                    success = True
                    log.info(
                        "No replicas to trim",
                        item=i,
                        path=obj,
                        has_compl_checksums=has_complete_checksums(obj),
                        has_match_checksums=has_matching_checksums(obj),
                        has_checksum_meta=has_checksum_metadata(obj),
                        has_compl_replicas=comp,
                        has_trim_replicas=trim,
                    )

            except RodsError as re:
                log.error(re.message, item=i, code=re.code)
                if print_fail:
                    _print(p, writer)
            except Exception as e:
                log.exception(e, item=i)
                if print_fail:
                    _print(p, writer)

            return success, repair

        with ThreadPool(num_threads) as tp:
            results, repaired = zip(*tp.starmap(fn, enumerate(reader)))

        return len(results), repaired.count(True), results.count(False)


def check_common_metadata(
    reader, writer, num_threads=1, num_clients=1, print_pass=True, print_fail=False
) -> (int, int, int):
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
        A tuple of the number of paths checked, the number of paths found to be correct
        and the number of errors (paths with incomplete metadata and/or paths that
        failed to be checked because of an exception).
    """
    with client_pool(num_clients) as bp:

        def fn(i: int, path: str) -> bool:
            success = False

            p = path.strip()
            try:
                obj = DataObject(p, pool=bp)
                if has_common_metadata(obj):
                    log.info("Common metadata complete", item=i, path=obj)
                    success = True
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

            except RodsError as re:
                log.error(re.message, item=i, code=re.code)
                if print_fail:
                    _print(p, writer)
            except Exception as e:
                log.exception(e, item=i)
                if print_fail:
                    _print(p, writer)

            return success

        with ThreadPool(num_threads) as tp:
            results = tp.starmap(fn, enumerate(reader))

        return len(results), results.count(True), results.count(False)


def repair_common_metadata(
    reader,
    writer,
    creator=None,
    num_threads=1,
    num_clients=1,
    print_repair=True,
    print_fail=False,
) -> (int, int, int):
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
        A tuple of the number of paths checked, the number of paths whose metadata
        was repaired and the number of errors (paths with incorrect metadata that
        could not be fixed and/or failed to be fixed because of an exception).
    """
    with client_pool(num_clients) as bp:

        def fn(i: int, path: str) -> (bool, bool):
            success = False
            repair = False

            p = path.strip()
            try:
                obj = DataObject(p, pool=bp)
                if has_common_metadata(obj):
                    log.info("Common metadata complete", item=i, path=obj)
                else:
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

                    if repair := ensure_common_metadata(obj, creator=creator):
                        if print_repair:
                            _print(p, writer)
                success = True
            except RodsError as re:
                log.error(re.message, item=i, code=re.code)
                if print_fail:
                    _print(p, writer)
            except Exception as e:
                log.exception(e, item=i)
                if print_fail:
                    _print(p, writer)

            return success, repair

        with ThreadPool(num_threads) as tp:
            results, repaired = zip(*tp.starmap(fn, enumerate(reader)))

        return len(results), repaired.count(True), results.count(False)


def update_secondary_metadata(
    reader, writer, mlwh_session, print_update=True, print_fail=False
) -> (int, int, int):
    """Update secondary metadata, including access permissions, on specified iRODS
    paths, according to current information in the ML warehouse.

    This function is sequencing platform-agnostic and accepts both collection and data
    object paths. The implementation of the updates varies per-platform and respects
    those limits. That means while you can pass in a mixture of ONT collections and
    Illumina data objects, you will see errors if you e.g. pass in Illumina collections
    because Illumina metadata is managed by data objects.

    Args:
        reader: A file supplying iRODS collection and/or data object paths to update,
            one per line.
        writer: A file where updated paths will be written, one per line.
        mlwh_session: An open SQL session (ML warehouse).
        print_update: Print the paths of objects that required updates and were
            updated successfully. Defaults to True.
        print_fail: Print the paths that required updates where the update failed.
            Defaults to False.

    Returns:
       A tuple of the number of paths checked, the number of paths whose metadata
       were updated and the number of errors (paths that could not be updated and/or
       failed to be updated because of an exception).
    """
    num_processed, num_updated, num_errors = 0, 0, 0

    for i, path in enumerate(reader):
        num_processed += 1
        try:
            p = path.strip()
            rods_item = make_rods_item(p)
            updated = False

            match infer_data_source(p):
                case Platform.ILLUMINA, AnalysisType.NUCLEIC_ACID_SEQUENCING:
                    log.info("Illumina", item=i, path=p)
                    updated = illumina.ensure_secondary_metadata_updated(
                        rods_item, mlwh_session
                    )
                case Platform.OXFORD_NANOPORE_TECHNOLOGIES, AnalysisType.NUCLEIC_ACID_SEQUENCING:
                    log.info("ONT", item=i, path=p)
                    updated = ont.ensure_secondary_metadata_updated(
                        rods_item, mlwh_session
                    )
                case platform, analysis:
                    log.warn(
                        "Unsupported platform/analysis",
                        path=p,
                        platform=platform,
                        analysis=analysis,
                    )

            if updated:
                num_updated += 1
                if print_update:
                    _print(p, writer)

        except RodsError as re:
            num_errors += 1
            log.error(re.message, item=i, code=re.code)
            if print_fail:
                _print(path, writer)
        except Exception as e:
            num_errors += 1
            log.exception(e, item=i)
            if print_fail:
                _print(path, writer)

    return num_processed, num_updated, num_errors


def check_consent_withdrawn(
    reader, writer, print_pass=True, print_fail=False
) -> (int, int, int):
    """Read iRODS data objects paths from a file and check that each one has a state
    consistent with consent having been withdrawn, printing the results to a writer.

    The conditions consistent with consent withdrawn are:

    - The data object has the correct metadata. Either:
        - sample_consent = 0 (data managed by the GAPI codebase)
        - sample_consent_withdrawn = 1 (data managed by the NPG codebase)

    - Read permission for any SequenceScape study iRODS groups (named ss_<Study ID>)
      absent.

    Args:
        reader: A file supplying iRODS data object paths to check, one per line.
        writer: A file where checked paths will be written, one per line.
        print_pass: Print the paths of objects passing the check. Defaults to True.
        print_fail: Print the paths of objects failing the check. Defaults to False.

    Returns:
        A tuple of the number of paths checked, the number of paths found to be correct
        and the number of errors (paths with a state inconsistent with consent withdrawn
        or paths that failed to be checked because of an exception).
    """
    num_processed, num_passed, num_errors = 0, 0, 0

    for i, path in enumerate(reader):
        p = path.strip()
        obj = DataObject(p)
        try:
            if has_consent_withdrawn(obj):
                log.info("Consent is withdrawn", item=i, path=obj)
                num_passed += 1
                if print_pass:
                    _print(p, writer)
            else:
                log.warn(
                    "Consent is not withdrawn",
                    item=i,
                    path=obj,
                    has_withdrawn_meta=has_consent_withdrawn_metadata(obj),
                    has_withdrawn_perm=has_consent_withdrawn_permissions(obj),
                )
                num_errors += 1
                if print_fail:
                    _print(p, writer)
        except RodsError as re:
            log.error(re.message, item=i, code=re.code)
            num_errors += 1
            if print_fail:
                _print(p, writer)
        except Exception as e:
            log.exception(e, item=i)
            num_errors += 1
            if print_fail:
                _print(p, writer)

        num_processed += 1

    return num_processed, num_passed, num_errors


def withdraw_consent(
    reader, writer, print_withdrawn=True, print_fail=False
) -> (int, int, int):
    """Read iRODS data objects paths from a file and update each to a state consistent
    with consent having been withdrawn, printing the results to a writer.

    Args:
        reader: A file supplying iRODS data object paths to update, one per line.
        writer: A file where updated paths will be written, one per line.
        print_withdrawn: Print the paths of objects that were updated. Defaults to True.
        print_fail: Print the paths of objects that failed to be updated. Defaults to
        False.

    Returns:
        A tuple of the number of paths checked, the number of paths updated in line
        with consent withdrawal and the number of errors (paths that could not be
        updated and/or failed to be updated because of an exception).
    """
    num_processed, num_withdrawn, num_errors = 0, 0, 0

    for i, path in enumerate(reader):
        p = path.strip()
        obj = DataObject(p)

        try:
            if has_consent_withdrawn(obj):
                log.info("Consent withdrawn", item=i, path=obj)
            else:
                log.info(
                    "Withdrawing consent",
                    item=i,
                    path=obj,
                    has_withdrawn_meta=has_consent_withdrawn_metadata(obj),
                    has_withdrawn_perm=has_consent_withdrawn_permissions(obj),
                )

                if ensure_consent_withdrawn(obj):
                    num_withdrawn += 1
                    if print_withdrawn:
                        _print(p, writer)

        except RodsError as re:
            log.error(re.message, item=i, code=re.code)
            num_errors += 1
            if print_fail:
                _print(p, writer)
        except Exception as e:
            log.exception(e, item=i)
            num_errors += 1
            if print_fail:
                _print(p, writer)

        num_processed += 1

    return num_processed, num_withdrawn, num_errors


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

        If exist_ok is True, the number copied does not include counts of collections
        and data objects which were not copied because they already existed at the
        destination.
    Raises:
        ChecksumError if checksums are inconsistent.
    """
    if src is None:
        raise ValueError(f"A source iRODS path is required")
    if dest is None:
        raise ValueError(f"A dest iRODS path is required")

    if not isinstance(src, RodsItem):
        src = make_rods_item(src)
    if not src.exists():
        raise ValueError(f"Cannot copy from a non-existent iRODS path '{src}'")

    if not isinstance(dest, RodsItem):
        if rods_path_type(dest):
            # The dest path exists, so make the appropriate object
            dest = make_rods_item(dest)
        elif src.rods_type == Collection:
            # The dest path doesn't exist and the src is a collection, so the dest must
            # also be a collection
            dest = Collection(dest)
        elif src.rods_type == DataObject:
            # The dest path doesn't exist and the src path is a data object, so the dest
            # can be either. In this case, we default to a data object.
            dest = DataObject(dest)
        else:
            raise ValueError(
                f"Invalid iRODS path type combination src: {src}: "
                f"src type: {src.rods_type}, "
                f"dest: {dest}, dest type: {dest.rods_type}"
            )

    if src == dest:
        raise ValueError(f"Cannot copy a path {src} to itself")

    # Allow copying of a collection with renaming i.e. rather than copying into a
    # destination that must exist, we are copying into the destination's parent
    # collection (which must exist), while renaming.
    into = True
    if src.rods_type == Collection and dest.rods_type is None:
        if rods_path_type(dest.path.parent.as_posix()) == Collection:
            into = False

    return _copy(
        src, dest, acl=acl, avu=avu, exist_ok=exist_ok, into=into, recurse=recurse
    )


def _copy(
    src, dest, acl=False, avu=False, into=True, exist_ok=False, recurse=False
) -> (int, int):
    """Implement copy for the public copy function.

    Args:
        src: A DataObject or Collection to copy from.
        dest: A DataObject or Collection to copy to.
        acl: If True, also copy any permissions.
        avu: If True, also copy any metadata.
        into: If True, copy any source collection contents directly into the
            destination, rather than copying the source collection itself into the
            destination (effectively renaming the collection while copying).
        exist_ok: If True, check for existing collections and data objects at the
            destination. If they exist and are identical to what would be the result of
            copying, do not raise an error.
        recurse: If True, recurse into collections when copying.

    Returns:
         A tuple of the number of items (collections and data objects) processed, the
        number of items copied.
    """
    num_processed, num_copied = 0, 0

    def _cp_avu_acl(s, d):
        if avu:
            n = d.add_metadata(*s.metadata())
            log.info(f"Added {n} AVUs", path=d)
        if acl:
            n = d.add_permissions(*s.permissions())
            log.info(f"Added {n} permissions", path=d, perm=s.permissions())

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
        icp(str(s), str(d), verify_checksum=True)
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
        d.create(exist_ok=exist_ok)

        return 1

    match (src.rods_type, dest.rods_type):
        case (partisan.irods.Collection, partisan.irods.DataObject):
            raise ValueError(
                f"Cannot copy a collection {src} into a data object {dest}"
            )

        case (partisan.irods.Collection, partisan.irods.Collection) | (
            partisan.irods.Collection,
            None,
        ):
            coll = Collection(PurePath(dest.path, src.path.name)) if into else dest
            num_processed += 1
            num_copied += _maybe_copy_coll(src, coll)
            _cp_avu_acl(src, coll)

            if recurse:
                for item in src.contents():
                    np, nc = _copy(
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


def write_safe_remove_commands(target, writer: io.TextIOBase):
    """Write safe path removal commands for a target path in iRODS.

    Args:
        target: An iRODS path (str, PurePath or RodsItem) to remove.
        writer: Writer where command strings will be written.
    """

    def _log_print(cmd, path):
        quoted_path = shlex.quote(str(path))
        log.info(f"{cmd} {quoted_path}")
        print(cmd, quoted_path, file=writer)

    if not isinstance(target, RodsItem):
        target = make_rods_item(target)
    if isinstance(target, partisan.irods.DataObject):
        _log_print("irm", target)
    else:
        collections = []
        for item in target.iter_contents(recurse=True):
            if isinstance(item, partisan.irods.DataObject):
                _log_print("irm", item)
            else:
                collections.append(item)
        collections.sort(reverse=True)
        for coll in collections:
            _log_print("irmdir", coll)
        _log_print("irmdir", target)


def write_safe_remove_script(path, root, stop_on_error=True, verbose=False):
    """Write a shell script that will safely and remove a collection and contents (or a
    data object) from iRODS. It will generate irm commands for data objects and irmdir
    commands for collections. None of the commands generated are themselves recursive.

    The generated script may be reviewed for correctness before running on a target
    system. The script uses the interpreter "/bin/bash".

    Args:
        path: The path of the script to be generated. Any existing file at this path will
            be overwritten without warning.
        root: A DataObject, Collection, PurePath or str path to remove.
        stop_on_error: Add "set -e" to the script to stop on the first error.
        verbose: Add "set -x" to the script to echo commands to STDERR as they are run.
    """
    with open(path, "w", encoding="utf-8") as f:
        print(f"#!/bin/bash", file=f)
        print(f"# Generated by npg-irods {version()}", file=f)

        if stop_on_error:
            print("set -e", file=f)
        if verbose:
            print("set -x", file=f)

        write_safe_remove_commands(root, f)
        os.chmod(path, 0o755)
