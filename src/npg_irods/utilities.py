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

import threading
from multiprocessing.pool import ThreadPool

from partisan.exception import RodsError
from partisan.irods import DataObject, client_pool
from structlog import get_logger

from npg_irods.metadata.common import (
    ensure_common_metadata,
    has_checksum_metadata,
    has_common_metadata,
    has_creation_metadata,
    has_type_metadata,
    requires_creation_metadata,
    requires_type_metadata,
)

log = get_logger(__name__)

print_lock = threading.Lock()


def _print(path, writer):
    with print_lock:
        print(path, file=writer)


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
                    log.info("Common metadata correct", item=i, path=obj)
                    if print_pass:
                        _print(p, writer)
                else:
                    log.info(
                        "Common metadata",
                        item=i,
                        path=obj,
                        req_checksum=requires_creation_metadata(obj),
                        has_checksum=has_checksum_metadata(obj),
                        req_creation=requires_creation_metadata(obj),
                        has_creation=has_creation_metadata(obj),
                        req_type=requires_type_metadata(obj),
                        has_type=has_type_metadata(obj),
                    )

                    if print_fail:
                        _print(p, writer)

                success = True

            except RodsError as re:
                log.error(re.message, code=re.code)  # Much more informative diagnostic
                if print_fail:
                    _print(p, writer)
            except Exception as e:
                log.error(e)
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
                        "Common metadata",
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
                log.error(re.message, code=re.code)  # Much more informative diagnostic
                if print_fail:
                    _print(p, writer)
            except Exception as e:
                log.error(e)
                if print_fail:
                    _print(p, writer)

            return success

        with ThreadPool(num_threads) as tp:
            succeeded = tp.starmap(fn, enumerate(reader))

        return all(succeeded)
