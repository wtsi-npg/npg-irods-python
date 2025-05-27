# -*- coding: utf-8 -*-
#
# Copyright © 2025 Genome Research Ltd. All rights reserved.
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

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path, PurePath

from partisan.irods import AC, AVU, Collection, DataObject, client_pool
from structlog import get_logger

from npg_irods.common import update_metadata, update_permissions
from npg_irods.exception import PublishingError
from npg_irods.metadata.common import ensure_common_metadata


log = get_logger(__name__)


def publish_directory(
    src: Path | str,
    dest: PurePath | str,
    avus: list[AVU] = None,
    acl: list[AC] = None,
    filter_fn=None,
    local_checksum=None,
    fill=False,
    force=False,
    handle_exceptions=True,
    num_clients: int = 1,
    timeout=None,
    tries=1,
):
    """Publish a directory and its contents to iRODS.

    Args:
        src: The local source directory path to publish.
        dest: The remote iRODS collection path to publish into. This collection must
            exist; the contents of the source directory will be published into it.
        avus: A list of AVU objects to add to each of the published root collection. The
            default is None, meaning no AVUs are added.
        acl: A list of AC objects to add to each of the published items. The default is
            None, meaning no ACL is added.
        filter_fn: A filter function to apply to each item in the source directory. The
            function should take a single argument (the item) and return True if the
            item should be filtered out, or False if it should be included. The default
            is None, meaning no items are filtered.
        local_checksum: A callable that returns a checksum for a local file. Optional,
            if None, each local file's MD5 checksum will be calculated. If local files
            are large, it may improve the publishing time to use this argument to
            supply pre-caculated checksums. The callable should take a single argument
            (the file path) and return the checksum as a string.
        fill: Fill in missing data objects in iRODS. If a data object already
            exists, the operation is skipped for that path. See DataObject.put() for
            more information. The default is False.
        force: If a data object is written, overwrite any data object already present
            in iRODS. The default is True.
        handle_exceptions: Report a count of any errors encountered during publishing,
            rather than raising an exception. The default is True. If False and one or
            more errors are encountered, a PublishingError is raised from the first
            error encountered.
        num_clients: Number of iRODS clients to use for the operation. The default is 1.
        timeout: Operation timeout in seconds.
        tries: Number of times to try the operation.

    Returns:
        A tuple containing the number of items processed, the number of items
        successfully published, and the number of errors encountered.
    """
    num_items, num_processed, num_errors = 0, 0, 0
    first_error: Exception | None = None

    if filter_fn is None:
        filter_fn = lambda _: False

    if num_clients is None:
        num_clients = 1
    if num_clients < 1:
        raise ValueError("num_clients must be at least 1")
    if num_clients > 24:
        raise ValueError("num_clients must be at most 24")

    num_threads = num_clients + int(num_clients / 4) + 1

    with client_pool(maxsize=num_clients) as bp:
        coll = Collection(dest, pool=bp)

        with ThreadPoolExecutor(
            thread_name_prefix="npg-irods-python.publish", max_workers=num_threads
        ) as executor:
            perm_updates = []
            meta_updates = []

            for item in coll.put(
                src,
                recurse=True,
                verify_checksum=True,
                compare_checksums=True,
                local_checksum=local_checksum,
                fill=fill,
                filter_fn=filter_fn,
                force=force,
                yield_exceptions=True,
                timeout=timeout,
                tries=tries,
            ):
                num_items += 1

                match item:
                    case Exception():
                        num_errors += 1
                        log.error("Error publishing item", error=str(item))
                        if first_error is None:
                            first_error = item
                        continue
                    case Collection():
                        num_processed += 1
                    case DataObject():
                        meta_updates.append(
                            executor.submit(ensure_common_metadata, item)
                        )
                        num_processed += 1
                    case _:
                        num_errors += 1
                        log.error("Unknown item type", path=item)
                        continue

                if acl is not None:
                    perm_updates.append(
                        executor.submit(update_permissions, item, acl=acl)
                    )

            for future in meta_updates:
                try:
                    future.result()
                except Exception as e:
                    num_errors += 1
                    log.error("Error updating data object metadata", error=str(e))
                    if first_error is None:
                        first_error = e

            for future in perm_updates:
                try:
                    future.result()
                except Exception as e:
                    num_errors += 1
                    log.error("Error updating permissions", error=str(e))
                    if first_error is None:
                        first_error = e

        if avus is not None:
            try:
                update_metadata(coll, avus)
            except Exception as e:
                num_errors += 1
                log.error("Error updating collection metadata", path=coll, error=str(e))
                if first_error is None:
                    first_error = e

    if not handle_exceptions and num_errors > 0:
        err = PublishingError(
            "Error while publishing",
            src=src,
            dest=dest,
            num_processed=num_processed,
            num_errors=num_errors,
        )
        if first_error is None:
            raise err

        raise err from first_error

    return num_items, num_processed, num_errors
