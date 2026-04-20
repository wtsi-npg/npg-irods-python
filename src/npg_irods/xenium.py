# -*- coding: utf-8 -*-
#
# Copyright © 2026 Genome Research Ltd. All rights reserved.
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

import json
from pathlib import Path, PurePath

from partisan.irods import AVU, Collection
from structlog import get_logger

from npg_irods.common import PlatformNamespace
from npg_irods.exception import PublishingError
from npg_irods.metadata.xenium import EXPERIMENT_FILENAME, Instrument
from npg_irods.publish import publish_directory
from npg_irods.utilities import sanitise_path

log = get_logger(__name__)


def is_output_directory(result_dir: Path) -> bool:
    """Return True if a directory is a Xenium result directory. These are identified
    by the presence of an 'experiment.xenium' file.

    Note that a Xenium run can comprise multiple directories as there will be one for
    each region of interest in the analysis.
    """
    d = result_dir.resolve()
    metadata_file = d / EXPERIMENT_FILENAME

    accept = d.is_dir() and metadata_file.is_file()
    log.debug(
        "Xenium result directory check",
        path=d.as_posix(),
        path_is_dir=d.is_dir(),
        metadata_path=metadata_file.as_posix(),
        metadata_is_file=metadata_file.is_file(),
        accept=accept,
    )

    return accept


def make_xenium_metadata(result_dir: Path) -> list[AVU]:
    """Create iRODS metadata for a Xenium result directory.

    Args:
        result_dir: Path to the Xenium result directory.

    Returns:
        Metadata derived from the directory's `experiment.xenium` file.
    """

    d = result_dir.resolve()
    return [
        AVU(k, v).with_namespace(PlatformNamespace.XENIUM)
        for k, v in _load_metadata(d).items()
        if k in Instrument
    ]


def publish_result_dirs(
    reader, writer, remote_root: PurePath, print_success=True, print_fail=False
):
    """Read local Xenium result directory paths from a reader and publish their contents
    to iRODS, printing the results to a writer.

    The data have spatial transcriptomic information, but they don’t have any sensitive
    information, they aren’t raw reads, so technically this is in the realm of count
    matrices regarding data protection.

    See https://www.10xgenomics.com/support/software/xenium-onboard-analysis/latest/analysis/xoa-output-understanding-outputs#protein-data


    Args:
        reader: A file supplying Xenium result directory paths, one per line.
        writer: A file to which to write the paths of successfully published directories.
        remote_root: The iRODS collection to which to publish the directories.
        print_success: Print the paths of successfully published directories. Defaults
            to True.
        print_fail: Print the paths of directories that failed to publish. Defaults
            to False.

    Returns:
        A tuple of the number of directories processed, the number successfully
        published, and the number that failed to publish.

    """
    num_dirs, num_published, number_failed = 0, 0, 0

    for path in reader:
        p = Path(sanitise_path(path)).resolve()

        num_dirs += 1
        try:
            publish_result_dir(p, remote_root)

            if print_success:
                num_published += 1
                print(p, file=writer)
        except Exception as e:
            log.error(f"Failed to publish '{p}': {e}")
            if print_fail:
                number_failed += 1
                print(p, file=writer)

    return num_dirs, num_published, number_failed


def publish_result_dir(
    result_dir: Path, remote_root: PurePath, tries: int = 3
) -> Collection:
    """Publish one Xenium results directory to iRODS.

    Args:
        result_dir: Path to the Xenium result directory.
        remote_root: iRODS path to the root of the Xenium results collection. This
            collection must exist.
        tries: Number of times to retry publishing if it fails.

    Returns:
        The iRODS collection containing the published results.
    """

    if not Collection(remote_root).exists():
        raise ValueError(f"Remote root collection '{remote_root}' does not exist")

    src = result_dir.resolve()
    dest = remote_root / _irods_partial_path(src)
    avus = make_xenium_metadata(src)

    log.info(
        "Publishing Xenium result",
        src=src.as_posix(),
        dest=dest.as_posix(),
        metadata=avus,
    )

    def filter_item(item: Path) -> bool:
        """Filter out symlinks and non-files/directories."""
        return item.is_symlink() or not (item.is_file() or item.is_dir())

    num_items, num_processed, num_errors = publish_directory(
        src,
        dest,
        avus=avus,
        filter_fn=filter_item,
        force=True,
        fill=True,
        num_clients=4,
        tries=tries,
    )

    if num_errors > 0:
        raise PublishingError(
            src=src.as_posix(),
            dest=dest.as_posix(),
            num_processed=num_processed,
            num_errors=num_errors,
        )

    published = Collection(dest)

    # Here is where we would normally apply study-level permissions.

    log.info(
        "Publishing complete",
        src=src.as_posix(),
        dest=dest.as_posix(),
        num_processed=num_processed,
        num_errors=num_errors,
    )

    return published


def _load_metadata(result_dir: Path) -> dict[str, str]:
    """Load experiment metadata from the 'experiment.xenium' file in the given Xenium
    result directory."""
    metadata_file = result_dir.resolve() / EXPERIMENT_FILENAME
    with open(metadata_file, "r") as f:
        metadata = json.load(f)

    return metadata


def _irods_partial_path(result_dir: Path) -> PurePath:
    """Given a Xenium result directory, return a relative iRODS path that uses our
    standard path structure for storing Xenium data.

    We are using slide_id in the path, rather than run_name, because the former is
    machine-readable while the latter is keyed in by hand on the instrument.

    """
    d = result_dir.resolve()
    if not is_output_directory(d):
        raise ValueError(f"Path '{d.as_posix()}' is not a Xenium result directory")

    metadata = _load_metadata(d)

    try:
        instrument = metadata[Instrument.INSTRUMENT_SN.value]
        slide_id = metadata[Instrument.SLIDE_ID.value]
    except KeyError as e:
        raise ValueError(
            f"Missing required metadata key: '{e}' from "
            f"experiment.xenium file in {d.as_posix()}"
        ) from e

    return PurePath(instrument, slide_id, d.name)
