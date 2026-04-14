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
# @author Keith James <kdj@sanger.ac.uk>

import argparse
import json
import sys
from pathlib import Path
from typing import Callable

import structlog
from npg.cli import add_logging_arguments, integer_in_range
from npg.log import configure_structlog
from partisan.irods import AC, AVU, Permission, current_user

from npg_irods import add_appinfo_structlog_processor
from npg_irods.common import infer_zone
from npg_irods.functions import make_path_filter
from npg_irods.publish import publish_directory
from npg_irods.utilities import read_md5_file, read_md5sums_file

description = """
A utility to (recursively) publish a local directory to iRODS, retaining the directory
structure.
"""


parser = argparse.ArgumentParser(
    description=description, formatter_class=argparse.RawDescriptionHelpFormatter
)
add_logging_arguments(parser)

parser.add_argument(
    "directory",
    help="The local directory to publish to iRODS.",
    type=str,
)
parser.add_argument(
    "collection",
    help="The iRODS collection to publish the local directory to.",
    type=str,
)
parser.add_argument(
    "--exclude",
    help="Exclude paths matching the given regular expression. May be used "
    "multiple times to filter on additional regular expressions. Exclude "
    "regular expressions are applied after any include regular expressions. "
    "Paths will be absolute or relative depending on whether directory is an "
    "absolute or relative path. "
    "Optional, defaults to none.",
    type=str,
    action="append",
    default=[],
)

parser.add_argument(
    "--include",
    help="Include paths matching the given regular expression. Only matching "
    "paths will be published, all others will be ignored. If more than one "
    "regex is supplied, the matches for all of them are aggregated. "
    "Paths will be absolute or relative depending on whether directory is an "
    "absolute or relative path. "
    "Optional, defaults to all.",
    type=str,
    action="append",
    default=[],
)


# Provide convenience method for common use case of --exclude.
# We often need to apply permissions and metadata to the files in root of a run
# differently from sample folders.
# We had several bugs in ultimagen_publish_to_irods.py from rnd_platforms
# due to issues filtering to just top level files using approach of excluding
# sample folders with a regular expression.
parser.add_argument(
    "--include-top-level-files",
    help="Include top level files. Composes with other filters.",
    action="store_true",
)

# Provide convenience method for common use case of --exclude.
parser.add_argument(
    "--exclude-md5",
    help="Exclude md5 files. Composes with other filters.",
    action="store_true",
)

ff_group = parser.add_mutually_exclusive_group(required=False)
ff_group.add_argument(
    "--fill",
    help="Fill missing data objects and those with mismatched checksums. "
    "Incompatible with --force.",
    action="store_true",
)
ff_group.add_argument(
    "--force",
    help="Force the update of existing data objects. Incompatible with --fill.",
    action="store_true",
)

parser.add_argument(
    "--group",
    help="iRODS group to have read access. Optional, defaults to none. "
    "May be used multiple times to add read permissions for multiple groups. "
    "Zone may be specified with #zone suffix, otherwise inferred from collection.",
    type=str,
    action="append",
    default=[],
)
parser.add_argument(
    "--metadata-file",
    help="Path to a JSON file containing metadata to add to the published "
    "root collection. The JSON must describe the metadata in baton syntax "
    '(an array of AVUs): E.g. [{"attribute": "attr1", "value": "val1"}]. '
    "Optional, defaults to none.",
    type=argparse.FileType("r", encoding="UTF-8"),
    default=None,
)
checksums_group = parser.add_mutually_exclusive_group(required=False)
checksums_group.add_argument(
    "--use-checksum-files",
    help="Expect checksum files to be present alongside the data files with "
    "the same name as the data file but with an additional '.md5' extension"
    "e.g. 'data.txt' and 'data.txt.md5'. Each checksum file should contain only "
    "the single MD5 checksum of the corresponding data file. This avoids having "
    "to calculate the checksums during the publish process. If this option is "
    "enabled and a checksum file cannot be read, an error will be raised for "
    "that file. Optional, defaults to false.",
    action="store_true",
)
checksums_group.add_argument(
    "--use-checksums-file",
    help="Expect checksums to be present in a checksums file at path specified "
    "following GNU coreutils md5sum format. This avoids having to calculate the "
    "checksums during the publish process. If this option is enabled and a "
    "checksum is missing or stale, an error will be raised for that file. "
    "Optional, defaults to none.",
    type=str,
    default=None,
)
parser.add_argument(
    "--num-clients",
    help="Number of iRODS clients to use for the operation, maximum 24. "
    "Optional, defaults to 4.",
    type=integer_in_range(1, 24),
    default=4,
)


def _parse_group(group: str) -> tuple[str, str | None]:
    name, zone = (group.split("#", maxsplit=1) + [None])[:2]
    return name, zone


def make_get_checksum(md5sums_path: Path) -> Callable[[Path | str], str]:
    md5sums = read_md5sums_file(md5sums_path)
    md5sums_modified = md5sums_path.stat().st_mtime

    def get_checksum(path: Path | str) -> str:
        path = Path(path) if isinstance(path, str) else path
        path = path.resolve()
        checksum = md5sums.get(path)
        if not checksum:
            raise ValueError(f"No checksum found for {path}")
        path_modified = path.stat().st_mtime
        if path_modified > md5sums_modified:
            raise ValueError(
                f"Checksum for {path} may be out of date, file modified ({path_modified}) more recently than {md5sums_path} ({md5sums_modified})"
            )
        return checksum

    return get_checksum


def main():
    args = parser.parse_args()
    configure_structlog(
        config_file=args.log_config,
        debug=args.debug,
        verbose=args.verbose,
        colour=args.colour,
        json=args.log_json,
    )
    add_appinfo_structlog_processor()
    log = structlog.get_logger("main")

    num_clients = args.num_clients
    inferred_zone = infer_zone(args.collection)
    groups = [_parse_group(group) for group in args.group]
    acl = [
        AC(name, Permission.READ, zone=zone if zone else inferred_zone)
        for name, zone in groups
    ]

    avus = []
    if args.metadata_file is not None:
        f = args.metadata_file
        try:
            meta = json.load(f)
            if not isinstance(meta, list):
                raise ValueError("Expected a JSON array")

            for item in meta:
                if not isinstance(item, dict):
                    raise ValueError(f"Expected a JSON object at: {item}")

                if "attribute" in item:
                    attr = item["attribute"]
                elif "a" in item:
                    attr = item["a"]
                else:
                    raise ValueError(f"Missing 'attribute' at: {item}")

                if "value" in item:
                    value = item["value"]
                elif "v" in item:
                    value = item["v"]
                else:
                    raise ValueError(f"Missing 'value' at: {item}")

                if "units" in item:
                    units = item["units"]
                elif "u" in item:
                    units = item["u"]
                else:
                    units = None

                avus.append(AVU(attr, value, units))
        except Exception as e:
            log.error(
                "Failed to read JSON from metadata file", path=f.name, error=str(e)
            )
            raise e

    log.info(
        "Publishing directory",
        src=args.directory,
        dest=args.collection,
        fill=args.fill,
        force=args.force,
        num_clients=num_clients,
    )

    filter_fn = (
        make_path_filter(
            include_patterns=args.include,
            exclude_patterns=args.exclude,
            include_top_level_files=args.include_top_level_files,
            exclude_md5=args.exclude_md5,
            src=Path(args.directory),
        )
        if args.exclude
        or args.include
        or args.include_top_level_files
        or args.exclude_md5
        else None
    )

    checksum_fn: Callable[[Path | str], str] | None
    if args.use_checksum_files:
        checksum_fn = read_md5_file
    elif args.use_checksums_file:
        try:
            checksum_fn = make_get_checksum(Path(args.use_checksums_file))
        except Exception as e:
            log.error(
                "Failed to read checksums file",
                path=args.use_checksums_file,
                error=str(e),
            )
            raise e
    else:
        checksum_fn = None

    num_items, num_processed, num_errors = publish_directory(
        args.directory,
        args.collection,
        avus=avus,
        acl=acl,
        filter_fn=filter_fn,
        local_checksum=checksum_fn,
        fill=args.fill,
        force=args.force,
        handle_exceptions=True,
        num_clients=num_clients,
    )

    if num_errors > 0:
        log.error(
            "Processed some items with errors",
            num_items=num_items,
            num_processed=num_processed,
            num_errors=num_errors,
        )
        sys.exit(1)

    log.info(
        "Processed all items successfully",
        num_items=num_items,
        num_processed=num_processed,
        num_errors=num_errors,
    )


if __name__ == "__main__":
    main()
