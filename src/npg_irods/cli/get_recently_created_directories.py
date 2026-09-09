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
# @author Calum Eadie <ce10@sanger.ac.uk>
import sys
from datetime import datetime, timedelta, UTC

import operator
from typing import BinaryIO, TextIO, Any

from npg_irods.system_calls import get_now_utc, get_ctime

from pathlib import Path
import argparse
import structlog
from npg.cli import (
    add_logging_arguments,
    open_output,
    open_input,
    add_io_arguments,
    parse_iso_date,
)
from npg.log import configure_structlog
from npg_irods import (
    add_appinfo_structlog_processor,
    version,
    parse_timedelta_from_hours,
)
from npg_irods.utilities import sanitise_path

description = """
Filters a list of directories to those recently created.

Reads directory paths from a file or STDIN, one per line, filters and writes
directory paths to a file or STDOUT, one per line.

Considers all files at any depth below directory.

Compares by ctime. Tool is only applicable to filesystems where
ctime is a creation time (i.e. some NFS filesystems depending on configuration) and
not the last metadata change (i.e. a typical Unix filesystem).   

Directories with "too recent" changes can be excluded. For example, to
heuristically guard against in progress transfers. 

Directories with "late" changes can be excluded. For example, for Xenium
publishing, we need to find directories recently copied from instrument to a
team NFS area. We need to be aware of any later modifications to those directories
and exclude from automatic publishing.
"""

epilog = """
notes:
  Error Handling: Continues to next directory on error.
  Symbolic Links: Follows file links. Does not follow directory links (to avoid filesystem loops).
  Exclusions: Excludes checksums (.md5) and macOS Finder metadata (.DS_Store) files.
  
history:
  We have previously created systems that find recently modified directories, for
  example in publishing Fluidigm and BioNano.
  For Xenium, when operators copy Xenium outputs from instrument onto NFS, instrument
  mtime is preserved whereas ctime is the time files were created on NFS.
"""


def logger():
    return structlog.get_logger(__name__)


def get_recently_created_directories(
    reader: BinaryIO | TextIO | Any,
    writer: BinaryIO | TextIO | Any,
    begin: datetime,
    end: datetime,
    max_creation_period: timedelta,
) -> tuple[int, int, int]:
    num_dirs, num_recent, num_errors = 0, 0, 0

    for line in reader:
        try:
            num_dirs += 1
            directory_path = Path(sanitise_path(line))

            ctimes: dict[Path, datetime] = {}

            for file_path in directory_path.rglob("*"):
                if not file_path.is_file():
                    continue

                if file_path.suffix.lower() == ".md5" or file_path.name == ".DS_Store":
                    continue

                # Python is moving towards depreciating ctime and making birthtime available.
                # Reading from an NFS drive that's configured so ctime is creation time
                # from Python running on Linux is a corner case.
                # st_ctime depreciated on windows in favour of st_birthtime.
                # st_ctime documented as time of most recent metadata change but
                # different in this corner case.
                # See https://docs.python.org/3/library/os.html#os.stat_result.st_ctime.
                ctimes[file_path] = datetime.fromtimestamp(get_ctime(file_path), UTC)

            if not ctimes:
                num_errors += 1
                logger().warning(
                    "Unexpected empty directory.",
                    directory=directory_path,
                )
                continue

            earliest_ctime_path, earliest_ctime_date = min(
                ctimes.items(), key=operator.itemgetter(1)
            )
            latest_ctime_path, latest_ctime_date = max(
                ctimes.items(), key=operator.itemgetter(1)
            )

            too_old = latest_ctime_date < begin
            if too_old:
                logger().debug(
                    "Filtered out: too old. Latest ctime before beginning of recent creation window.",
                    directory=directory_path,
                    begin=begin,
                    latest_ctime_path=latest_ctime_path,
                    latest_ctime_date=latest_ctime_date,
                )
                continue

            too_new = latest_ctime_date > end
            if too_new:
                logger().info(
                    "Filtered out: too new (avoid in progress). Latest ctime after end of recent creation window.",
                    directory=directory_path,
                    begin=begin,
                    latest_ctime_path=latest_ctime_path,
                    latest_ctime_date=latest_ctime_date,
                )
                continue

            creation_period = latest_ctime_date - earliest_ctime_date
            if creation_period > max_creation_period:
                logger().warning(
                    "Unexpected later change to file",
                    directory=directory_path,
                    creation_period=creation_period,
                    earliest_ctime_path=earliest_ctime_path,
                    earliest_ctime_date=earliest_ctime_date,
                    latest_ctime_path=latest_ctime_path,
                    latest_ctime_date=latest_ctime_date,
                )
                num_errors += 1
                continue

            num_recent += 1
            print(directory_path, file=writer)
            logger().debug(
                "Filtered in.",
                directory=directory_path,
                begin=begin,
                end=end,
                creation_period=creation_period,
                latest_ctime_path=latest_ctime_path,
                latest_ctime_date=latest_ctime_date,
            )
        except Exception as e:
            num_errors += 1
            logger().exception("Could not filter directory", line=line, error=e)
            continue

    return num_dirs, num_recent, num_errors


def main():
    parser = argparse.ArgumentParser(
        description=description, formatter_class=argparse.RawDescriptionHelpFormatter
    )

    add_io_arguments(parser)

    add_logging_arguments(parser)

    # We need to deviate from npg.cli.add_date_range_arguments
    begin_delta_days = 7
    parser.add_argument(
        "--begin-date",
        help="Limit to after this date. Defaults to 7 days ago. The argument must "
        "be an ISO8601 UTC date or date and time "
        "e.g. 2022-01-30, 2022-01-30T11:11:03Z",
        type=parse_iso_date,
        default=get_now_utc() - timedelta(days=begin_delta_days),
    )
    end_delta_hours = 1
    parser.add_argument(
        "--end-date",
        help="Limit to before this date. Defaults to 1 hour ago providing a "
        "level of mitigation against in progress uploads. The argument must "
        "be an ISO8601 UTC date or date and time "
        "e.g. 2022-01-30, 2022-01-30T11:11:03Z",
        type=parse_iso_date,
        default=get_now_utc() - timedelta(hours=end_delta_hours),
    )

    parser.add_argument(
        "--max-creation-period",
        help="Filter out and warn about directories where the period between "
        "the earliest and latest file creation exceeds a specified period. "
        "Defaults to 6 hours. The argument must be an integer number of hours.",
        type=parse_timedelta_from_hours,
        default=timedelta(hours=6),
    )

    parser.add_argument(
        "--version",
        help="Print the version and exit.",
        action="version",
        version=version(),
    )

    args = parser.parse_args()
    configure_structlog(
        config_file=args.log_config,
        debug=args.debug,
        verbose=args.verbose,
        colour=args.colour,
        json=args.log_json,
    )
    add_appinfo_structlog_processor()

    input_path = sanitise_path(args.input)
    output_path = sanitise_path(args.output)

    logger().info("Getting recently created directories")

    begin: datetime = args.begin_date
    end: datetime = args.end_date
    max_creation_period: timedelta = args.max_creation_period

    logger().debug("Filtering", begin=begin, end=end)

    with open_input(input_path, encoding="utf-8") as reader:
        with open_output(output_path, encoding="utf-8") as writer:
            num_dirs, num_recent, num_errors = get_recently_created_directories(
                reader,
                writer,
                begin=begin,
                end=end,
                max_creation_period=max_creation_period,
            )

    if num_errors:
        logger().error(
            "Some errors whilst getting recently created directories",
            num_dirs=num_dirs,
            num_recent=num_recent,
            num_errors=num_errors,
        )
        sys.exit(1)

    logger().info(
        "Got recently created directories",
        num_dirs=num_dirs,
        num_recent=num_recent,
        num_errors=num_errors,
    )


if __name__ == "__main__":
    main()
