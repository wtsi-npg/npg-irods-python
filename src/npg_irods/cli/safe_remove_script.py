# -*- coding: utf-8 -*-
#
# Copyright © 2022, 2023, 2024 Genome Research Ltd. All rights reserved.
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
import sys

import structlog
from npg.cli import add_logging_arguments
from npg.log import configure_structlog

from npg_irods import add_appinfo_structlog_processor, version
from npg_irods.utilities import write_safe_remove_script

description = """
Writes a shell script to allow safe recursive deletion of collections and data objects
while avoiding use of `irm -r`.

Use of `irm -r` is dangerous in a production environment because accidentally
introducing a single space could cause unwanted deletion. e.g.

`irm -r /seq/my/bad/data`

versus

`irm -r /seq /my/bad/data`

This tool writes a shell script that uses only non-recursive `irm` commands to remove
data objects and `irmdir` to remove collections. This automates the step of creating
the removal commands, but the script must still be used according to Standard Operating
Procedures e.g. be manually reviewed before use.
"""

parser = argparse.ArgumentParser(
    description=description, formatter_class=argparse.RawDescriptionHelpFormatter
)
add_logging_arguments(parser)
parser.add_argument(
    "target",
    help="Target collection or data object path to remove, recursively if a "
    "collection. Must be an absolute path.",
)
parser.add_argument(
    "-o",
    "--output",
    help="Output filename.",
    type=argparse.FileType("w", encoding="UTF-8"),
    default=sys.stdout,
)
parser.add_argument(
    "--echo-commands",
    help="Create a script that echos each command executed.",
    action="store_true",
)
parser.add_argument(
    "--stop-on-error",
    help="Create a script that will stop on the first error encountered.",
    action="store_true",
)
parser.add_argument(
    "--version", help="Print the version and exit.", action="version", version=version()
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
log = structlog.get_logger("main")


def main():
    try:
        write_safe_remove_script(
            args.output,
            args.target,
            stop_on_error=args.stop_on_error,
            verbose=args.echo_commands,
        )

    except Exception as e:
        log.error(e)
        sys.exit(1)
