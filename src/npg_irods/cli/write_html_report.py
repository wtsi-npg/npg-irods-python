# -*- coding: utf-8 -*-
#
# Copyright © 2024, 2025 Genome Research Ltd. All rights reserved.
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
from pathlib import PurePath

import structlog
from npg.cli import add_logging_arguments
from npg.log import configure_structlog
from partisan.exception import RodsError
from yattag import indent

from npg_irods import add_appinfo_structlog_processor, version
from npg_irods.common import PlatformNamespace
from npg_irods.html_reports import ont_runs_html_report_this_year, publish_report

description = """Writes an HTML report summarising data in iRODS.

The reports include HTTP links to data objects and collections in iRODS. The links
are only accessible if the report is rendered by a web server that can access the
relevant iRODS zone.

If the `--publish` option is used, the report will be written to iRODS with metadata
that will allow it to be indexed by a Sqyrrl server. If the path exists, it will be
overwritten.

Available reports are:

    - ont: Oxford Nanopore Technology sequencing data objects and collections.
    
    A summary of ONT runs for the calendar year to date.

"""

parser = argparse.ArgumentParser(
    description=description,
    formatter_class=argparse.RawDescriptionHelpFormatter,
)
add_logging_arguments(parser)
outputs = parser.add_mutually_exclusive_group(required=True)

outputs.add_argument(
    "-o",
    "--output",
    help="Write the report to a local files. Default is STDOUT.",
    type=argparse.FileType("w", encoding="UTF-8"),
    default=sys.stdout,
)
outputs.add_argument(
    "-p",
    "--publish",
    help="Publish the report to iRODS. The report will be written to the "
    "specified path in iRODS with metadata that will allow it to be indexed by a "
    "Sqyrrl server. If the path exists, it "
    "will be overwritten.",
    type=str,
)
parser.add_argument(
    "report",
    help="Report type.",
    type=str,
    choices=[PlatformNamespace.OXFORD_NANOPORE_TECHNOLOGIES],
    nargs=1,
)
parser.add_argument(
    "--zone",
    help="Specify a federated iRODS zone in which to find data objects and/or "
    "collections. This is not required if the target paths are on the local zone.",
    type=str,
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
    report = args.report[0]

    try:
        match report:
            case PlatformNamespace.OXFORD_NANOPORE_TECHNOLOGIES:
                doc = ont_runs_html_report_this_year(zone=args.zone)
            case _:
                raise ValueError(f"Invalid HTML report type '{report}'")

        if args.publish:
            dest = PurePath(args.publish)
            category = PlatformNamespace.OXFORD_NANOPORE_TECHNOLOGIES

            obj = publish_report(doc, dest, category=category)
            log.info("Published report to iRODS", path=obj.path, category=category)
        else:
            print(indent(doc.getvalue(), indent_text=True), file=args.output)

    except RodsError as re:
        log.error(re.message, code=re.code)
        sys.exit(1)
    except Exception as e:
        log.error(e)
        sys.exit(1)
