# -*- coding: utf-8 -*-
#
# Copyright © 2024 Genome Research Ltd. All rights reserved.
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
from partisan.exception import RodsError
from yattag import indent

from npg_irods import add_appinfo_structlog_processor, version
from npg_irods.html_reports import ont_runs_html_report_this_year

description = """Writes an HTML report summarising data in iRODS.

The reports include HTTP links to data objects and collections in iRODS. The links
are only accessible if the report is rendered by a web server that can access the
relevant iRODS zone. 

Available reports are:

    - ont: Oxford Nanopore sequencing data objects and collections.
    
    A summary of ONT runs for the calendar year to date.

"""

parser = argparse.ArgumentParser(
    description=description,
    formatter_class=argparse.RawDescriptionHelpFormatter,
)
add_logging_arguments(parser)
parser.add_argument(
    "-o",
    "--output",
    help="Output filename.",
    type=argparse.FileType("w", encoding="UTF-8"),
    default=sys.stdout,
)
parser.add_argument(
    "report",
    help="Report type.",
    type=str,
    choices=["ont"],
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
            case "ont":
                doc = ont_runs_html_report_this_year(zone=args.zone)
            case _:
                raise ValueError(f"Invalid HTML report type '{report}'")

        print(indent(doc.getvalue(), indent_text=True), file=args.output)
    except RodsError as re:
        log.error(re.message, code=re.code)
        sys.exit(1)
    except Exception as e:
        log.error(e)
        sys.exit(1)
