#!/usr/bin/env python3
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
# @author Michael Kubiak <mk35@sanger.ac.uk>

import argparse
import logging
import structlog
from math import floor

parser = argparse.ArgumentParser(
    description="A script to backfill the iRODS location mlwh table for a set of run ids"
)

parser.add_argument("run_ids", nargs="+", type=str, help="A list of runs to load")

parser.add_argument(
    "-n",
    "--novaseq",
    action="store_true",
    help="A flag to use NovaSeq paths (/seq/illumina/runs/...)",
)

parser.add_argument(
    "-p",
    "--processes",
    type=int,
    default=1,
    help="The number of processes to run (each process uses a maximum of 1 baton process)",
)

parser.add_argument(
    "-j",
    "--json",
    type=str,
    default="out.json",
    help="The output filename, defaults to out.json",
)

parser.add_argument(
    "-v", "--verbose", action="store_true", help="Enable INFO level logging"
)

args = parser.parse_args()

logging_level = logging.WARN
if args.verbose:
    logging_level = logging.INFO

logging.basicConfig(level=logging_level, encoding="utf-8")

structlog.configure(logger_factory=structlog.stdlib.LoggerFactory())

log = structlog.get_logger(__file__)

from npg_irods.mlwh_locations.illumina import generate_files


def main():

    if args.novaseq:
        colls = [
            f"/seq/illumina/runs/{str(floor(int(run_id)/1000))}/{str(run_id)}"
            for run_id in args.run_ids
        ]
    else:
        colls = [f"/seq/{str(run_id)}" for run_id in args.run_ids]

    generate_files(colls, args.processes, args.json)


if __name__ == "__main__":
    main()
