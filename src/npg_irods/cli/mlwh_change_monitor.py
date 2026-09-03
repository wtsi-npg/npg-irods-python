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

import argparse
from pathlib import Path

import sqlalchemy
import structlog
from npg.cli import (
    add_date_range_arguments,
    add_db_config_arguments,
    add_logging_arguments,
)
from npg.conf import IniData
from npg.log import configure_structlog
from sqlalchemy.orm import Session

from npg_irods import db
from npg_irods.db.mlwh_cache import MlwhChangeCache

description = """
A utility that monitors the MLWH for changes to the sample and study tables in order
to record the number of updates that include substantive data changes and those that
only change the 'recorded_at' timestamp.
"""


def logger():
    return structlog.get_logger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description=description, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser = add_logging_arguments(parser)
    parser = add_db_config_arguments(parser)

    add_date_range_arguments(parser)  # This should return the parser, but doesn't

    parser.add_argument(
        "--mlwh-cache",
        "--mlwh_cache",
        help="Path to a SQLite cache used to filter Sample/Study updates by content.",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--prime-mlwh-cache",
        "--prime_mlwh_cache",
        help="Prime the MLWH cache with all Sample/Study rows before filtering.",
        action="store_true",
    )

    args = parser.parse_args()

    configure_structlog(
        config_file=args.log_config,
        debug=args.debug,
        verbose=args.verbose,
        colour=args.colour,
        json=args.log_json,
    )

    dbconfig = IniData(db.Config).from_file(args.db_config, "mlwh_ro")
    engine = sqlalchemy.create_engine(
        dbconfig.url, pool_pre_ping=True, pool_recycle=3600
    )

    cache_path = Path(args.mlwh_cache).resolve()
    begin = args.begin_date
    end = args.end_date

    with Session(engine) as sess:
        with MlwhChangeCache(
            path=cache_path, hash_schema_version=1, prime_cache=args.prime_mlwh_cache
        ) as cache:
            samples = cache.changed_sample_keys(sess, since=begin, until=end)
            logger().info("Samples changed", num=len(samples), begin=begin, end=end)
            for i, sample in enumerate(samples):
                print(f"sample {i}\t{sample}")

            studies = cache.changed_study_keys(sess, since=begin, until=end)
            logger().info("Studies changed", num=len(studies), begin=begin, end=end)
            for i, study in enumerate(studies):
                print(f"study {i}\t{study}")
