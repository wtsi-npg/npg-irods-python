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

import argparse
from itertools import starmap
import sys

from sqlalchemy import Engine
import sqlalchemy
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
import structlog
from npg.cli import add_db_config_arguments, add_io_arguments, add_logging_arguments
from npg.conf import IniData
from npg.log import configure_structlog

from npg_irods import add_appinfo_structlog_processor, db, version

from npg_irods.common import update_metadata
from npg_irods.db.mlwh import Sample, session_context

from partisan.irods import make_rods_item, RodsError

from npg_irods.metadata.common import avu_if_value
from npg_irods.metadata.lims import TrackedSample

description = """
Add sample_lims and sample_uuid given a DataObject or Collection in input.
The AVU is retrieved from the MLWH and added only if it is not present. 
In particular, the metadata update is skipped when:
    - Both AVUs are present
    - One of the AVUs has NULL value in MLWH
    - No sample_id has been found on the iRODS path
    - No sample_id record is found in MLWH
    - Multiple records have been found in MLWH with the sample_id from iRODS metadata
"""


log = structlog.get_logger("main")


def add_lims_uuid_from_input(engine: Engine, input):
    num_updated = 0
    num_skipped = 0
    num_failed = 0
    with session_context(engine) as mlwh_session:
        for path in input:
            try:
                iobj = make_rods_item(path.strip())
                if not iobj.has_metadata_attrs(TrackedSample.ID):
                    msg = f"No Sample ID attribute ({TrackedSample.ID}) found on {iobj}"
                    log.info(msg)
                    num_skipped += 1
                    continue

                sample_id_avu = iobj.avu(TrackedSample.ID)

                query = mlwh_session.query(Sample).filter(
                    Sample.id_sample_lims == sample_id_avu.value,
                )
                results = query.all()
                record_count = len(results)
                if record_count == 0:
                    msg = f"No record found for sample ID: {sample_id_avu.value}"
                    log.error(msg)
                    num_skipped += 1
                    continue
                if record_count > 1:
                    msg = f"Multiple records found for sample ID: {sample_id_avu.value}"
                    log.error(msg)
                    num_skipped += 1
                    continue

                lims_ok = iobj.has_metadata_attrs([TrackedSample.LIMS])
                uuid_ok = iobj.has_metadata_attrs([TrackedSample.UUID])
                avu_to_add = []
                if lims_ok:
                    msg = f"Sample LIMS attribute ({TrackedSample.LIMS}) already found on {iobj}"
                    log.info(msg)
                else:
                    avu_to_add.append([TrackedSample.LIMS, results[0].id_lims])

                if uuid_ok:
                    msg = f"Sample UUID attribute ({TrackedSample.UUID}) already found on {iobj}"
                    log.info(msg)
                else:
                    avu_to_add.append([TrackedSample.UUID, results[0].uuid_sample_lims])

                if lims_ok and uuid_ok:
                    num_skipped += 1
                    msg = (
                        f"Both Sample UUID and LIMS attributes already found on {iobj}"
                    )
                    log.info(msg)
                    continue

                no_null_avus = [
                    avu for avu in starmap(avu_if_value, avu_to_add) if avu is not None
                ]
                if len(avu_to_add) > len(no_null_avus):
                    msg = f"Possible NULL values for {TrackedSample.LIMS} or {TrackedSample.UUID} on {iobj}"
                    num_skipped += 1
                    log.warning(msg)
                    continue

                num_avu_added = iobj.add_metadata(*no_null_avus)
                if num_avu_added > 0:
                    num_updated += 1
                else:
                    num_skipped += 1

            except RodsError as re:
                num_failed += 1
                log.error(re.message, item=iobj, code=re.code)
            except SQLAlchemyError as se:
                num_failed += 1
                log.error(se, item=iobj)
                sys.exit(1)
            except Exception as e:
                num_failed += 1
                log.error(e, item=iobj)

    return num_updated, num_skipped, num_failed


def main():
    parser = argparse.ArgumentParser(
        description=description, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    add_logging_arguments(parser)
    add_db_config_arguments(parser)
    parser.add_argument(
        "--input",
        help="Input file",
        type=argparse.FileType("r", encoding="UTF-8"),
        default=sys.stdin,
    )
    parser.add_argument(
        "--db_section",
        "--db-section",
        help="Input file",
        type=str,
        choices=["mlwh_ro", "github"],
        default="mlwh_ro",
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
        json=args.json,
    )
    add_appinfo_structlog_processor()

    dbconfig = IniData(db.Config).from_file(args.db_config.name, args.db_section)
    engine = sqlalchemy.create_engine(
        dbconfig.url, pool_pre_ping=True, pool_recycle=3600
    )

    num_updated, num_skipped, num_failed = add_lims_uuid_from_input(engine, args.input)

    if num_failed:
        log.error(
            "Update failed",
            num_updated=num_updated,
            num_skipped=num_skipped,
            num_errors=num_failed,
        )
        sys.exit(1)

    msg = "All updates were successful" if num_updated else "No updates were required"
    log.info(
        msg,
        num_updated=num_updated,
        num_skipped=num_skipped,
    )
