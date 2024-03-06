# -*- coding: utf-8 -*-
#
# Copyright © 2023, 2024 Genome Research Ltd. All rights reserved.
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
from datetime import datetime

import sqlalchemy
import structlog
from partisan.irods import AVU, DataObject, query_metadata
from sqlalchemy.orm import Session

from npg_irods import illumina, ont, pacbio
from npg_irods.cli.util import (
    add_date_range_arguments,
    add_logging_arguments,
    configure_logging,
    integer_in_range,
    with_previous,
)
from npg_irods.db import DBConfig
from npg_irods.db.mlwh import find_consent_withdrawn_samples
from npg_irods.exception import CollectionNotFound
from npg_irods.illumina import find_qc_collection
from npg_irods.metadata.common import SeqConcept
from npg_irods.metadata.illumina import Instrument
from npg_irods.metadata.lims import TrackedSample
from npg_irods.ont import barcode_collections
from npg_irods.version import version

description = """
A utility for locating sets of data objects in iRODS.

Each sub-command runs canned queries first on the ML warehouse and then, using that
result, on iRODS to locate relevant data objects. The available sub-commands are
described below.

Usage:

To see the CLI options available for the base command, use:

    locate-data-objects --help

each sub-command provides additional options which may be seen using:

    locate-data-objects <sub-command> --help

Note that the sub-command available options may differ between sub-commands.

Examples:

    locate-data-objects --verbose --colour --database-config db.ini --zone seq \\
        consent-withdrawn

    locate-data-objects --verbose --json  --database-config db.ini --zone seq \\
        illumina-updates --begin-date `date --iso --date=-7day` \\
        --skip-absent-runs

    locate-data-objects --verbose --colour --database-config db.ini --zone seq \\
        ont-updates --begin-date `date --iso --date=-7day`

The database config file must be an INI format file as follows:

[mlwh_ro]
host = <hostname>
port = <port number>
schema = <database schema name>
user = <database user name>
password = <database password>

The paths of the data objects and/or collections located are printed to STDOUT.
Whether data objects or collection paths are printed depends on which of these
the matched iRODS metadata are attached to. If the metadata are directly attached
to a data object, its path is printed, while if the metadata are attached to a
collection containing a data object, directly or as a root collection, the
collection path is printed.
"""

log = structlog.get_logger("main")


def consent_withdrawn(cli_args):
    dbconfig = DBConfig.from_file(cli_args.database_config.name, "mlwh_ro")
    engine = sqlalchemy.create_engine(dbconfig.url)
    with Session(engine) as session:
        num_processed = num_errors = 0

        for i, s in enumerate(find_consent_withdrawn_samples(session)):
            num_processed += 1
            log.info("Finding data objects", item=i, sample=s)

            if s.id_sample_lims is None:
                num_errors += 1
                log.error("Missing id_sample_lims", item=i, sample=s)
                continue

            query = AVU(TrackedSample.ID, s.id_sample_lims)
            zone = cli_args.zone

            try:
                for obj in query_metadata(
                    query, data_object=True, collection=False, zone=zone
                ):
                    print(obj)
                for coll in query_metadata(
                    query, data_object=False, collection=True, zone=zone
                ):
                    for item in coll.iter_contents(recurse=True):
                        if item.rods_type == DataObject:
                            print(item)
            except Exception as e:
                num_errors += 1
                log.exception(e, item=i, sample=s)

    log.info(f"Processed {num_processed} with {num_errors} errors")

    if num_errors:
        sys.exit(1)


def illumina_updates_cli(cli_args):
    """Process the command line arguments for finding Illumina data objects and execute
    the command.

    Args:
        cli_args: ArgumentParser

    Returns:
        None
    """
    dbconf = DBConfig.from_file(cli_args.database_config.name, "mlwh_ro")
    eng = sqlalchemy.create_engine(dbconf.url)
    since = cli_args.begin_date
    until = cli_args.end_date
    skip_absent_runs = cli_args.skip_absent_runs
    zone = cli_args.zone

    with Session(eng) as sess:
        num_proc, num_errors = illumina_updates(
            sess, since, until, skip_absent_runs=skip_absent_runs, zone=zone
        )

        if num_errors:
            sys.exit(1)


def illumina_updates(
    sess: Session,
    since: datetime,
    until: datetime,
    skip_absent_runs=True,
    zone: str = None,
) -> (int, int):
    """Find recently updated Illumina data in the ML warehouse, locate corresponding
    data objects in iRODS and print their paths.

    Args:
        sess: SQLAlchemy session
        since: Earliest changes to find
        until: Latest changes to find
        skip_absent_runs: Skip any runs that have no data in iRODS. Defaults to True.
        zone: iRODS zone to query

    Returns:
        The number of ML warehouse records processed, the number of errors encountered.
    """
    num_processed = num_errors = 0
    attempts = successes = 0
    to_print = set()

    for prev, curr in with_previous(
        illumina.find_updated_components(sess, since=since, until=until)
    ):
        if curr is None:  # Last item when this is reached
            continue

        num_processed += 1

        avus = [
            AVU(Instrument.RUN, curr.id_run),
            AVU(Instrument.LANE, curr.position),
        ]
        if curr.tag_index is not None:
            avus.append(AVU(SeqConcept.TAG_INDEX, curr.tag_index))

        log_kwargs = {
            "item": num_processed,
            "comp": curr,
            "query": avus,
            "since": since.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "until": until.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

        if skip_absent_runs and successes == 0 and attempts == skip_absent_runs:
            msg = "Skipping run after unsuccessful attempts to find it"
            log.info(msg, attempts=attempts, **log_kwargs)
            continue

        try:
            log.info("Searching iRODS", **log_kwargs)
            result = query_metadata(*avus, collection=False, zone=zone)
            if not result:
                attempts += 1
                continue
            successes += 1

            for obj in result:
                to_print.add(str(obj))

                try:
                    qc_coll = find_qc_collection(obj)
                    for item in qc_coll.iter_contents(recurse=True):
                        to_print.add(str(item))
                except CollectionNotFound as e:
                    log.warning("QC collection missing", path=e.path)

            if prev is not None and curr.id_run != prev.id_run:  # Reached next run
                for obj in sorted(to_print):
                    print(obj)
                to_print.clear()
                successes = attempts = 0

        except Exception as e:
            num_errors += 1
            log.exception(e, item=num_processed, comp=curr)

    for obj in sorted(to_print):
        print(obj)

    log.info(f"Processed {num_processed} with {num_errors} errors")

    return num_processed, num_errors


def ont_updates_cli(cli_args):
    """Process the command line arguments for finding ONT data objects and execute the
    command.

    Args:
        cli_args: ArgumentParser

    Returns:
        None
    """
    dbconf = DBConfig.from_file(cli_args.database_config.name, "mlwh_ro")
    eng = sqlalchemy.create_engine(dbconf.url)
    since = cli_args.begin_date
    until = cli_args.end_date
    report_tags = cli_args.report_tags
    zone = cli_args.zone

    with Session(eng) as sess:
        num_proc, num_errors = ont_updates(
            sess, since, until, report_tags=report_tags, zone=zone
        )

        if num_errors:
            sys.exit(1)


def ont_updates(
    sess: Session,
    since: datetime,
    until: datetime,
    report_tags: bool = False,
    zone: str = None,
) -> (int, int):
    num_processed = num_errors = 0

    for i, c in enumerate(
        ont.find_updated_components(
            sess, since=since, until=until, include_tags=report_tags
        )
    ):
        num_processed += 1
        avus = [
            AVU(ont.Instrument.EXPERIMENT_NAME, c.experiment_name),
            AVU(ont.Instrument.INSTRUMENT_SLOT, c.instrument_slot),
        ]
        log.info(
            "Searching iRODS",
            item=i,
            comp=c,
            query=avus,
            since=since.strftime("%Y-%m-%dT%H:%M:%SZ"),
            until=until.strftime("%Y-%m-%dT%H:%M:%SZ"),
        )

        try:
            for coll in query_metadata(*avus, data_object=False, zone=zone):
                # Report only the run folder collection for multiplexed runs
                # unless requested to report the deplexed collections
                if report_tags and c.tag_identifier is not None:
                    for bcoll in barcode_collections(coll, c.tag_identifier):
                        print(bcoll)
                else:
                    print(coll)

        except Exception as e:
            num_errors += 1
            log.exception(e, item=i, comp=c)

    log.info(f"Processed {num_processed} with {num_errors} errors")

    return num_processed, num_errors


def pacbio_updates_cli(cli_args):
    """Process the command line arguments for finding PacBio data objects and execute the
    command.

    Args:
        cli_args: ArgumentParser

    Returns:
        None
    """
    dbconf = DBConfig.from_file(cli_args.database_config.name, "mlwh_ro")
    eng = sqlalchemy.create_engine(dbconf.url)
    since = cli_args.begin_date
    until = cli_args.end_date
    skip_absent_runs = cli_args.skip_absent_runs
    zone = cli_args.zone

    with Session(eng) as sess:
        num_proc, num_errors = pacbio_updates(
            sess, since, until, skip_absent_runs=skip_absent_runs, zone=zone
        )

        if num_errors:
            sys.exit(1)


def pacbio_updates(
    sess: Session,
    since: datetime,
    until: datetime,
    skip_absent_runs=True,
    zone: str = None,
) -> (int, int):
    num_processed = num_errors = 0
    attempts = successes = 0
    to_print = set()

    for prev, curr in with_previous(
        pacbio.find_updated_components(sess, since=since, until=until)
    ):
        if curr is None:  # Last item when this is reached
            continue

        num_processed += 1
        avus = [
            AVU(pacbio.Instrument.RUN_NAME, curr.run_name),
            AVU(pacbio.Instrument.WELL_LABEL, curr.well_label),
        ]
        if curr.tag_sequence is not None:
            avus.append(AVU(pacbio.Instrument.TAG_SEQUENCE, curr.tag_sequence))

        log_kwargs = {
            "item": num_processed,
            "comp": curr,
            "query": avus,
            "since": since.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "until": until.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

        if skip_absent_runs and successes == 0 and attempts == skip_absent_runs:
            msg = "Skipping run after unsuccessful attempts to find it"
            log.info(msg, attempts=attempts, **log_kwargs)
            continue

        try:
            log.info("Searching iRODS", **log_kwargs)
            result = query_metadata(*avus, data_object=True, zone=zone)
            if not result:
                attempts += 1
                continue
            successes += 1

            for obj in result:
                to_print.add(str(obj))

            if prev is not None and curr.run_name != prev.run_name:  # Reached next run
                for obj in sorted(to_print):
                    print(obj)
                to_print.clear()
                successes = attempts = 0

        except Exception as e:
            num_errors += 1
            log.exception(e, item=num_processed, comp=curr)

    log.info(f"Processed {num_processed} with {num_errors} errors")

    return num_processed, num_errors


def main():
    parser = argparse.ArgumentParser(
        description=description, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    add_logging_arguments(parser)
    parser.add_argument(
        "--database-config",
        "--database_config",
        "--db-config",
        "--db_config",
        help="Configuration file for database connection",
        type=argparse.FileType("r"),
        required=True,
    )

    parser.add_argument(
        "--zone",
        help="Specify a federated iRODS zone in which to find data objects to check. "
        "This is not required if the target collections are in the local zone.",
        type=str,
    )
    parser.add_argument(
        "--version", help="Print the version and exit", action="store_true"
    )

    subparsers = parser.add_subparsers(title="Sub-commands", required=True)

    cwdr_parser = subparsers.add_parser(
        "consent-withdrawn",
        help="Find data objects related to samples whose consent for data use has "
        "been withdrawn.",
    )
    cwdr_parser.set_defaults(func=consent_withdrawn)

    ilup_parser = subparsers.add_parser(
        "illumina-updates",
        help="Find data objects, which are components of Illumina runs, whose tracking "
        "metadata in the ML warehouse have changed since a specified time.",
    )
    add_date_range_arguments(ilup_parser)
    ilup_parser.add_argument(
        "--skip-absent-runs",
        "--skip_absent_runs",
        help="Skip runs that cannot be found in iRODS after the number of attempts "
        "given as an argument to this option. The argument may be an integer from "
        "1 to 10, inclusive and defaults to 3.",
        nargs="?",
        action="store",
        type=integer_in_range(1, 10),
        default=3,
    )
    ilup_parser.set_defaults(func=illumina_updates_cli)

    ontup_parser = subparsers.add_parser(
        "ont-updates",
        help="Find collections, containing data objects for ONT runs, whose tracking"
        "metadata in the ML warehouse have changed since a specified time.",
    )
    add_date_range_arguments(ontup_parser)
    ontup_parser.add_argument(
        "--report-tags",
        "--report_tags",
        help="Include barcode sub-collections of runs containing de-multiplexed data.",
        action="store_true",
    )
    ontup_parser.set_defaults(func=ont_updates_cli)

    pbup_parser = subparsers.add_parser(
        "pacbio-updates",
        help="Find data objects, which are components of PacBio runs, whose tracking "
        "metadata in the ML warehouse have changed since a specified time.",
    )
    add_date_range_arguments(pbup_parser)
    pbup_parser.add_argument(
        "--skip-absent-runs",
        "--skip_absent_runs",
        help="Skip runs that cannot be found in iRODS after the number of attempts "
        "given as an argument to this option. The argument may be an integer from "
        "1 to 10, inclusive and defaults to 3.",
        nargs="?",
        action="store",
        type=integer_in_range(1, 10),
        default=3,
    )

    pbup_parser.set_defaults(func=pacbio_updates_cli)

    args = parser.parse_args()
    configure_logging(
        config_file=args.log_config,
        debug=args.debug,
        verbose=args.verbose,
        colour=args.colour,
        json=args.json,
    )

    if args.version:
        print(version())
        sys.exit(0)
    args.func(args)
