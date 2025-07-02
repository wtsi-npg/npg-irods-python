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
from typing import Any, Iterator

import sqlalchemy
import structlog
from npg.cli import (
    add_date_range_arguments,
    add_db_config_arguments,
    add_logging_arguments,
    integer_in_range,
)
from npg.conf import IniData
from npg.iter import with_previous
from npg.log import configure_structlog
from partisan.irods import AVU, DataObject, RodsItem, query_metadata
from sqlalchemy.orm import Session

from npg_irods import (
    add_appinfo_structlog_processor,
    db,
    illumina,
    ont,
    pacbio,
    sequenom,
    version,
)
from npg_irods.db.mlwh import (
    find_consent_withdrawn_samples,
    find_updated_samples,
    find_updated_studies,
)
from npg_irods.exception import CollectionNotFound
from npg_irods.illumina import find_qc_collection
from npg_irods.metadata import infinium
from npg_irods.metadata.common import SeqConcept
from npg_irods.metadata.illumina import Instrument
from npg_irods.metadata.lims import TrackedSample, TrackedStudy
from npg_irods.ont import barcode_collections

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

    locate-data-objects --verbose --colour --db-config db.ini --zone seq \\
        consent-withdrawn

    locate-data-objects --verbose --json --db-config db.ini --zone seq \\
        illumina-updates --begin-date `date --iso --date=-7day` \\
        --skip-absent-runs 5

    locate-data-objects --verbose --colour --db-config db.ini --zone seq \\
        ont-updates --begin-date `date --iso --date=-7day`

The --skip-absent-runs option is used to skip runs that cannot be found in iRODS after
the number of attempts given as an argument to this option. This prevents the script
from trying to find further data objects after it is clear that the run has not yet
reached iRODS.

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


def consent_withdrawn(cli_args: argparse.ArgumentParser):
    dbconfig = IniData(db.Config).from_file(cli_args.db_config.name, "mlwh_ro")
    json = cli_args.report_json

    engine = sqlalchemy.create_engine(
        dbconfig.url, pool_pre_ping=True, pool_recycle=3600
    )

    num_processed = num_errors = 0

    with Session(engine) as session:
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
                    _print(obj, json=json)
                for coll in query_metadata(
                    query, data_object=False, collection=True, zone=zone
                ):
                    for item in coll.iter_contents(recurse=True):
                        if item.rods_type == DataObject:
                            _print(item, json=json)
            except Exception as e:
                num_errors += 1
                log.exception(e, item=i, sample=s)

    log.info(f"Processed {num_processed} with {num_errors} errors")

    if num_errors:
        sys.exit(1)


def illumina_updates_cli(cli_args: argparse.ArgumentParser):
    """Process the command line arguments for finding Illumina data objects and execute
    the command."""
    dbconfig = IniData(db.Config).from_file(cli_args.db_config.name, "mlwh_ro")
    engine = sqlalchemy.create_engine(
        dbconfig.url, pool_pre_ping=True, pool_recycle=3600
    )
    since = cli_args.begin_date
    until = cli_args.end_date
    skip_absent_runs = cli_args.skip_absent_runs
    json = cli_args.report_json
    zone = cli_args.zone

    with Session(engine) as sess:
        num_proc, num_errors = illumina_updates(
            sess, since, until, skip_absent_runs=skip_absent_runs, json=json, zone=zone
        )

        if num_errors:
            sys.exit(1)


def illumina_updates(
    sess: Session,
    since: datetime,
    until: datetime,
    skip_absent_runs: int = None,
    json: bool = False,
    zone: str = None,
) -> (int, int):
    """Find recently updated Illumina data in the ML warehouse, locate corresponding
    data objects in iRODS and print their paths.

    Args:
        sess: An open SQL session.
        since: Earliest changes to find.
        until: Latest changes to find.
        skip_absent_runs: Skip any runs where no data objects have been found after
            this number of attempts.
        json: Print output in JSON format.
        zone: iRODS zone to query.

    Returns:
        The number of ML warehouse records processed, the number of errors encountered.
    """
    num_processed = num_errors = 0
    attempts = successes = 0
    to_print = set()

    if skip_absent_runs is not None:
        log.info("Skipping absent runs after n attempts", n=skip_absent_runs)

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

        if skip_absent_runs is not None:
            if successes == 0 and attempts == skip_absent_runs:
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
                to_print.add(obj)

                try:
                    qc_coll = find_qc_collection(obj)
                    for item in qc_coll.iter_contents(recurse=True):
                        to_print.add(item)
                except CollectionNotFound as e:
                    log.warning("QC collection missing", path=e.path)

            if prev is not None and curr.id_run != prev.id_run:  # Reached next run
                _print_batch(to_print, json=json)
                to_print.clear()
                successes = attempts = 0

        except Exception as e:
            num_errors += 1
            log.exception(e, item=num_processed, comp=curr)

    _print_batch(to_print, json=json)
    log.info(f"Processed {num_processed} with {num_errors} errors")

    return num_processed, num_errors


def ont_updates_cli(cli_args: argparse.ArgumentParser):
    """Process the command line arguments for finding ONT data objects and execute the
    command."""
    dbconfig = IniData(db.Config).from_file(cli_args.db_config.name, "mlwh_ro")
    engine = sqlalchemy.create_engine(
        dbconfig.url, pool_pre_ping=True, pool_recycle=3600
    )
    since = cli_args.begin_date
    until = cli_args.end_date
    report_tags = cli_args.report_tags
    json = cli_args.report_json
    zone = cli_args.zone

    with Session(engine) as sess:
        num_proc, num_errors = ont_updates(
            sess, since, until, report_tags=report_tags, json=json, zone=zone
        )

        if num_errors:
            sys.exit(1)


def ont_updates(
    sess: Session,
    since: datetime,
    until: datetime,
    report_tags: bool = False,
    json: bool = False,
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
                        _print(bcoll, json=json)
                else:
                    _print(coll, json=json)

        except Exception as e:
            num_errors += 1
            log.exception(e, item=i, comp=c)

    log.info(f"Processed {num_processed} with {num_errors} errors")

    return num_processed, num_errors


def ont_run_collections_created_cli(cli_args: argparse.ArgumentParser):
    """Process the command line arguments for finding ONT runfolder collections
    selected on the time they were created in iRODS, and execute the command."""
    since = cli_args.begin_date
    until = cli_args.end_date
    json = cli_args.report_json
    zone = cli_args.zone

    num_proc, num_errors = ont_run_collections_created(
        since, until, json=json, zone=zone
    )

    if num_errors:
        sys.exit(1)


def ont_run_collections_created(
    since: datetime, until: datetime, json: bool = False, zone: str = None
) -> (int, int):
    num_processed = num_errors = 0

    log.info(
        "Searching iRODS",
        since=since.strftime("%Y-%m-%dT%H:%M:%SZ"),
        until=until.strftime("%Y-%m-%dT%H:%M:%SZ"),
    )

    for i, coll in enumerate(ont.find_run_collections(since, until, zone=zone)):
        num_processed += 1

        try:
            _print(coll, json=json)
        except Exception as e:
            num_errors += 1
            log.exception(e, item=i, coll=coll)

    log.info(f"Processed {num_processed} with {num_errors} errors")

    return num_processed, num_errors


def pacbio_updates_cli(cli_args: argparse.ArgumentParser):
    """Process the command line arguments for finding PacBio data objects and execute
    the command."""
    dbconfig = IniData(db.Config).from_file(cli_args.db_config.name, "mlwh_ro")
    engine = sqlalchemy.create_engine(
        dbconfig.url, pool_pre_ping=True, pool_recycle=3600
    )
    since = cli_args.begin_date
    until = cli_args.end_date
    skip_absent_runs = cli_args.skip_absent_runs
    json = cli_args.report_json
    zone = cli_args.zone

    with Session(engine) as sess:
        num_proc, num_errors = pacbio_updates(
            sess, since, until, skip_absent_runs=skip_absent_runs, json=json, zone=zone
        )

        if num_errors:
            sys.exit(1)


def pacbio_updates(
    sess: Session,
    since: datetime,
    until: datetime,
    skip_absent_runs: int = None,
    json: bool = False,
    zone: str = None,
) -> (int, int):
    num_processed = num_errors = 0
    attempts = successes = 0
    to_print = set()

    if skip_absent_runs is not None:
        log.info("Skipping absent runs after n attempts", n=skip_absent_runs)

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

        if skip_absent_runs is not None:
            if successes == 0 and attempts == skip_absent_runs:
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
                to_print.add(obj)

            if prev is not None and curr.run_name != prev.run_name:  # Reached next run
                for obj in sorted(to_print):
                    _print(obj, json=json)
                to_print.clear()
                successes = attempts = 0

        except Exception as e:
            num_errors += 1
            log.exception(e, item=num_processed, comp=curr)

    for obj in sorted(to_print):
        _print(obj, json=json)

    log.info(f"Processed {num_processed} with {num_errors} errors")

    return num_processed, num_errors


def infinium_updates_cli(cli_args: argparse.ArgumentParser):
    """Process the command line arguments for finding Infinium microarray data objects
    and execute the command."""
    dbconfig = IniData(db.Config).from_file(cli_args.db_config.name, "mlwh_ro")
    engine = sqlalchemy.create_engine(
        dbconfig.url, pool_pre_ping=True, pool_recycle=3600
    )
    since = cli_args.begin_date
    until = cli_args.end_date
    json = cli_args.report_json
    zone = cli_args.zone

    with Session(engine) as sess:
        num_proc, num_errors = infinium_microarray_updates(
            sess, since, until, json=json, zone=zone
        )

        if num_errors:
            sys.exit(1)


def infinium_microarray_updates(
    sess: Session,
    since: datetime,
    until: datetime,
    json: bool = False,
    zone: str = None,
) -> (int, int):
    query = [AVU(infinium.Instrument.BEADCHIP, "%", operator="like")]
    num_processed, num_errors = _print_data_objects_updated_in_mlwh(
        sess, query, since=since, until=until, json=json, zone=zone
    )

    log.info(f"Processed {num_processed} with {num_errors} errors")

    return num_processed, num_errors


def sequenom_updates_cli(cli_args: argparse.ArgumentParser):
    """Process the command line arguments for finding Sequenom genotype data objects
    and execute the command."""
    dbconfig = IniData(db.Config).from_file(cli_args.db_config.name, "mlwh_ro")
    engine = sqlalchemy.create_engine(
        dbconfig.url, pool_pre_ping=True, pool_recycle=3600
    )
    since = cli_args.begin_date
    until = cli_args.end_date
    zone = cli_args.zone

    with Session(engine) as sess:
        num_proc, num_errors = sequenom_genotype_updates(sess, since, until, zone=zone)

        if num_errors:
            sys.exit(1)


def sequenom_genotype_updates(
    sess: Session, since: datetime, until: datetime, zone: str = None
) -> (int, int):
    query = [AVU(sequenom.Instrument.SEQUENOM_PLATE, "%", operator="like")]
    num_processed, num_errors = _print_data_objects_updated_in_mlwh(
        sess, query, since=since, until=until, zone=zone
    )

    log.info(f"Processed {num_processed} with {num_errors} errors")

    return num_processed, num_errors


def _print_data_objects_updated_in_mlwh(
    sess: Session,
    query: list[AVU],
    since: datetime,
    until: datetime,
    json: bool = False,
    zone: str = None,
) -> (int, int):
    num_processed = num_errors = 0

    studies = find_updated_studies(sess, since=since, until=until)
    np, ne = _find_and_print_data_objects(
        TrackedStudy.ID, studies, query, since=since, until=until, json=json, zone=zone
    )
    num_processed += np
    num_errors += ne

    samples = find_updated_samples(sess, since=since, until=until)
    np, ne = _find_and_print_data_objects(
        TrackedSample.ID, samples, query, since=since, until=until, json=json, zone=zone
    )
    num_processed += np
    num_errors += ne

    return num_processed, num_errors


def _find_and_print_data_objects(
    attr: Any,
    values: Iterator[int],
    query: list[AVU],
    since: datetime,
    until: datetime,
    json: bool = False,
    zone: str = None,
) -> (int, int):
    """Print data object paths identified by their metadata e.g. sample ID or study ID.

    Args:
        attr: An AVU attribute to search on e.g. sample ID or study ID.
        values: An interator of AVU values corresponding to the attribute to search for.
        query: Additional AVUs to combine with attr and values in the queries.
        since: Earliest changes to find.
        until: Latest changes to find.
        json: Print output in JSON format.
        zone: iRODS zone to query.

    Returns:
        The number of records processed, the number of errors encountered.
    """
    num_processed = num_errors = 0

    for i, value in enumerate(values):
        avu = AVU(attr, value)

        num_processed += 1
        log.info(
            "Searching iRODS",
            item=i,
            query=[avu, *query],
            since=since.strftime("%Y-%m-%dT%H:%M:%SZ"),
            until=until.strftime("%Y-%m-%dT%H:%M:%SZ"),
        )

        try:
            for obj in query_metadata(
                avu, *query, data_object=True, collection=False, zone=zone
            ):
                _print(obj, json=json)
        except Exception as e:
            num_errors += 1
            log.exception(e, item=i, attr=attr, value=value)

    return num_processed, num_errors


def _print(item: RodsItem, json: bool = False):
    if json:
        print(item.to_json(indent=None, sort_keys=True))
    else:
        print(item)


def _print_batch(items: set[RodsItem], json: bool = False):
    if json:
        lines = [item.to_json(indent=None, sort_keys=True) for item in items]
    else:
        lines = [str(item) for item in items]
    lines.sort()
    print("\n".join(lines))


def main():
    parser = argparse.ArgumentParser(
        description=description, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    add_logging_arguments(parser)
    add_db_config_arguments(parser)

    parser.add_argument(
        "--zone",
        help="Specify a federated iRODS zone in which to find data objects to check. "
        "This is not required if the target collections are in the local zone.",
        type=str,
    )
    parser.add_argument(
        "--version",
        help="Print the version and exit.",
        action="version",
        version=version(),
    )

    subparsers = parser.add_subparsers(title="Sub-commands", required=True)

    cwdr_parser = subparsers.add_parser(
        "consent-withdrawn",
        help="Find data objects related to samples whose consent for data use has "
        "been withdrawn.",
    )
    cwdr_parser.add_argument(
        "--report-json",
        "--report_json",
        help="Print output in JSON format.",
        action="store_true",
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
    ilup_parser.add_argument(
        "--report-json",
        "--report_json",
        help="Print output in JSON format.",
        action="store_true",
    )
    ilup_parser.set_defaults(func=illumina_updates_cli)

    ontcre_parser = subparsers.add_parser(
        "ont-run-creation",
        help="Find ONT runfolder collections created in iRODS within a specified time "
        "range.",
    )
    add_date_range_arguments(ontcre_parser)
    ontcre_parser.add_argument(
        "--report-json",
        "--report_json",
        help="Print output in JSON format.",
        action="store_true",
    )
    ontcre_parser.set_defaults(func=ont_run_collections_created_cli)

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
    ontup_parser.add_argument(
        "--report-json",
        "--report_json",
        help="Print output in JSON format.",
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
    pbup_parser.add_argument(
        "--report-json",
        "--report_json",
        help="Print output in JSON format.",
        action="store_true",
    )
    pbup_parser.set_defaults(func=pacbio_updates_cli)

    imup_parser = subparsers.add_parser(
        "infinium-updates",
        help="Find data objects related to Infinium microarray samples whose tracking "
        "metadata in the ML warehouse have changed since a specified time.",
    )
    add_date_range_arguments(imup_parser)
    imup_parser.add_argument(
        "--report-json",
        "--report_json",
        help="Print output in JSON format.",
        action="store_true",
    )
    imup_parser.set_defaults(func=infinium_updates_cli)

    squp_parser = subparsers.add_parser(
        "sequenom-updates",
        help="Find data objects related to Sequenom genotype samples whose tracking "
        "metadata in the ML warehouse have changed since a specified time.",
    )
    add_date_range_arguments(squp_parser)
    squp_parser.add_argument(
        "--report-json",
        "--report_json",
        help="Print output in JSON format.",
        action="store_true",
    )
    squp_parser.set_defaults(func=sequenom_updates_cli)

    args = parser.parse_args()
    configure_structlog(
        config_file=args.log_config,
        debug=args.debug,
        verbose=args.verbose,
        colour=args.colour,
        json=args.log_json,
    )
    add_appinfo_structlog_processor()

    args.func(args)
