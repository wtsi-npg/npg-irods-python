# -*- coding: utf-8 -*-
#
# Copyright © 2023, 2026 Genome Research Ltd. All rights reserved.
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

"""Business logic API and schema-level API for the ML warehouse."""

import enum
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any, Generator, Iterator, Type

import structlog
from npgmlwarehouse.db.schema import Sample, Study
from sqlalchemy import Engine, asc, not_, select
from sqlalchemy.orm import Session

SQL_CHUNK_SIZE = 1000

log = structlog.get_logger(__package__)


class Platform(enum.Enum):
    """Sequencing platform values for SeqProductIrodsLocations.seq_platform_name"""

    Illumina = 1
    ONT = 2
    PacBio = 3


@contextmanager
def session_context(engine: Engine) -> Generator[Session, Any, None]:
    """Yield a session and close, or rollback on error. This context manager does
    not handle exceptions and will raise them to the caller."""
    session = Session(engine)
    try:
        yield session
        log.debug("Committing MLWH session", session=session)
        session.commit()
    except Exception as e:
        session.rollback()
        log.error("Rolling back MLWH session", session=session)
        raise e
    finally:
        log.debug("Closing MLWH session", session=session)
        session.close()


def find_consent_withdrawn_samples(sess: Session) -> list[Type[Sample]]:
    """Return a list of all samples with consent withdrawn.

    Args:
        sess: An open session to the ML warehouse.

    Returns:
        All samples marked as having their consent withdrawn.
    """
    return sess.query(Sample).filter(Sample.consent_withdrawn == 1).all()


def find_study_by_study_id(sess: Session, study_id: str) -> Study:
    """Return a Study from a study ID.

    Args:
        sess: An open SQL session.
        study_id: A Study ID in the ML warehouse.

    Returns:
        An ML warehouse schema Study.
    """
    return sess.execute(
        select(Study).where(Study.id_study_lims == study_id)
    ).scalar_one()


def find_sample_by_sample_id(sess: Session, sample_id: str) -> Sample:
    """Return a Sample from a sample ID.

    Args:
        sess: An open SQL session.
        sample_id: A Sample ID in the ML warehouse.

    Returns:
        An ML warehouse schema Sample.
    """
    return sess.execute(
        select(Sample).where(Sample.id_sample_lims == sample_id)
    ).scalar_one()


def find_updated_samples(
    sess: Session, since: datetime, until: datetime
) -> Iterator[Sample]:
    """Return Samples that have been updated in the ML warehouse.

    Args:
        sess: An open SQL session.
        since: The start of the time range.
        until: The end of the time range.

    Returns:
        Iterator of Samples.
    """
    recent_creation = since - timedelta(days=1)  # FIXME

    query = (
        sess.query(Sample)
        .filter(
            Sample.recorded_at.between(since, until)
            & not_(Sample.created.between(recent_creation, since))
        )
        .order_by(asc(Sample.recorded_at))
    )

    for sample in query.yield_per(SQL_CHUNK_SIZE):
        yield sample


def find_updated_studies(
    sess: Session, since: datetime, until: datetime
) -> Iterator[Study]:
    """Return Studies that have been updated in the ML warehouse.

    Args:
        sess: An open SQL session.
        since: The start of the time range.
        until: The end of the time range.

    Returns:
        Iterator of Studies.
    """
    recent_creation = since - timedelta(days=1)  # FIXME

    query = (
        sess.query(Study)
        .filter(
            Study.recorded_at.between(since, until)
            & not_(Study.created.between(recent_creation, since))
        )
        .order_by(asc(Study.recorded_at))
    )

    for study in query.yield_per(SQL_CHUNK_SIZE):
        yield study
