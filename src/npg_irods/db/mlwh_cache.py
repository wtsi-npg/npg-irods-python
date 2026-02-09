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

"""SQLite-backed cache of ML warehouse content hashes."""

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence

from sqlalchemy import asc, select
from sqlalchemy.orm import Session
from structlog import get_logger

from npg_irods.db.mlwh import Sample, Study, find_updated_samples, find_updated_studies


def logger():
    """Return a logger for this module."""

    return get_logger(__name__)


CACHE_SCHEMA_VERSION = 1
SQLITE_BUSY_TIMEOUT_MS = 5000
CACHE_CHUNK_SIZE = (
    500  # Don't make this bigger than 32,766 (maximum size of an SQLite IN clause)
)

SAMPLE_HASH_FIELDS = (
    "id_lims",
    "id_sample_lims",
    "consent_withdrawn",
    "name",
    "organism",
    "accession_number",
    "common_name",
    "cohort",
    "sanger_sample_id",
    "supplier_name",
    "public_name",
    "donor_id",
    "date_of_consent_withdrawn",
    "marked_as_consent_withdrawn_by",
    "uuid_sample_lims",
)

STUDY_HASH_FIELDS = (
    "id_lims",
    "id_study_lims",
    "name",
    "accession_number",
    "description",
    "contains_human_dna",
    "contaminated_human_dna",
    "remove_x_and_autosomes",
    "separate_y_chromosome_data",
    "ena_project_id",
    "study_title",
    "study_visibility",
    "ega_dac_accession_number",
    "data_access_group",
)


SAMPLE_CACHE_CREATE_SQL = (
    "CREATE TABLE IF NOT EXISTS sample_cache ("
    "id_sample_lims TEXT PRIMARY KEY, "
    "content_hash TEXT NOT NULL, "
    "hash_schema_version INTEGER NOT NULL, "
    "last_changed_at TEXT NOT NULL)"
)
STUDY_CACHE_CREATE_SQL = (
    "CREATE TABLE IF NOT EXISTS study_cache ("
    "id_study_lims TEXT PRIMARY KEY, "
    "content_hash TEXT NOT NULL, "
    "hash_schema_version INTEGER NOT NULL, "
    "last_changed_at TEXT NOT NULL)"
)

SAMPLE_CACHE_UPSERT_SQL = (
    "INSERT INTO sample_cache (id_sample_lims, content_hash, hash_schema_version, "
    "last_changed_at) VALUES (?, ?, ?, ?) "
    "ON CONFLICT(id_sample_lims) DO UPDATE SET "
    "content_hash=excluded.content_hash, "
    "hash_schema_version=excluded.hash_schema_version, "
    "last_changed_at=excluded.last_changed_at "
    "WHERE content_hash != excluded.content_hash "
    "OR hash_schema_version != excluded.hash_schema_version"
)
STUDY_CACHE_UPSERT_SQL = (
    "INSERT INTO study_cache (id_study_lims, content_hash, hash_schema_version, last_changed_at) "
    "VALUES (?, ?, ?, ?) "
    "ON CONFLICT(id_study_lims) DO UPDATE SET "
    "content_hash=excluded.content_hash, "
    "hash_schema_version=excluded.hash_schema_version, "
    "last_changed_at=excluded.last_changed_at "
    "WHERE content_hash != excluded.content_hash "
    "OR hash_schema_version != excluded.hash_schema_version"
)


@dataclass
class MlwhChangeCache:
    """Cache for detecting ML warehouse Sample/Study content changes.

    Uses an SQLite database to store content hashes for rows in the ML warehouse so
    that timestamp-only updates can be filtered out.

    The timestamp columns in the study and sample tables are not sufficient to for us
    to tell whether or not row data values have changed. This is because the timestamp
    columns are updated whenever a row is modified, even if no values have changed.
    (Possibly related to the MLWH update mechanism which deletes and inserts new rows
    rather than updating existing rows.)

    Therefore, we need to use content hashes to detect changes in the actual data.

    Args:
        path: Filesystem path to the SQLite cache file.
        hash_schema_version: Version number for the hashing schema.
        busy_timeout_ms: SQLite busy timeout in milliseconds.
        prime_cache: When True, populate the cache with all rows before filtering
            for changes.
    """

    path: Path
    hash_schema_version: int = CACHE_SCHEMA_VERSION
    busy_timeout_ms: int = SQLITE_BUSY_TIMEOUT_MS
    prime_cache: bool = False

    _conn: sqlite3.Connection | None = None

    def __enter__(self):
        """Open the cache and ensure required tables exist.

        Returns:
            The open cache instance.
        """

        path = self.path.resolve()
        path.parent.mkdir(parents=True, exist_ok=True)

        self._conn = sqlite3.connect(
            path.as_posix(), timeout=self.busy_timeout_ms / 1000
        )
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")

        _ensure_schema(self._conn)

        return self

    def __exit__(self, err_type, err, traceback):
        """Close the cache connection.

        Args:
            err_type: Exception type raised within the context, if any.
            err: Exception raised within the context, if any.
            traceback: Traceback for the exception, if any.
        """

        if self._conn is not None:
            self._conn.close()
        self._conn = None

    def changed_sample_ids(
        self, sess: Session, since: datetime, until: datetime
    ) -> set[str]:
        """Return Sample IDs with content changes in the given time range.

        Args:
            sess: Open SQLAlchemy session for the ML warehouse.
            since: Start of the recorded_at time window.
            until: End of the recorded_at time window.

        Returns:
            Set of sample IDs whose content has changed since the last cache run.
        """

        if self.prime_cache:
            self.prime_samples(sess)

        return _filter_changed_rows(
            sess,
            self._active_conn(),
            Sample,
            "id_sample_lims",
            SAMPLE_HASH_FIELDS,
            find_updated_samples(sess, since, until),
            self.hash_schema_version,
            _load_sample_cache,
            _upsert_sample_cache,
        )

    def changed_study_ids(
        self, sess: Session, since: datetime, until: datetime
    ) -> set[str]:
        """Return Study IDs with content changes in the given time range.

        Args:
            sess: Open SQLAlchemy session for the ML warehouse.
            since: Start of the recorded_at time window.
            until: End of the recorded_at time window.

        Returns:
            Set of study IDs whose content has changed since the last cache run.
        """

        if self.prime_cache:
            self.prime_studies(sess)

        return _filter_changed_rows(
            sess,
            self._active_conn(),
            Study,
            "id_study_lims",
            STUDY_HASH_FIELDS,
            find_updated_studies(sess, since, until),
            self.hash_schema_version,
            _load_study_cache,
            _upsert_study_cache,
        )

    def prime_samples(self, sess: Session) -> int:
        """Populate the sample cache with hashes for all MLWH Sample rows."""

        total = _prime_cache(
            sess,
            self._active_conn(),
            Sample,
            "id_sample_lims",
            SAMPLE_HASH_FIELDS,
            self.hash_schema_version,
            _upsert_sample_cache,
        )
        logger().info("Primed sample cache", cache=self.path.as_posix(), rows=total)

        return total

    def prime_studies(self, sess: Session) -> int:
        """Populate the study cache with hashes for all MLWH Study rows."""

        total = _prime_cache(
            sess,
            self._active_conn(),
            Study,
            "id_study_lims",
            STUDY_HASH_FIELDS,
            self.hash_schema_version,
            _upsert_study_cache,
        )
        logger().info("Primed study cache", cache=self.path.as_posix(), rows=total)

        return total

    def _active_conn(self) -> sqlite3.Connection:
        """Return the active SQLite connection or raise if not open."""

        if self._conn is None:
            raise RuntimeError("Cache is not open")

        return self._conn


def _filter_changed_rows(
    sess: Session,
    conn: sqlite3.Connection,
    model,
    id_attr: str,
    hash_fields: Sequence[str],
    candidate_ids: Iterable[str],
    hash_schema_version: int,
    load_cache,
    upsert_cache,
) -> set[str]:
    """Return IDs whose cached hashes differ from the current content."""

    changed: set[str] = set()

    for chunk in _chunked(candidate_ids, CACHE_CHUNK_SIZE):
        ids = list(dict.fromkeys(chunk))
        if not ids:
            continue

        rows = (
            sess.execute(select(model).where(getattr(model, id_attr).in_(ids)))
            .scalars()
            .all()
        )
        if not rows:
            continue

        cache_map = load_cache(conn, [getattr(row, id_attr) for row in rows])
        updates: list[tuple[str, str, int, str]] = []
        now = datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat()

        for row in rows:
            row_id = getattr(row, id_attr)
            content_hash = _stable_hash(_payload(row, hash_fields))
            cached = cache_map.get(row_id)
            if cached is None:
                changed.add(row_id)
                updates.append((row_id, content_hash, hash_schema_version, now))
            elif cached[0] != content_hash or cached[1] != hash_schema_version:
                changed.add(row_id)
                updates.append((row_id, content_hash, hash_schema_version, now))

        upsert_cache(conn, updates)

    return changed


def _prime_cache(
    sess: Session,
    conn: sqlite3.Connection,
    model,
    id_attr: str,
    hash_fields: Sequence[str],
    hash_schema_version: int,
    upsert_cache,
) -> int:
    """Insert hashes for all rows in the model into the cache."""

    updates: list[tuple[str, str, int, str]] = []
    total = 0
    query = sess.query(model).order_by(asc(getattr(model, id_attr)))

    for row in query.yield_per(CACHE_CHUNK_SIZE):
        row_id = getattr(row, id_attr)
        content_hash = _stable_hash(_payload(row, hash_fields))
        now = datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat()
        updates.append((row_id, content_hash, hash_schema_version, now))

        if len(updates) >= CACHE_CHUNK_SIZE:
            upsert_cache(conn, updates)
            total += len(updates)
            updates.clear()

    if updates:
        upsert_cache(conn, updates)
        total += len(updates)

    return total


def _load_sample_cache(conn: sqlite3.Connection, ids: list[str]) -> dict:
    """Load cached sample hashes for the given IDs."""

    if not ids:
        return {}

    placeholders = ",".join("?" for _ in ids)
    query = (
        "SELECT id_sample_lims, content_hash, hash_schema_version "
        f"FROM sample_cache WHERE id_sample_lims IN ({placeholders})"
    )
    rows = conn.execute(query, ids).fetchall()
    logger().debug(
        "Loaded sample cache rows", num_requested=len(ids), num_loaded=len(rows)
    )

    return {row[0]: (row[1], row[2]) for row in rows}


def _load_study_cache(conn: sqlite3.Connection, ids: list[str]) -> dict:
    """Load cached study hashes for the given IDs."""

    if not ids:
        return {}

    placeholders = ",".join("?" for _ in ids)
    query = (
        "SELECT id_study_lims, content_hash, hash_schema_version "
        "FROM study_cache "
        f"WHERE id_study_lims IN ({placeholders})"
    )
    rows = conn.execute(query, ids).fetchall()
    logger().debug(
        "Loaded study cache rows", num_requested=len(ids), num_loaded=len(rows)
    )

    return {row[0]: (row[1], row[2]) for row in rows}


def _upsert_sample_cache(
    conn: sqlite3.Connection, updates: list[tuple[str, str, int, str]]
) -> None:
    """Insert or update sample cache rows."""

    if not updates:
        return

    conn.executemany(SAMPLE_CACHE_UPSERT_SQL, updates)
    conn.commit()
    logger().debug("Upserted new sample cache rows", n=len(updates))


def _upsert_study_cache(
    conn: sqlite3.Connection, updates: list[tuple[str, str, int, str]]
) -> None:
    """Insert or update study cache rows."""

    if not updates:
        return

    conn.executemany(STUDY_CACHE_UPSERT_SQL, updates)
    conn.commit()
    logger().debug("Upserted new study cache rows", n=len(updates))


def _ensure_schema(conn: sqlite3.Connection) -> None:
    """Create cache tables if missing."""

    conn.execute(SAMPLE_CACHE_CREATE_SQL)
    conn.execute(STUDY_CACHE_CREATE_SQL)
    conn.commit()


def _chunked(values: Iterable[str], size: int) -> Iterable[list[str]]:
    """Yield lists of values in fixed-size chunks."""

    chunk: list[str] = []
    for value in values:
        chunk.append(value)
        if len(chunk) >= size:
            yield chunk
            chunk = []

    if chunk:
        yield chunk


def _payload(row, fields: Sequence[str]) -> dict:
    """Build a dict payload of selected attributes for hashing."""

    def _normalise_value(value):
        """Return a JSON-safe representation for hashing."""

        if isinstance(value, datetime):
            return value.isoformat()

        return value

    return {field: _normalise_value(getattr(row, field)) for field in fields}


def _stable_hash(payload: dict) -> str:
    """Return a stable SHA-256 hash of the payload."""

    s = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()
