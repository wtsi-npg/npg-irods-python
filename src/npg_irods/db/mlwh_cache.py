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
from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import batched
from pathlib import Path
from sqlite3 import Connection, connect
from typing import Iterable, Sequence

from npgmlwarehouse.db.schema import Sample, Study
from sqlalchemy import asc
from sqlalchemy.orm import Session
from structlog import get_logger

from npg_irods.db.mlwh import find_updated_samples, find_updated_studies


def logger():
    return get_logger(__name__)


HASH_SCHEMA_VERSION = 1
SQLITE_BUSY_TIMEOUT_MS = 5000
CACHE_CHUNK_SIZE = (
    500  # Don't make this bigger than 32,766 (maximum size of an SQLite IN clause)
)

SAMPLE_HASH_COLS = (
    "accession_number",
    "cohort",
    "common_name",
    "consent_withdrawn",
    "date_of_consent_withdrawn",
    "donor_id",
    "id_lims",
    "id_sample_lims",
    "marked_as_consent_withdrawn_by",
    "name",
    "organism",
    "public_name",
    "sanger_sample_id",
    "supplier_name",
)

STUDY_HASH_COLS = (
    "accession_number",
    "contains_human_dna",
    "contaminated_human_dna",
    "data_access_group",
    "description",
    "ega_dac_accession_number",
    "ena_project_id",
    "id_lims",
    "id_study_lims",
    "name",
    "remove_x_and_autosomes",
    "separate_y_chromosome_data",
    "study_title",
    "study_visibility",
)

SAMPLE_KEY = "uuid_sample_lims"
STUDY_KEY = "uuid_study_lims"


SAMPLE_CACHE_CREATE_SQL = (
    "CREATE TABLE IF NOT EXISTS sample_cache ("
    f"{SAMPLE_KEY} TEXT PRIMARY KEY, "
    "content_hash TEXT NOT NULL, "
    "hash_schema_version INTEGER NOT NULL, "
    "last_changed_at TEXT NOT NULL)"
)
STUDY_CACHE_CREATE_SQL = (
    "CREATE TABLE IF NOT EXISTS study_cache ("
    f"{STUDY_KEY} TEXT PRIMARY KEY, "
    "content_hash TEXT NOT NULL, "
    "hash_schema_version INTEGER NOT NULL, "
    "last_changed_at TEXT NOT NULL)"
)

SAMPLE_CACHE_UPSERT_SQL = (
    f"INSERT INTO sample_cache ({SAMPLE_KEY}, content_hash, hash_schema_version, last_changed_at) "
    "VALUES (?, ?, ?, ?) "
    f"ON CONFLICT({SAMPLE_KEY}) DO UPDATE SET "
    "content_hash=excluded.content_hash, "
    "hash_schema_version=excluded.hash_schema_version, "
    "last_changed_at=excluded.last_changed_at "
    "WHERE content_hash != excluded.content_hash "
    "OR hash_schema_version != excluded.hash_schema_version"
)
STUDY_CACHE_UPSERT_SQL = (
    f"INSERT INTO study_cache ({STUDY_KEY}, content_hash, hash_schema_version, last_changed_at) "
    "VALUES (?, ?, ?, ?) "
    f"ON CONFLICT({STUDY_KEY}) DO UPDATE SET "
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

    Attributes:
        path: Filesystem path to the SQLite cache file.
        hash_schema_version: Version number for the hashing schema.
        busy_timeout_ms: SQLite busy timeout in milliseconds.
        prime_cache: When True, populate the cache with all rows before filtering
            for changes.
    """

    path: Path
    hash_schema_version: int = HASH_SCHEMA_VERSION
    busy_timeout_ms: int = SQLITE_BUSY_TIMEOUT_MS
    prime_cache: bool = False

    _conn: Connection | None = None

    def __enter__(self):
        """Open the cache and ensure required tables exist.

        Returns:
            The open cache instance.
        """
        path = self.path.resolve()
        path.parent.mkdir(parents=True, exist_ok=True)

        self._conn = connect(path.as_posix(), timeout=self.busy_timeout_ms / 1000)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")

        self._conn.execute(SAMPLE_CACHE_CREATE_SQL)  # Create if missing
        self._conn.execute(STUDY_CACHE_CREATE_SQL)  # Create if missing
        self._conn.commit()

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

    def changed_sample_keys(
        self, mlwh_sess: Session, since: datetime, until: datetime
    ) -> set[str]:
        """Return Sample IDs with content changes in the given time range.

        Args:
            mlwh_sess: Open SQLAlchemy session for the ML warehouse.
            since: Start of the recorded_at time window.
            until: End of the recorded_at time window.

        Returns:
            Set of sample IDs whose content has changed since the last cache run.
        """
        if self.prime_cache:
            total = self._prime_cache(
                mlwh_sess, Sample, SAMPLE_KEY, SAMPLE_HASH_COLS, _upsert_sample_cache
            )

            logger().info("Primed sample cache", cache=self.path.as_posix(), rows=total)
            return set()

        samples = find_updated_samples(mlwh_sess, since, until)

        return self._filter_changed_rows(
            SAMPLE_KEY,
            SAMPLE_HASH_COLS,
            samples,
            _load_sample_cache,
            _upsert_sample_cache,
        )

    def changed_study_keys(
        self, mlwh_sess: Session, since: datetime, until: datetime
    ) -> set[str]:
        """Return Study IDs with content changes in the given time range.

        Args:
            mlwh_sess: Open SQLAlchemy session for the ML warehouse.
            since: Start of the recorded_at time window.
            until: End of the recorded_at time window.

        Returns:
            Set of study IDs whose content has changed since the last cache run.
        """
        if self.prime_cache:
            total = self._prime_cache(
                mlwh_sess, Study, STUDY_KEY, STUDY_HASH_COLS, _upsert_study_cache
            )

            logger().info("Primed study cache", cache=self.path.as_posix(), rows=total)
            return set()

        studies = find_updated_studies(mlwh_sess, since, until)

        return self._filter_changed_rows(
            STUDY_KEY, STUDY_HASH_COLS, studies, _load_study_cache, _upsert_study_cache
        )

    def _open_conn(self) -> Connection:
        """Return the open SQLite connection or raise if not open."""
        if self._conn is None:
            raise RuntimeError("Cache is not open")

        return self._conn

    def _prime_cache(
        self,
        mlwh_sess: Session,
        model,
        id_attr: str,
        hash_cols: Sequence[str],
        cache_upsert_fn,
    ) -> int:
        """Insert hashes for all rows in the model into the cache.

        Args:
            mlwh_sess: An open MLWH session.
            model: An MLWH model (Sample or Study)
            id_attr: A unique identifier for a model row.
            hash_cols: Columns in the row that will be used to create a content hash.
            cache_upsert_fn:

        Returns:

        """
        updates: list[tuple[str, str, int, str]] = []
        total = 0
        query = mlwh_sess.query(model).order_by(asc(getattr(model, id_attr)))

        conn = self._open_conn()
        for row in query.yield_per(CACHE_CHUNK_SIZE):
            row_id = getattr(row, id_attr)
            content_hash = _stable_hash(_payload(row, hash_cols))
            now = datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat()
            updates.append((row_id, content_hash, self.hash_schema_version, now))

            if len(updates) >= CACHE_CHUNK_SIZE:
                cache_upsert_fn(conn, updates)
                total += len(updates)
                updates.clear()

        if updates:
            cache_upsert_fn(conn, updates)
            total += len(updates)

        return total

    def _filter_changed_rows(
        self,
        id_attr: str,
        hash_fields: Sequence[str],
        candidate_rows: Iterable[Sample | Study],
        cache_load_fn,
        cache_upsert_fn,
    ) -> set[str]:
        """Return IDs whose cached hashes differ from the current content."""
        changed: set[str] = set()

        conn = self._open_conn()
        num_candidates, num_updates = 0, 0
        for rows in batched(candidate_rows, CACHE_CHUNK_SIZE):
            cache_map = cache_load_fn(conn, [getattr(row, id_attr) for row in rows])
            updates: list[tuple[str, str, int, str]] = []

            now = datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat()
            for row in rows:
                num_candidates += 1
                row_id = getattr(row, id_attr)
                content_hash = _stable_hash(_payload(row, hash_fields))
                cached = cache_map.get(row_id)

                clog = logger().bind(attr=id_attr, row=row_id, fields=hash_fields)

                update = True
                if cached is None:
                    clog.debug("No current cached value; updating the cache")
                elif cached[1] != self.hash_schema_version:
                    clog.debug("Hash schema changed; updating the cache")
                elif cached[0] != content_hash:
                    clog.debug("Hash value changed; updating the cache")
                else:
                    clog.debug("Hash unchanged; skipping cache update")
                    update = False

                if update:
                    num_updates += 1
                    changed.add(row_id)
                    updates.append(
                        (row_id, content_hash, self.hash_schema_version, now)
                    )

            cache_upsert_fn(conn, updates)

        logger().info(
            "Filtered candidate updates",
            attr=id_attr,
            num_candidates=num_candidates,
            num_updates=num_updates,
        )

        return changed


def _load_sample_cache(conn: Connection, ids: list[str]) -> dict:
    """Load cached sample hashes for the given IDs."""
    if not ids:
        return {}

    placeholders = ",".join("?" for _ in ids)
    query = (
        f"SELECT {SAMPLE_KEY}, content_hash, hash_schema_version "
        f"FROM sample_cache WHERE {SAMPLE_KEY} IN ({placeholders})"
    )
    rows = conn.execute(query, ids).fetchall()
    logger().debug(
        "Loaded sample cache rows", num_requested=len(ids), num_loaded=len(rows)
    )

    return {row[0]: (row[1], row[2]) for row in rows}


def _load_study_cache(conn: Connection, ids: list[str]) -> dict:
    """Load cached study hashes for the given IDs."""
    if not ids:
        return {}

    placeholders = ",".join("?" for _ in ids)
    query = (
        f"SELECT {STUDY_KEY}, content_hash, hash_schema_version "
        f"FROM study_cache WHERE {STUDY_KEY} IN ({placeholders})"
    )
    rows = conn.execute(query, ids).fetchall()
    logger().debug(
        "Loaded study cache rows", num_requested=len(ids), num_loaded=len(rows)
    )

    return {row[0]: (row[1], row[2]) for row in rows}


def _upsert_sample_cache(
    conn: Connection, updates: list[tuple[str, str, int, str]]
) -> None:
    """Insert or update sample cache rows."""
    if not updates:
        logger().debug("No sample cache rows to upsert")
        return

    conn.executemany(SAMPLE_CACHE_UPSERT_SQL, updates)
    conn.commit()
    logger().debug("Upserted new sample cache rows", n=len(updates))


def _upsert_study_cache(
    conn: Connection, updates: list[tuple[str, str, int, str]]
) -> None:
    """Insert or update study cache rows."""
    if not updates:
        logger().debug("No study cache rows to upsert")
        return

    conn.executemany(STUDY_CACHE_UPSERT_SQL, updates)
    conn.commit()
    logger().debug("Upserted new study cache rows", n=len(updates))


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
