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

from datetime import timedelta

from npgmlwarehouse.db.schema import Sample, Study
from pytest import mark as m

from helpers import BEGIN, EARLY, LATE, LATEST
from npg_irods.db.mlwh_cache import (
    CACHE_CHUNK_SIZE,
    SAMPLE_KEY,
    STUDY_KEY,
    MlwhChangeCache,
)


@m.describe("Caching ML warehouse content hashes")
class TestMlwhChangeCache:
    @m.context("When the cache is empty and the MLWH contains existing entries")
    @m.it("Should create tables keyed by the configured MLWH attributes")
    def test_primes_existing_mlwh_entries(self, study_and_samples_mlwh, tmp_path):
        with MlwhChangeCache(tmp_path / "mlwh_cache.sqlite", prime_cache=True) as cache:
            sample_ids = cache.changed_sample_keys(
                study_and_samples_mlwh, BEGIN, LATEST
            )
            study_ids = cache.changed_study_keys(study_and_samples_mlwh, BEGIN, LATEST)
            conn = cache._open_conn()
            sample_columns = [
                row[1] for row in conn.execute("PRAGMA table_info(sample_cache)")
            ]
            study_columns = [
                row[1] for row in conn.execute("PRAGMA table_info(study_cache)")
            ]

        assert sample_ids == set()
        assert study_ids == set()
        assert sample_columns[0] == SAMPLE_KEY
        assert study_columns[0] == STUDY_KEY

    @m.context("When cached MLWH entries have content updates")
    @m.it("Should return those entries")
    def test_content_updates_returned(self, study_and_samples_mlwh, tmp_path):
        sample_ids = {"id_sample_lims1", "id_sample_lims2"}
        changed_sample_id = "id_sample_lims1"
        changed_sample_key = "82429892-0ab6-11ee-b5ba-fa163eac3ag7"

        cache_path = tmp_path / "mlwh_cache.sqlite"
        with MlwhChangeCache(cache_path, prime_cache=True) as cache:
            cache.changed_sample_keys(study_and_samples_mlwh, BEGIN, LATE)

        for sample in study_and_samples_mlwh.query(Sample).filter(
            Sample.id_sample_lims.in_(sample_ids)
        ):
            sample.recorded_at = LATEST

        changed_sample = (
            study_and_samples_mlwh.query(Sample)
            .filter_by(id_sample_lims=changed_sample_id)
            .one()
        )
        changed_sample.name = "updated sample name"
        study_and_samples_mlwh.commit()

        with MlwhChangeCache(cache_path) as cache:
            changed_samples = cache.changed_sample_keys(
                study_and_samples_mlwh, LATE, LATEST
            )

            assert changed_samples == {changed_sample_key}

    @m.context("When cached MLWH entries have timestamp-only updates")
    @m.it("Should not return them")
    def test_timestamp_changes_not_returned(self, study_and_samples_mlwh, tmp_path):
        sample_ids = {"id_sample_lims1", "id_sample_lims2"}

        cache_path = tmp_path / "mlwh_cache.sqlite"
        with MlwhChangeCache(cache_path, prime_cache=True) as cache:
            cache.changed_sample_keys(study_and_samples_mlwh, BEGIN, LATE)

        for sample in study_and_samples_mlwh.query(Sample).filter(
            Sample.id_sample_lims.in_(sample_ids)
        ):
            sample.recorded_at = LATEST

        study_and_samples_mlwh.commit()

        with MlwhChangeCache(cache_path) as cache:
            changed_samples = cache.changed_sample_keys(
                study_and_samples_mlwh, LATE, LATEST
            )

            assert changed_samples == set()

    @m.context("When more updated rows exist than the cache batch size")
    @m.it("Should return every changed sample ID")
    def test_changed_sample_ids_multiple_batches(self, mlwh_session, tmp_path):
        sample_keys = {
            f"00000000-0000-0000-0000-{n:012d}" for n in range(CACHE_CHUNK_SIZE + 1)
        }
        mlwh_session.add_all(
            Sample(
                id_lims="LIMS_01",
                id_sample_lims=f"sample_{n}",
                name=f"sample_{n}",
                uuid_sample_lims=sample_key,
                created=BEGIN - timedelta(days=2),
                last_updated=LATEST,
                recorded_at=LATEST,
                consent_withdrawn=0,
            )
            for n, sample_key in enumerate(sample_keys)
        )
        mlwh_session.commit()

        with MlwhChangeCache(tmp_path / "mlwh_cache.sqlite") as cache:
            changed_ids = cache.changed_sample_keys(mlwh_session, BEGIN, LATEST)

        assert changed_ids == sample_keys
