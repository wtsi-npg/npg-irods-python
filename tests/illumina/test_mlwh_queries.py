# -*- coding: utf-8 -*-
#
# Copyright © 2020, 2022, 2023 Genome Research Ltd. All rights reserved.
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

from datetime import timedelta

from pytest import mark as m

from helpers import BEGIN, LATE, LATEST
from npg_irods.metadata import illumina
from npg_irods.metadata.lims import TrackedSample, TrackedStudy


@m.describe("Finding Illumina recently changed information in Illumina tables")
class TestIlluminaMLWarehouseQueries(object):
    @m.context("When given a datetime")
    @m.it("Finds rows updated since that datetime")
    def test_recently_changed(self, illumina_backfill_mlwh):
        late_expected = [
            {
                TrackedStudy.ACCESSION_NUMBER: "ST0000000001",
                TrackedStudy.NAME: "Recently Changed",
                TrackedStudy.TITLE: "Recently changed study",
                TrackedStudy.ID: "4000",
                TrackedSample.ACCESSION_NUMBER: "SA000002",
                TrackedSample.ID: "SAMPLE_02",
                TrackedSample.NAME: "Unchanged",
                TrackedSample.PUBLIC_NAME: "Unchanged",
                TrackedSample.COMMON_NAME: "Unchanged",
                TrackedSample.SUPPLIER_NAME: "Unchanged_supplier",
                TrackedSample.COHORT: "cohort_02",
                TrackedSample.DONOR_ID: "DONOR_02",
                TrackedSample.CONSENT_WITHDRAWN: 0,
                "library_id": "LIBRARY_01",
                "manual_qc": 0,
                "primer_panel": "Primer_panel_01",
            },
            {
                TrackedStudy.ACCESSION_NUMBER: "ST0000000002",
                TrackedStudy.NAME: "Unchanged",
                TrackedStudy.TITLE: "Unchanged study",
                TrackedStudy.ID: "5000",
                TrackedSample.ACCESSION_NUMBER: "SA000001",
                TrackedSample.ID: "SAMPLE_01",
                TrackedSample.NAME: "Recently changed",
                TrackedSample.PUBLIC_NAME: "Recently changed",
                TrackedSample.COMMON_NAME: "Recently changed",
                TrackedSample.SUPPLIER_NAME: "Recently_changed_supplier",
                TrackedSample.COHORT: "cohort_01",
                TrackedSample.DONOR_ID: "DONOR_01",
                TrackedSample.CONSENT_WITHDRAWN: 0,
                "library_id": "LIBRARY_02",
                "manual_qc": 0,
                "primer_panel": "Primer_panel_02",
            },
            {
                TrackedStudy.ACCESSION_NUMBER: "ST0000000002",
                TrackedStudy.NAME: "Unchanged",
                TrackedStudy.TITLE: "Unchanged study",
                TrackedStudy.ID: "5000",
                TrackedSample.ACCESSION_NUMBER: "SA000002",
                TrackedSample.ID: "SAMPLE_02",
                TrackedSample.NAME: "Unchanged",
                TrackedSample.PUBLIC_NAME: "Unchanged",
                TrackedSample.COMMON_NAME: "Unchanged",
                TrackedSample.SUPPLIER_NAME: "Unchanged_supplier",
                TrackedSample.COHORT: "cohort_02",
                TrackedSample.DONOR_ID: "DONOR_02",
                TrackedSample.CONSENT_WITHDRAWN: 0,
                "library_id": "LIBRARY_04",
                "manual_qc": 0,
                "primer_panel": "Primer_panel_04",
            },
            {
                TrackedStudy.ACCESSION_NUMBER: "ST0000000002",
                TrackedStudy.NAME: "Unchanged",
                TrackedStudy.TITLE: "Unchanged study",
                TrackedStudy.ID: "5000",
                TrackedSample.ACCESSION_NUMBER: "SA000002",
                TrackedSample.ID: "SAMPLE_02",
                TrackedSample.NAME: "Unchanged",
                TrackedSample.PUBLIC_NAME: "Unchanged",
                TrackedSample.COMMON_NAME: "Unchanged",
                TrackedSample.SUPPLIER_NAME: "Unchanged_supplier",
                TrackedSample.COHORT: "cohort_02",
                TrackedSample.DONOR_ID: "DONOR_02",
                TrackedSample.CONSENT_WITHDRAWN: 0,
                "library_id": "LIBRARY_03",
                "manual_qc": 0,
                "primer_panel": "Primer_panel_03",
            },
        ]
        before_early_expected = late_expected + [
            {
                TrackedStudy.ACCESSION_NUMBER: "ST0000000002",
                TrackedStudy.NAME: "Unchanged",
                TrackedStudy.TITLE: "Unchanged study",
                TrackedStudy.ID: "5000",
                TrackedSample.ACCESSION_NUMBER: "SA000002",
                TrackedSample.ID: "SAMPLE_02",
                TrackedSample.NAME: "Unchanged",
                TrackedSample.PUBLIC_NAME: "Unchanged",
                TrackedSample.COMMON_NAME: "Unchanged",
                TrackedSample.SUPPLIER_NAME: "Unchanged_supplier",
                TrackedSample.COHORT: "cohort_02",
                TrackedSample.DONOR_ID: "DONOR_02",
                TrackedSample.CONSENT_WITHDRAWN: 0,
                "library_id": "LIBRARY_05",
                "manual_qc": 0,
                "primer_panel": "Primer_panel_05",
            }
        ]
        before_early = BEGIN - timedelta(days=1)
        after_latest = LATEST + timedelta(days=1)

        # only recently updated
        assert illumina.recently_changed(illumina_backfill_mlwh, LATE) == late_expected
        # all
        assert (
            illumina.recently_changed(illumina_backfill_mlwh, before_early)
            == before_early_expected
        )
        # none
        assert illumina.recently_changed(illumina_backfill_mlwh, after_latest) == []
