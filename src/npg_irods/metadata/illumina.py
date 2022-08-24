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

from ml_warehouse.schema import Study, Sample, IseqFlowcell, IseqProductMetrics
from typing import List, Tuple
from sqlalchemy.orm import Session
from datetime import datetime
from npg_irods.metadata.lims import TrackedStudy, TrackedSample, avu_if_value
from partisan.irods import AVU


def _recently_changed_query(sess: Session, start_time: datetime) -> List[Tuple]:
    """
    Runs a query to find recently changed rows that correspond to
    irods metadata.

    Args:
       sess: An open SQL session.
       start_time: The datetime from which 'recent' is defined.

    Returns:
        List of tuples.
    """
    return (
        sess.query(
            Study.accession_number,
            Study.name,
            Study.study_title,
            Study.id_study_lims,
            Sample.accession_number,
            Sample.id_sample_lims,
            Sample.name,
            Sample.public_name,
            Sample.common_name,
            Sample.supplier_name,
            Sample.cohort,
            Sample.donor_id,
            Sample.consent_withdrawn,
            IseqFlowcell.id_library_lims,
            IseqProductMetrics.qc,
            IseqFlowcell.primer_panel,
        )
        .distinct()
        .join(
            IseqFlowcell.sample,
            IseqFlowcell.study,
            IseqFlowcell.iseq_product_metrics,
        )
        .filter(
            (Sample.recorded_at > start_time)
            | (Study.recorded_at > start_time)
            | (IseqFlowcell.recorded_at > start_time)
            | (IseqProductMetrics.last_changed > start_time)
        )
        .all()
    )


def recently_changed(sess: Session, start_time: datetime) -> List[List[AVU]]:
    """
    Gets recently changed metadata values and associates them with
    their attribute keys.

    Args:
        sess: An open SQL session.
        start_time: The datetime from which 'recent' is defined.

    Returns:
        List of dictionaries.
    """
    responses = _recently_changed_query(sess, start_time)
    attributes = [
        TrackedStudy.ACCESSION_NUMBER,
        TrackedStudy.NAME,
        TrackedStudy.TITLE,
        TrackedStudy.ID,
        TrackedSample.ACCESSION_NUMBER,
        TrackedSample.ID,
        TrackedSample.NAME,
        TrackedSample.PUBLIC_NAME,
        TrackedSample.COMMON_NAME,
        TrackedSample.SUPPLIER_NAME,
        TrackedSample.COHORT,
        TrackedSample.DONOR_ID,
        TrackedSample.CONSENT_WITHDRAWN,
        "library_id",
        "manual_qc",
        "primer_panel",
    ]
    changed = []
    for response in responses:
        avus = []
        for i in range(len(attributes)):
            avus.append(avu_if_value(attributes[i], response[i]))
        changed.append(avus)
    return changed
