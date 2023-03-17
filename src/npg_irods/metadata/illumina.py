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

from typing import List, Dict
from sqlalchemy.orm import Session, Query
from datetime import datetime

from npg_irods.db.mlwh import IseqFlowcell, IseqProductMetrics, Sample, Study
from npg_irods.metadata.lims import TrackedStudy, TrackedSample

column_to_attribute = {
    Study.accession_number: TrackedStudy.ACCESSION_NUMBER,
    Study.name: TrackedStudy.NAME,
    Study.study_title: TrackedStudy.TITLE,
    Study.id_study_lims: TrackedStudy.ID,
    Sample.accession_number: TrackedSample.ACCESSION_NUMBER,
    Sample.id_sample_lims: TrackedSample.ID,
    Sample.name: TrackedSample.NAME,
    Sample.public_name: TrackedSample.PUBLIC_NAME,
    Sample.common_name: TrackedSample.COMMON_NAME,
    Sample.supplier_name: TrackedSample.SUPPLIER_NAME,
    Sample.cohort: TrackedSample.COHORT,
    Sample.donor_id: TrackedSample.DONOR_ID,
    Sample.consent_withdrawn: TrackedSample.CONSENT_WITHDRAWN,
    IseqFlowcell.id_library_lims: "library_id",
    IseqProductMetrics.qc: "manual_qc",
    IseqFlowcell.primer_panel: "primer_panel",
}


def _recently_changed_query(sess: Session, start_time: datetime) -> Query:
    """
    Runs a query to find recently changed rows that correspond to
    irods metadata.

    Args:
       sess: An open SQL session.
       start_time: The datetime from which 'recent' is defined.

    Returns:
        SQLalchemy Query object.
    """
    return (
        sess.query(
            *[
                column.label(str(attribute))
                for column, attribute in column_to_attribute.items()
            ]
        )
        .distinct()
        .join(IseqFlowcell.sample)
        .join(IseqFlowcell.study)
        .join(IseqFlowcell.iseq_product_metrics)
        .filter(
            (Sample.recorded_at > start_time)
            | (Study.recorded_at > start_time)
            | (IseqFlowcell.recorded_at > start_time)
            | (IseqProductMetrics.last_changed > start_time)
        )
    )


def recently_changed(sess: Session, start_time: datetime) -> List[Dict]:
    """
    Gets recently changed metadata values and associates them with
    their attribute keys.

    Args:
        sess: An open SQL session.
        start_time: The datetime from which 'recent' is defined.

    Returns:
        List of dictionaries.
    """
    query = _recently_changed_query(sess, start_time)

    changed = []
    for response in query.all():
        response_dict = {
            key: getattr(response, str(key)) for key in column_to_attribute.values()
        }

        changed.append(response_dict)
    return changed
