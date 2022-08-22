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
from sqlalchemy.orm import Session
from datetime import datetime


def illumina_recently_changed(sess: Session, max_age: datetime):
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
            (Sample.recorded_at > max_age)
            | (Study.recorded_at > max_age)
            | (IseqFlowcell.recorded_at > max_age)
            | (IseqProductMetrics.last_changed > max_age)
        )
        .all()
    )
