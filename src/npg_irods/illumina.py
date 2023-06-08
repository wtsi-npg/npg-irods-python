# -*- coding: utf-8 -*-
#
# Copyright © 2023 Genome Research Ltd. All rights reserved.
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

from enum import Enum, unique
from typing import Type

from partisan.exception import RodsError
from partisan.irods import make_rods_item
from sqlalchemy import asc
from sqlalchemy.orm import Session
from structlog import get_logger

from npg_irods.common import Component
from npg_irods.db.mlwh import IseqFlowcell, IseqProductMetrics
from npg_irods.metadata.lims import (
    is_managed_access,
    make_sample_acl,
    make_sample_metadata,
    make_study_metadata,
)
from npg_irods.metadata.common import SeqConcept

log = get_logger(__package__)


@unique
class TagIndex(Enum):
    """Sequencing tag indexes which have special meaning or behaviour."""

    BIN = 0
    """Tag index 0 is not a real tag i.e. there is no DNA sequence corresponding to it.
    Rather, it is a bin for reads that cannot be associated with any of the candidate
    tags in a pool after sequencing."""

    CONTROL_198 = 198

    CONTROL_888 = 888
    """Tag index 888 is conventionally used to indicate a control sample e.g. Phi X
    that has been added to a pool."""


class MetadataUpdate:
    def update_secondary_metadata(
        self, paths, mlwh_session, include_controls=False
    ) -> (int, int, int):
        """

        - A data object relating to a single sample instance e.g. a cram file for a
        single plex from a pool that has been de-multiplexed by identifying its indexing
        tag(s), will get sample metadata appropriate for that single sample. It will get
        study metadata (which includes appropriate opening of access controls) for the
        single study that sample is a member of.

        - A data objects relating to multiple samples that were sequenced separately and
        then had their sequence data merged will get sample metadata appropriate to all
        the constituent samples. They will get study metadata (which includes
        appropriate opening of access controls) only if all the samples are from the
        same study. A data object with mixed-study data will not be made accessible.

        - Data objects which contain control data from spiked-in controls e.g. Phi X
        where the control was not added as a member of a pool are treated as any other
        data object derived from the sample they were mixed with. They get no special
        treatment for metadata or permissions and are not considered members of any
        control study.

        - Data objects which contain control data from spiked-in controls e.g. Phi X
        where the control was added as a member of a pool (typically with tag index 198
        or 888) are treated as any other member of a pool and have their own identity as
        samples in LIMS. They get no special treatment for metadata or permissions and
        are considered members the appropriate control study.

        - Data objects which contain human data lacking explicit consent ("unconsented")





        Args:
            paths:
            mlwh_session:
            include_controls:

        Returns:

        """
        num_input, num_updated, num_errors = 0, 0, 0

        log.debug("Updating iRODS paths", n=len(paths))

        for path in paths:
            num_input += 1
            try:
                # This will work equally well for collections or data objects
                item = make_rods_item(path)
                secondary_metadata, acl = [], []

                component_avus = item.metadata(attr=SeqConcept.COMPONENT.value)
                for avu in component_avus:
                    c = Component.from_avu(avu)
                    log.debug(
                        "Not multiplexed" if c.tag_index is None else "Multiplexed",
                        path=item,
                        run=c.suid,
                        pos=c.position,
                        tag=c.tag_index,
                    )

                    for fc in find_flowcells_by_component(
                        mlwh_session, c, include_controls=include_controls
                    ):
                        secondary_metadata.extend(make_sample_metadata(fc.sample))
                        secondary_metadata.extend(make_study_metadata(fc.study))
                        acl.extend(make_sample_acl(fc.sample, fc.study))

                item.supersede_metadata(*secondary_metadata, history=True)
                keep = [ac for ac in item.acl() if is_managed_access(ac)]
                item.supersede_permissions(*keep, *acl)

                num_updated += 1

            except RodsError as re:
                log.error(re.message, code=re.code)
                num_errors += 1
                raise re
            except Exception as e:
                log.error(e, path=path)
                num_errors += 1
                raise e

        return num_input, num_updated, num_errors


def find_flowcells_by_component(
    sess: Session, component: Component, include_controls=False
) -> list[Type[IseqFlowcell]]:
    query = (
        sess.query(IseqFlowcell)
        .distinct()
        .join(IseqFlowcell.iseq_product_metrics)
        .filter(IseqProductMetrics.id_run == int(component.suid))
    )

    if component.position is not None:
        query = query.filter(IseqProductMetrics.position == component.position)

    match component.tag_index:
        case TagIndex.CONTROL_198.value | TagIndex.CONTROL_888.value if include_controls:
            query = query.filter(IseqProductMetrics.tag_index == component.tag_index)
        case TagIndex.CONTROL_198.value | TagIndex.CONTROL_888.value:
            raise ValueError(
                "Attempted to exclude controls for a query specifically requesting "
                f"control tag index {component.tag_index}"
            )
        case TagIndex.BIN.value:
            query = query.filter(IseqProductMetrics.tag_index.is_not(None))
        case int():
            query = query.filter(IseqProductMetrics.tag_index == component.tag_index)
        case None:
            query = query.filter(IseqProductMetrics.tag_index.is_(None))
        case _:
            raise ValueError(f"Invalid tag index {component.tag_index}")

    return query.order_by(asc(IseqFlowcell.id_iseq_flowcell_tmp)).all()
