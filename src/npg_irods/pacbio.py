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

"""PacBio-specific business logic API."""

from dataclasses import dataclass
from datetime import datetime
from pathlib import PurePath
from typing import Iterator, Optional, Type

from partisan.irods import AVU, Collection, DataObject
from sqlalchemy.orm import Session
from structlog import get_logger

from npg_irods.common import infer_zone, update_metadata, update_permissions
from npg_irods.db.mlwh import PacBioRun, SQL_CHUNK_SIZE, Sample, Study
from npg_irods.exception import DataObjectNotFound
from npg_irods.metadata.common import SeqConcept, SeqSubset
from npg_irods.metadata.lims import (
    ensure_consent_withdrawn,
    has_consent_withdrawn_metadata,
    make_sample_acl,
    make_sample_metadata,
    make_study_metadata,
)
from npg_irods.metadata.pacbio import Instrument, add_well_padding, remove_well_padding

log = get_logger(__package__)


@dataclass(order=True)
class Component:
    run_name: str
    well_label: str
    tag_sequence: str
    plate_number: Optional[int]
    subset: Optional[SeqSubset]

    def __init__(
        self,
        run_name: str,
        well_label: str,
        tag_sequence: str = None,
        plate_number: int = None,
        subset: str = None,
    ):
        self.run_name = run_name
        self.well_label = add_well_padding(well_label)
        self.tag_sequence = tag_sequence
        self.plate_number = plate_number
        self.subset = SeqSubset.from_string(subset)

    @classmethod
    def from_avus(cls, *avus):
        def _ensure_one_value(d: dict, k: str, required=False):
            if k not in d:
                if required:
                    raise ValueError(f"Missing required key: {k}")
                else:
                    return None

            vals = d[k]
            if len(vals) != 1:
                raise ValueError(
                    f"Expected one value for key {k} but got {len(vals)}: {vals}"
                )
            val = vals[0].value
            if k == Instrument.PLATE_NUMBER.value:
                return int(val)
            return val

        avu_dict = AVU.collate(*avus)

        args = []
        for key in [Instrument.RUN_NAME.value, Instrument.WELL_LABEL.value]:
            args.append(_ensure_one_value(avu_dict, key, required=True))

        kwargs = {}
        for key in [
            Instrument.PLATE_NUMBER.value,
            Instrument.TAG_SEQUENCE.value,
            SeqConcept.SUBSET.value,
        ]:
            kwargs[key] = _ensure_one_value(avu_dict, key, required=False)

        return Component(*args, **kwargs)


def ensure_secondary_metadata_updated(
    item: Collection | DataObject, mlwh_session: Session
) -> bool:
    """Update iRODS secondary metadata and permissions on PacBio data objects.

    Prerequisites:
      - The instance has `run`, `well` and optionally `tag_sequence` metadata (used
    to identify the constituent run / well / tag sequence components of the data).

    Args:
        item: A Collection or DataObject.
        mlwh_session: An open SQL session.

    Returns:
        True if updated.
    """
    if managed_access := requires_managed_access(item):
        log.debug("Requires managed access", path=item)
    else:
        log.debug("Does not require managed access", path=item)

    components = find_associated_components(item)
    log.debug("Found associated components", path=item, comp=components)

    zone = infer_zone(item)
    secondary_metadata, acl = [], []
    for c in components:
        runs = find_runs_by_component(mlwh_session, c)
        log.debug("Found associated run records", path=item, runs=runs, comp=c)
        for run in runs:
            secondary_metadata.extend(make_sample_metadata(run.sample))
            secondary_metadata.extend(make_study_metadata(run.study))
            if managed_access:
                acl.extend(make_sample_acl(run.sample, run.study, c.subset, zone=zone))

    secondary_metadata = sorted(set(secondary_metadata))
    acl = sorted(set(acl))

    meta_update = update_metadata(item, secondary_metadata)

    cons_update = perm_update = False
    if has_consent_withdrawn_metadata(item):
        log.info("Consent withdrawn", path=item)
        cons_update = ensure_consent_withdrawn(item)
    else:
        perm_update = update_permissions(item, acl)

    return any([meta_update, cons_update, perm_update])


def find_associated_components(item: Collection | DataObject) -> list[Component]:
    """Return a list of components associated with the given data object.

    The components are inferred from the data object's metadata. Usually, there will
    be only one component, but there are cases where the data was not deplexed and
    consequently has multiple tag sequences, yielding one component per run, well.
    tag sequence tuple.

    Args:
        item: A Collection or DataObject.

    Returns:
        A list of components.
    """
    if item.rods_type == Collection:
        raise DataObjectNotFound(
            "Failed to find an associated data object bearing component metadata. "
            "PacBio component metadata is only associated with data objects, while "
            f"{item} is a collection"
        )

    avus = [item.avu(Instrument.RUN_NAME), item.avu(Instrument.WELL_LABEL)]
    if item.metadata(SeqConcept.SUBSET):
        avus.append(item.avu(SeqConcept.SUBSET))

    # PacBio does not have explicit JSON component metadata like Illumina does. Instead
    # of parsing JSON for each component AVU, we have to construct the component by
    # combining run, well and tag_sequence AVUs appropriately.
    tags = item.metadata(Instrument.TAG_SEQUENCE)
    if len(tags) > 0:
        return [Component.from_avus(*avus, tag) for tag in tags]

    return [Component.from_avus(*avus)]


def requires_managed_access(obj: DataObject) -> bool:
    """Return True if the given data object requires managed access control.

    For example, data objects containing primary sequence or genotype data should be
    managed.
    """
    # Is the following the best way - the Perl code relies on the object having
    # the "source" key in it iRODS metadata
    managed_access = len(obj.metadata(Instrument.SOURCE)) > 0

    # Extra check here because we expect certain types to be managed and if we find they
    # are not, it warrants a warning.
    if not managed_access and any(
        suffix in [".bam", ".h5", ".fasta"] for suffix in PurePath(obj.name).suffixes
    ):
        log.warning(
            f"Sequence object does not have managed access; the "
            f"'{Instrument.SOURCE}' AVU is missing from its metadata",
            path=obj,
        )

    return managed_access


# We could optimise this to use a single query to the ML warehouse to get all the
# run records for a given set of components if the components differ in only the tag
# i.e. where run = x and well = y and tag_sequence in (a, b, c, d). However, this will
# only be useful where the data object contains non-deplexed data (and hence lots of
# tag sequences), which is uncommon.
def find_runs_by_component(
    sess: Session, component: Component
) -> list[Type[PacBioRun]]:
    """Query the ML warehouse for PacBio run information for the given component.

    Args:
        sess: An open SQL session.
        component: A component.

    Returns:
        The associated run records.
    """
    query = sess.query(PacBioRun).filter(
        PacBioRun.pac_bio_run_name == component.run_name,
        PacBioRun.well_label == remove_well_padding(component.well_label),
    )

    if component.tag_sequence is not None:
        query = query.filter(PacBioRun.tag_sequence == component.tag_sequence)
    if component.plate_number is not None:
        query = query.filter(PacBioRun.plate_number == component.plate_number)

    order = [PacBioRun.pac_bio_run_name, PacBioRun.well_label]
    if component.tag_sequence is not None:
        order.append(PacBioRun.tag_sequence)

    query = query.order_by(*order)

    return query.all()


def find_updated_components(
    sess: Session, since: datetime, until: datetime
) -> Iterator[Component]:
    """Find in the ML warehouse any PacBio sequence components whose tracking
    metadata has been changed within the given time range.

    A change is defined as the "recorded_at" column (Sample, Study, PacBioRun)
    having a timestamp within the range.

    Args:
        sess: An open ML warehouse session.
        since: The start of the time range.
        until: The end of the time range.

    Returns:
        An iterator over Components whose tracking metadata have changed.
    """

    query = (
        sess.query(
            PacBioRun.pac_bio_run_name,
            PacBioRun.well_label,
            PacBioRun.plate_number,
            PacBioRun.tag_sequence,
        )
        .distinct()
        .join(PacBioRun.sample)
        .join(PacBioRun.study)
        .filter(
            Sample.recorded_at.between(since, until)
            | Study.recorded_at.between(since, until)
            | PacBioRun.recorded_at.between(since, until)
        )
        .order_by(
            PacBioRun.pac_bio_run_name,
            PacBioRun.well_label,
            PacBioRun.plate_number,
            PacBioRun.tag_sequence,
        )
    )

    for run, well, plate, tag in query.yield_per(SQL_CHUNK_SIZE):
        yield Component(run, well, plate_number=plate, tag_sequence=tag)
