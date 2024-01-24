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

import json
import re
from dataclasses import dataclass
from datetime import datetime
from enum import Enum, unique
from pathlib import PurePath
from typing import Iterator, Optional, Type

from partisan.irods import AVU, Collection, DataObject
from partisan.metadata import AsValueEnum
from sqlalchemy import asc
from sqlalchemy.orm import Session
from structlog import get_logger

from npg_irods.common import infer_zone, update_metadata, update_permissions
from npg_irods.db.mlwh import IseqFlowcell, IseqProductMetrics, Sample, Study
from npg_irods.exception import CollectionNotFound, DataObjectNotFound, NonUniqueError
from npg_irods.metadata.common import SeqConcept, SeqSubset
from npg_irods.metadata.illumina import Instrument
from npg_irods.metadata.lims import (
    ensure_consent_withdrawn,
    has_consent_withdrawn_metadata,
    make_reduced_sample_metadata,
    make_reduced_study_metadata,
    make_sample_acl,
    make_sample_metadata,
    make_study_metadata,
)

log = get_logger(__package__)


@unique
class TagIndex(Enum):
    """Sequencing tag indexes which have special meaning or behaviour."""

    BIN = 0
    """Tag index 0 is not a real tag i.e. there is no DNA sequence corresponding to it.
    Rather, it is a bin for reads that cannot be associated with any of the candidate
    tags in a pool after sequencing."""


@unique
class EntityType(AsValueEnum):
    """The type of sequenced material applied to a flowcell. This related to the
    entity_type column in the MLWH. The values are defined in the MLWH schema
    metadata."""

    LIBRARY = "library"
    LIBRARY_CONTROL = "library_control"
    LIBRARY_INDEXED = "library_indexed"
    LIBRARY_INDEXED_SPIKE = "library_indexed_spike"


@dataclass(order=True)
class Component:
    """A set of reads from an Illumina sequencing run."""

    id_run: int
    """The run ID generated by WSI tracking database."""

    position: int
    """The 1-based instrument position where the sample was sequenced."""

    tag_index: Optional[int]
    """The 1-based index in a pool of tags, if multiplexed."""

    subset: Optional[SeqSubset]
    """The subset of the reads for this run/position/tag index, if filtered."""

    @classmethod
    def from_avu(cls, avu: AVU):
        """Return a new Component instance by parsing the value of an Illumina
        `component` AVU from iRODS."""
        try:
            if avu.attribute != SeqConcept.COMPONENT.value:
                raise ValueError(
                    f"Cannot create a Component from metadata {avu}; "
                    f"invalid attribute {avu.attribute}"
                )

            avu_value = json.loads(avu.value)
            subset = avu_value.get(SeqConcept.SUBSET.value, None)

            return Component(
                avu_value[Instrument.RUN.value],
                avu_value[SeqConcept.POSITION.value],
                tag_index=avu_value.get(SeqConcept.TAG_INDEX.value, None),
                subset=subset,
            )
        except Exception as e:
            raise ValueError(
                f"Failed to create a Component from metadata {avu}: {e}",
            ) from e

    def __init__(
        self, id_run: int, position: int, tag_index: int = None, subset: str = None
    ):
        self.id_run = id_run
        self.position = position
        self.tag_index = int(tag_index) if tag_index is not None else None

        match subset:
            case SeqSubset.HUMAN.value:
                self.subset = SeqSubset.HUMAN
            case SeqSubset.XAHUMAN.value:
                self.subset = SeqSubset.XAHUMAN
            case SeqSubset.YHUMAN.value:
                self.subset = SeqSubset.YHUMAN
            case SeqSubset.PHIX.value:
                self.subset = SeqSubset.PHIX
            case None:
                self.subset = None
            case _:
                raise ValueError(f"Invalid subset '{subset}'")

    def contains_nonconsented_human(self):
        """Return True if this component contains non-consented human sequence."""
        return self.subset is not None and self.subset in [
            SeqSubset.HUMAN,
            SeqSubset.XAHUMAN,
        ]

    def __repr__(self):
        rep = {
            Instrument.RUN.value: self.id_run,
            SeqConcept.POSITION.value: self.position,
        }
        if self.tag_index is not None:
            rep[SeqConcept.TAG_INDEX.value] = self.tag_index
        if self.subset is not None:
            rep[SeqConcept.SUBSET.value] = self.subset.value

        return json.dumps(rep, sort_keys=True, separators=(",", ":"))


def ensure_secondary_metadata_updated(
    item: Collection | DataObject, mlwh_session, include_controls=False
) -> bool:
    """Update iRODS secondary metadata and permissions on Illumina run collections
    and data objects.

    Prerequisites:
      - The instance has `component` metadata (used to identify the constituent
    run / position / tag index components of the data).

    - Instances relating to a single sample instance e.g. a cram file for a single
    plex from a pool that has been de-multiplexed by identifying its indexing tag(s),
    will get sample metadata appropriate for that single sample. They will get study
    metadata (which includes appropriate opening of access controls) for the
    single study that sample is a member of.

    - Instances relating to multiple samples that were sequenced separately and
    then had their sequence data merged will get sample metadata appropriate to all
    the constituent samples. They will get study metadata (which includes
    appropriate opening of access controls) only if all the samples are from the
    same study. A data object with mixed-study data will not be made accessible.

    - Instances which contain control data from spiked-in controls e.g. Phi X
    where the control was not added as a member of a pool are treated as any other
    data object derived from the sample they were mixed with. They get no special
    treatment for metadata or permissions and are not considered members of any
    control study.

    - Instances which contain control data from spiked-in controls e.g. Phi X
    where the control was added as a member of a pool (typically with tag index 198
    or 888) are treated as any other member of a pool and have their own identity as
    samples in LIMS. They get no special treatment for metadata or permissions and
    are considered members the appropriate control study.

    - Instances which contain human data lacking explicit consent ("unconsented")
    are treated the same way as human samples with consent withdrawn with respect to
    permissions i.e. all access permissions are removed, leaving only permissions
    for the current user (who is making these changes) and for any rodsadmin users
    who currently have access.

    Args:
        item: A Collection or DataObject.
        mlwh_session: An open SQL session.
        include_controls: If True, include any control samples in the metadata and
        permissions.

    Returns:
       True if updated.
    """
    zone = infer_zone(item)
    secondary_metadata, acl = [], []

    def empty_acl(*args):
        return []

    if requires_full_metadata(item):
        log.debug("Requires full metadata", path=item)
        sample_fn, study_fn = make_sample_metadata, make_study_metadata
    else:
        log.debug("Requires reduced metadata", path=item)
        sample_fn, study_fn = make_reduced_sample_metadata, make_reduced_study_metadata

    if requires_managed_access(item):
        log.debug("Requires managed access", path=item)
        acl_fn = make_sample_acl
    else:
        log.debug("Does not require managed access", path=item)
        acl_fn = empty_acl

    # Each component may be associated with multiple flowcells
    components = find_associated_components(item)
    log.debug("Found associated components", path=item, comp=components)

    for c in components:
        flowcells = find_flowcells_by_component(
            mlwh_session, c, include_controls=include_controls
        )
        log.debug("Found associated flowcells", path=item, flowcells=flowcells, comp=c)
        for fc in flowcells:
            secondary_metadata.extend(sample_fn(fc.sample))
            secondary_metadata.extend(study_fn(fc.study))
            acl.extend(acl_fn(fc.sample, fc.study, c.subset, zone=zone))

    # Remove duplicates
    secondary_metadata = sorted(set(secondary_metadata))
    acl = sorted(set(acl))

    meta_update = update_metadata(item, secondary_metadata)

    cons_update = xahu_update = perm_update = False
    if has_consent_withdrawn_metadata(item):
        log.info("Consent withdrawn", path=item)
        cons_update = ensure_consent_withdrawn(item)
    elif any(c.contains_nonconsented_human() for c in components):  # Illumina specific
        log.info("Non-consented human data", path=item)
        xahu_update = update_permissions(item, acl)
    else:
        perm_update = update_permissions(item, acl)

    return any([meta_update, cons_update, xahu_update, perm_update])


def without_suffixes(path: PurePath) -> PurePath:
    """Return a copy of path with all suffixes removed."""
    p = PurePath(path)
    while p.suffix != "":
        p = p.with_suffix("")
    return p


def is_qc_data_object(item: DataObject | Collection) -> bool:
    """Return True if the given data object is in the qc sub-collection."""
    return item.rods_type == DataObject and item.path.parent.name == "qc"


def split_name(name: str) -> tuple[str, str]:
    """Split the name of an Illumina data object into its stem and suffixes.

    In most cases, the stem can be determined using the Python pathlib API. However,
    there are a set of historical naming inconsistencies that require special handling.
    This function handles those cases or delegates to the pathlib API if the name is
    consistent with the default naming convention.

    Extending this method to handle new types of file:

    If your new file can be handled by the pathlib API then you do not need to extend
    the capabilities of this function. Otherwise, you will need to add an additional
    regular expression to parse the stem from the file name.

    """

    # Handle this form:
    #
    # 9930555.ACXX.paired158.550b751b96_F0x900.stats
    # 9930555.ACXX.paired158.550b751b96_F0xB00.stats
    # 9930555.ACXX.paired158.550b751b96_F0xF04_target.stats
    if match := re.match(r"(\d+\.\w+\.\w+\.\w+)(_F0x\d+.*)$", name):
        stem, suffixes = match.groups()

    # Handle this form:
    #
    # 9930555.ACXX.paired158.550b751b96.flagstat
    # 9930555.ACXX.paired158.550b751b96.g.vcf.gz
    elif match := re.match(r"(\d+\.\w+\.\w+\.\w+)(.*)$", name):
        stem, suffixes = match.groups()

    # Handle this form:
    #
    # [prefix]_F0x900.stats
    # [prefix]_F0xB00.stats
    # [prefix]_F0xF04_target.stats
    # [prefix]_F0xF04_target_autosome.stats
    elif match := re.match(r"([\w#]+)(_F0x\d+.*)$", name):
        stem, suffixes = match.groups()

    # Handle this form:
    #
    # [prefix]_quality_cycle_caltable.txt
    # [prefix]_quality_cycle_surv.txt
    # [prefix]_quality_error.txt
    elif match := re.match(r"([\w#]+)(_quality_\.txt)$", name):
        stem, suffixes = match.groups()

    else:
        p = PurePath(name)
        stem = without_suffixes(p)
        suffixes = "".join(p.suffixes)

    return stem, suffixes


def find_associated_components(item: DataObject | Collection) -> list[Component]:
    """Return a list of Illumina components associated with the given item. Components
    allow us to look up the associated sample and study metadata in the ML Warehouse.

    The iRODS metadata describing the Components may be on the item itself, or it may
    be on another, related data object. Why is it not always on the item itself? This is
    to work around the limited vertical scaling possible within an iRODS zone; our
    metadata link table already contains >3 billion rows, so we cannot afford to add
    metadata to every data that we would like, even though that would be the simplest
    and most explicit method.

    All data objects containing primary sequence data (BAM, CRAM) have Component
    metadata attached. Ancillary files, such as JSON files containing QC metrics, do not
    and their Components must be inferred from the file name.

    In most cases the file name association is straightforward; the file stem of the
    BAM/CRAM file and its associated ancillary files are identical. The stem can be
    determined using the Python pathlib API. However, there are a set of historical
    naming inconsistencies that require special handling.

    Extending this function to handle new types of file:

    - If your new file respects the naming convention described above (related files
    have the same stem) and the stem can be determined using the pathlib API then you
    can simply add the file to iRODS and whenever its path is passed to this function,
    the correct Components will be returned.

    - If your new file does not respect the default naming convention, then you will
    need to extend the split_name() function to handle the new file type.

    Args:
        item:

    Returns:

    """
    errmsg = "Failed to find an associated data object bearing component metadata"

    if item.rods_type == Collection:
        raise DataObjectNotFound(
            f"{errmsg}. Illumina component metadata is only associated with data "
            f"objects, while {item} is a collection"
        )
    item_stem, item_suffix = split_name(item.name)

    # The item itself holds the associated metadata (true for BAM and CRAM files)
    if item_suffix in [".bam", ".cram"]:
        return [Component.from_avu(avu) for avu in item.metadata(SeqConcept.COMPONENT)]

    # Try to find the associated BAM or CRAM file. This will be in the same collection,
    # except in the case of QC data objects, where it will be in the parent collection.
    if is_qc_data_object(item):
        coll = Collection(item.path.parent.parent)
    else:
        coll = Collection(item.path.parent)

    if not coll.exists():
        raise CollectionNotFound(
            f"{errmsg} in this collection (path does not exist)", path=coll
        )

    bams, crams = [], []
    for obj in coll.iter_contents():
        if obj.rods_type != DataObject:
            continue

        stem, suffix = split_name(obj.name)
        if stem != item_stem:
            continue

        if suffix == ".bam":
            bams.append(obj)
        elif suffix == ".cram":
            crams.append(obj)

    associated = crams if len(crams) > 0 else bams

    if len(associated) == 0:
        raise DataObjectNotFound(f"{errmsg} for {item} in {coll}", path=item)
    if len(associated) > 1:
        raise NonUniqueError(
            f"{errmsg}. Multiple associated data objects for {item} "
            f"found in {coll}: {associated}",
            path=item,
            observed=associated,
        )

    obj = associated.pop()

    return [Component.from_avu(avu) for avu in obj.metadata(SeqConcept.COMPONENT)]


def requires_full_metadata(obj: DataObject) -> bool:
    """Return True if the given data object requires full metadata.

    Ideally we wouldn't have a special cases, however, we need to be economical with
    metadata storage because the iRODS metadata link table is already >3 billion rows,
    which is impacting query performance.
    """
    full = [".bam", ".cram"]
    return any(suffix in full for suffix in PurePath(obj.name).suffixes)


def requires_managed_access(obj: DataObject) -> bool:
    """Return True if the given data object requires managed access control.

    For example, data objects containing primary sequence or genotype data should be
    managed.
    """
    managed = [
        ".bam",
        ".bed",
        ".cram",
        ".fasta",
        ".fastq",
        ".gatk_collecthsmetrics",
        ".genotype",
        ".tab",
        ".vcf",
        ".zip",
    ]
    return any(suffix in managed for suffix in PurePath(obj.name).suffixes)


def has_component_metadata(item: Collection | DataObject) -> bool:
    """Return True if the given item has Illumina component metadata."""
    return len(item.metadata(SeqConcept.COMPONENT)) > 0


def find_qc_collection(path: Collection | DataObject) -> Collection:
    qc = Collection(path.path / "qc")
    if not qc.exists():
        raise CollectionNotFound(qc, path=qc)
    return qc


def find_flowcells_by_component(
    sess: Session, component: Component, include_controls=False
) -> list[Type[IseqFlowcell]]:
    """Query the ML warehouse for flowcell information for the given component.

    Args:
        sess: An open SQL session.
        component: A component
        include_controls: If False, include query arguments to exclude spiked-in
            controls in the result. Defaults to False.

    Returns:
        The associated flowcells.
    """
    query = (
        sess.query(IseqFlowcell)
        .distinct()
        .join(IseqFlowcell.iseq_product_metrics)
        .filter(
            IseqProductMetrics.id_run == component.id_run,
            IseqFlowcell.position == component.position,
        )
    )

    if not include_controls:
        query = query.filter(
            IseqFlowcell.entity_type.notin_(
                [
                    EntityType.LIBRARY_CONTROL.value,
                    EntityType.LIBRARY_INDEXED_SPIKE.value,
                ]
            )
        )

    match component.tag_index:
        case TagIndex.BIN.value:
            query = query.filter(IseqProductMetrics.tag_index.isnot(None))
        case int():
            query = query.filter(IseqProductMetrics.tag_index == component.tag_index)
        case None:
            query = query.filter(IseqProductMetrics.tag_index.is_(None))
        case _:
            raise ValueError(f"Invalid tag index {component.tag_index}")

    return query.order_by(asc(IseqFlowcell.id_iseq_flowcell_tmp)).all()


def find_updated_components(
    sess: Session, since: datetime, until: datetime
) -> Iterator[Component]:
    """Find in the ML warehouse any Illumina sequence components whose tracking
    metadata has been changed within a specified time range

    A change is defined as the "recorded_at" column (Sample, Study, IseqFlowcell) or
    "last_changed" colum (IseqProductMetrics) having a timestamp more recent than the
    given time.

    Args:
        sess: An open SQL session.
        since: A datetime.
        until: A datetime.

    Returns:
        An iterator over Components whose tracking metadata have changed.
    """
    for rpt in (
        sess.query(
            IseqProductMetrics.id_run, IseqFlowcell.position, IseqFlowcell.tag_index
        )
        .distinct()
        .join(IseqFlowcell.sample)
        .join(IseqFlowcell.study)
        .join(IseqFlowcell.iseq_product_metrics)
        .filter(
            Sample.recorded_at.between(since, until)
            | Study.recorded_at.between(since, until)
            | IseqFlowcell.recorded_at.between(since, until)
            | IseqProductMetrics.last_changed.between(since, until)
        )
        .order_by(
            asc(IseqProductMetrics.id_run),
            asc(IseqFlowcell.position),
            asc(IseqFlowcell.tag_index),
        )
    ):
        yield Component(*rpt)
