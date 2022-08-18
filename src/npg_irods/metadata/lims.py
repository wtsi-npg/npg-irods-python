# -*- coding: utf-8 -*-
#
# Copyright © 2021, 2022 Genome Research Ltd. All rights reserved.
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

from datetime import datetime
from enum import unique
from itertools import starmap
from typing import List

from ml_warehouse.schema import Sample, Study
from partisan.irods import AC, AVU, Permission

from partisan.metadata import AsValueEnum, DublinCore


@unique
class TrackedSample(AsValueEnum):
    """SequenceScape Sample metadata."""

    ACCESSION_NUMBER = "sample_accession_number"
    COHORT = "sample_cohort"
    COMMON_NAME = "sample_common_name"
    CONSENT = "sample_consent"
    CONSENT_WITHDRAWN = "sample_consent_withdrawn"
    CONTROL = "sample_control"
    DONOR_ID = "sample_donor_id"
    ID = "sample_id"
    NAME = "sample"
    PUBLIC_NAME = "sample_public_name"
    SUPPLIER_NAME = "sample_supplier_name"

    def __str__(self):
        return str(self.__repr__())


@unique
class TrackedStudy(AsValueEnum):
    """SequenceScape Study metadata."""

    ACCESSION_NUMBER = "study_accession_number"
    ID = "study_id"
    NAME = "study"
    TITLE = "study_title"

    def __str__(self):
        return str(self.__repr__())


@unique
class SeqConcept(AsValueEnum):
    """Sequencing terminology."""

    TAG_INDEX = "tag_index"
    REFERENCE = "reference"
    COMPONENT = "component"
    ID_PRODUCT = "id_product"
    ALT_PROCESS = "alt_process"

    def __str__(self):
        return str(self.__repr__())

    def __eq__(self, other):
        return str(self) == str(other)


def make_sample_metadata(sample: Sample) -> List[AVU]:
    """Return standard iRODS metadata for a Sample:

     - sample ID
     - sample name
     - sample accession
     - sample donor ID
     - sample supplier name
     - sample consent withdrawn

    Args:
        sample: An ML warehouse schema Sample.

    Returns: List[AVU]
    """
    av = [
        [TrackedSample.ID, sample.sanger_sample_id],
        [TrackedSample.NAME, sample.name],
        [TrackedSample.ACCESSION_NUMBER, sample.accession_number],
        [TrackedSample.DONOR_ID, sample.donor_id],
        [TrackedSample.SUPPLIER_NAME, sample.supplier_name],
        [
            TrackedSample.CONSENT_WITHDRAWN,
            1 if sample.consent_withdrawn else None,
        ],
    ]

    return list(filter(lambda avu: avu is not None, starmap(avu_if_value, av)))


def make_study_metadata(study: Study):
    av = [
        [TrackedStudy.ID, study.id_study_lims],
        [TrackedStudy.NAME, study.name],
        [TrackedStudy.ACCESSION_NUMBER, study.accession_number],
    ]

    return list(filter(lambda avu: avu is not None, starmap(avu_if_value, av)))


def make_creation_metadata(creator: str, created: datetime):
    """Return standard iRODS metadata for data creation:

      - creator
      - created

    Args:
        creator: name of user or service creating data
        created: creation timestamp

    Returns: List[AVU]
    """
    return [
        AVU(DublinCore.CREATOR.value, creator, namespace=DublinCore.namespace),
        AVU(
            DublinCore.CREATED.value,
            created.isoformat(timespec="seconds"),
            namespace=DublinCore.namespace,
        ),
    ]


def make_modification_metadata(modified: datetime):
    return [
        AVU(
            DublinCore.MODIFIED.value,
            modified.isoformat(timespec="seconds"),
            namespace=DublinCore.namespace,
        )
    ]


def make_sample_acl(sample: Sample, study: Study) -> List[AC]:
    irods_group = f"ss_{study.id_study_lims}"
    perm = Permission.NULL if sample.consent_withdrawn else Permission.READ

    return [AC(irods_group, perm)]


def avu_if_value(attribute, value):
    if value is not None:
        return AVU(attribute, value)
