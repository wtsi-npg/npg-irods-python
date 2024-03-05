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

"""Business logic API and schema-level API for the ML warehouse."""

import enum
from typing import Type

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    select,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship


class Platform(enum.Enum):
    """Sequencing platform values for SeqProductIrodsLocations.seq_platform_name"""

    Illumina = 1
    ONT = 2
    PacBio = 3


class Base(DeclarativeBase):
    pass


class Sample(Base):
    __tablename__ = "sample"

    id_sample_tmp = mapped_column(Integer, primary_key=True, autoincrement=True)
    id_lims = mapped_column(String(10), nullable=False)
    id_sample_lims = mapped_column(String(20), nullable=False)
    last_updated = mapped_column(DateTime, nullable=False)
    recorded_at = mapped_column(DateTime, nullable=False)
    consent_withdrawn = mapped_column(Integer, nullable=False, default=0)
    name = mapped_column(String(255), index=True)
    organism = mapped_column(String(255))
    accession_number = mapped_column(String(50), index=True)
    common_name = mapped_column(String(255))
    cohort = mapped_column(String(255))
    sanger_sample_id = mapped_column(String(255), index=True)
    supplier_name = mapped_column(String(255), index=True)
    public_name = mapped_column(String(255))
    donor_id = mapped_column(String(255))
    date_of_consent_withdrawn = mapped_column(DateTime)
    marked_as_consent_withdrawn_by = mapped_column(String(255))

    iseq_flowcell: Mapped["IseqFlowcell"] = relationship(
        "IseqFlowcell", back_populates="sample"
    )

    oseq_flowcell: Mapped["OseqFlowcell"] = relationship(
        "OseqFlowcell", back_populates="sample"
    )

    pac_bio_run: Mapped["PacBioRun"] = relationship(
        "PacBioRun", back_populates="sample"
    )

    def __repr__(self):
        return (
            f"<Sample pk={self.id_sample_tmp} id_sample_lims={self.id_sample_lims} "
            f"name='{self.name}'>"
        )


class Study(Base):
    __tablename__ = "study"

    id_study_tmp = mapped_column(Integer, primary_key=True, autoincrement=True)
    id_lims = mapped_column(String(10), nullable=False)
    id_study_lims = mapped_column(String(20), nullable=False)
    last_updated = mapped_column(DateTime, nullable=False)
    recorded_at = mapped_column(DateTime, nullable=False)
    name = mapped_column(String(255), index=True)
    accession_number = mapped_column(String(50), index=True)
    description = mapped_column(Text)
    contains_human_dna = mapped_column(Integer, default=0)
    contaminated_human_dna = mapped_column(Integer, default=0)
    remove_x_and_autosomes = mapped_column(Integer, default=0)
    separate_y_chromosome_data = mapped_column(Integer, default=0)
    ena_project_id = mapped_column(String(255))
    study_title = mapped_column(String(255))
    study_visibility = mapped_column(String(255))
    ega_dac_accession_number = mapped_column(String(255))
    data_access_group = mapped_column(String(255))

    iseq_flowcell: Mapped["IseqFlowcell"] = relationship(
        "IseqFlowcell", back_populates="study"
    )

    oseq_flowcell: Mapped["OseqFlowcell"] = relationship(
        "OseqFlowcell", back_populates="study"
    )

    pac_bio_run: Mapped["PacBioRun"] = relationship("PacBioRun", back_populates="study")

    def __repr__(self):
        return f"<Study pk={self.id_study_tmp} id_study_lims={self.id_study_lims}>"


class IseqFlowcell(Base):
    __tablename__ = "iseq_flowcell"

    id_iseq_flowcell_tmp = mapped_column(Integer, primary_key=True, autoincrement=True)
    last_updated = mapped_column(DateTime, nullable=False)
    recorded_at = mapped_column(DateTime, nullable=False)
    id_sample_tmp = mapped_column(
        ForeignKey("sample.id_sample_tmp"), nullable=False, index=True
    )
    id_lims = mapped_column(String(10), nullable=False)
    id_flowcell_lims = mapped_column(String(20), nullable=False)
    position = mapped_column(Integer, nullable=False)
    entity_type = mapped_column(String(30), nullable=False)
    entity_id_lims = mapped_column(String(20), nullable=False)
    id_pool_lims = mapped_column(String(20), nullable=False)
    id_study_tmp = mapped_column(ForeignKey("study.id_study_tmp"), index=True)
    manual_qc = mapped_column(Integer)
    tag_index = mapped_column(Integer)
    pipeline_id_lims = mapped_column(String(60))
    id_library_lims = mapped_column(String(255), index=True)
    primer_panel = mapped_column(String(255))

    sample: Mapped["Sample"] = relationship("Sample", back_populates="iseq_flowcell")
    study: Mapped["Study"] = relationship("Study", back_populates="iseq_flowcell")
    iseq_product_metrics: Mapped["IseqProductMetrics"] = relationship(
        "IseqProductMetrics", back_populates="iseq_flowcell"
    )

    def __repr__(self):
        return f"<IseqFlowcell pk={self.id_iseq_flowcell_tmp}>"


class IseqProductMetrics(Base):
    __tablename__ = "iseq_product_metrics"

    id_iseq_pr_metrics_tmp = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    id_iseq_product = mapped_column(String(64), nullable=False, unique=True)
    last_changed = mapped_column(DateTime)
    id_iseq_flowcell_tmp = mapped_column(
        ForeignKey("iseq_flowcell.id_iseq_flowcell_tmp", ondelete="SET NULL"),
        index=True,
        comment='Flowcell id, see "iseq_flowcell.id_iseq_flowcell_tmp"',
    )
    id_run = mapped_column(Integer)
    position = mapped_column(Integer)
    tag_index = mapped_column(Integer)
    qc = mapped_column(Integer)

    iseq_flowcell: Mapped["IseqFlowcell"] = relationship(
        "IseqFlowcell", back_populates="iseq_product_metrics"
    )

    def __repr__(self):
        return (
            f"<IseqProductMetrics pk={self.id_iseq_pr_metrics_tmp} "
            f"id_run={self.id_run} position={self.position} "
            f"tag_index={self.tag_index} flowcell={self.iseq_flowcell}>"
        )


class OseqFlowcell(Base):
    __tablename__ = "oseq_flowcell"

    id_oseq_flowcell_tmp = mapped_column(Integer, primary_key=True, autoincrement=True)
    id_flowcell_lims = mapped_column(String(255), nullable=False)
    last_updated = mapped_column(DateTime, nullable=False)
    recorded_at = mapped_column(DateTime, nullable=False)
    id_sample_tmp = mapped_column(
        ForeignKey("sample.id_sample_tmp"), nullable=False, index=True
    )
    id_study_tmp = mapped_column(
        ForeignKey("study.id_study_tmp"), nullable=False, index=True
    )
    experiment_name = mapped_column(String(255), nullable=False)
    instrument_name = mapped_column(String(255), nullable=False)
    instrument_slot = mapped_column(Integer, nullable=False)
    id_lims = mapped_column(String(10), nullable=False)
    pipeline_id_lims = mapped_column(String(255))
    requested_data_type = mapped_column(String(255))
    tag_identifier = mapped_column(String(255))
    tag_sequence = mapped_column(String(255))
    tag_set_id_lims = mapped_column(String(255))
    tag_set_name = mapped_column(String(255))
    tag2_identifier = mapped_column(String(255))
    tag2_sequence = mapped_column(String(255))
    tag2_set_id_lims = mapped_column(String(255))
    tag2_set_name = mapped_column(String(255))
    flowcell_id = mapped_column(String(255))
    run_id = mapped_column(String(255))

    sample: Mapped["Sample"] = relationship("Sample", back_populates="oseq_flowcell")
    study: Mapped["Study"] = relationship("Study", back_populates="oseq_flowcell")

    def __repr__(self):
        return (
            f"<OseqFlowcell expt_name={self.experiment_name} "
            f"slot={self.instrument_slot} "
            f"flowcell={self.flowcell_id}>"
        )


class PacBioRun(Base):
    __tablename__ = "pac_bio_run"

    id_pac_bio_tmp = mapped_column(Integer, primary_key=True, autoincrement=True)
    last_updated = mapped_column(DateTime, nullable=False)
    recorded_at = mapped_column(DateTime, nullable=False)
    id_sample_tmp = mapped_column(
        Integer, ForeignKey("sample.id_sample_tmp"), nullable=False
    )
    id_study_tmp = mapped_column(
        Integer, ForeignKey("study.id_study_tmp"), nullable=False
    )
    pac_bio_run_name = mapped_column(String(255))
    well_label = mapped_column(String(255), nullable=False)
    plate_number = mapped_column(Integer)
    id_lims = mapped_column(String(10), nullable=False)
    id_pac_bio_run_lims = mapped_column(String(20), nullable=False)
    tag_identifier = mapped_column(String(30))
    tag_sequence = mapped_column(String(30))
    tag_set_id_lims = mapped_column(String(20))
    tag_set_name = mapped_column(String(100))
    tag2_sequence = mapped_column(String(30))
    tag2_set_id_lims = mapped_column(String(20))
    tag2_set_name = mapped_column(String(100))
    tag2_identifier = mapped_column(String(30))

    sample: Mapped["Sample"] = relationship("Sample", back_populates="pac_bio_run")
    study: Mapped["Study"] = relationship("Study", back_populates="pac_bio_run")
    pac_bio_product_metrics: Mapped["PacBioProductMetrics"] = relationship(
        "PacBioProductMetrics", back_populates="pac_bio_run"
    )

    def __repr__(self):
        return (
            f"<PacBioRun run_name={self.pac_bio_run_name} "
            f"well_label={self.well_label} "
            f"plate_number={self.plate_number}>"
        )


class PacBioRunWellMetrics(Base):
    __tablename__ = "pac_bio_run_well_metrics"
    __table_args__ = (
        Index(
            "pac_bio_metrics_run_well",
            "pac_bio_run_name",
            "well_label",
            "plate_number",
            unique=True,
        ),
    )

    id_pac_bio_rw_metrics_tmp = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )

    last_changed = mapped_column(DateTime)
    id_pac_bio_product = mapped_column(String(64), nullable=False)
    pac_bio_run_name = mapped_column(String(255), nullable=False)
    well_label = mapped_column(String(255), nullable=False)
    plate_number = mapped_column(Integer)

    pac_bio_product_metrics: Mapped["PacBioProductMetrics"] = relationship(
        "PacBioProductMetrics", back_populates="pac_bio_run_well_metrics"
    )

    def __repr__(self):
        return (
            f"<PacBioRunWellMetrics run_name={self.pac_bio_run_name} "
            f"well_label={self.well_label} "
            f"plate_number={self.plate_number} "
            f"product={self.id_pac_bio_product}>"
        )


class PacBioProductMetrics(Base):
    __tablename__ = "pac_bio_product_metrics"
    __table_args__ = (
        Index(
            "pac_bio_metrics_product",
            "id_pac_bio_tmp",
            "id_pac_bio_rw_metrics_tmp",
            unique=True,
        ),
    )

    last_changed = mapped_column(DateTime)
    id_pac_bio_pr_metrics_tmp = mapped_column(Integer, primary_key=True)
    id_pac_bio_rw_metrics_tmp = mapped_column(
        ForeignKey("pac_bio_run_well_metrics.id_pac_bio_rw_metrics_tmp"),
        nullable=False,
        index=True,
    )
    id_pac_bio_tmp = mapped_column(
        ForeignKey("pac_bio_run.id_pac_bio_tmp"), nullable=False, index=True
    )
    id_pac_bio_product = mapped_column(String(64), nullable=False, unique=True)
    qc = mapped_column(Boolean, nullable=False)

    pac_bio_run_well_metrics: Mapped["PacBioRunWellMetrics"] = relationship(
        "PacBioRunWellMetrics", back_populates="pac_bio_product_metrics"
    )
    pac_bio_run: Mapped["PacBioRun"] = relationship(
        "PacBioRun", back_populates="pac_bio_product_metrics"
    )


class SeqProductIrodsLocations(Base):
    __tablename__ = "seq_product_irods_locations"

    id_seq_product_irods_locations_tmp = mapped_column(
        BigInteger, primary_key=True, autoincrement=True
    )
    created = mapped_column(DateTime)
    last_changed = mapped_column(DateTime)
    id_product = mapped_column(String(64), nullable=False)
    seq_platform_name = mapped_column(Enum(Platform), nullable=False)
    pipeline_name = mapped_column(String(32), nullable=False)
    irods_root_collection = mapped_column(String(255), nullable=False)
    irods_data_relative_path = mapped_column(String(255))
    irods_secondary_data_relative_path = mapped_column(String(255))


def find_consent_withdrawn_samples(sess: Session) -> list[Type[Sample]]:
    """Return a list of all samples with consent withdrawn.

    Args:
        sess: An open session to the ML warehouse.

    Returns:
        All samples marked as having their consent withdrawn.
    """
    return sess.query(Sample).filter(Sample.consent_withdrawn == 1).all()


def find_study_by_study_id(sess: Session, id: String) -> Study:
    """Return a study from a study_id.

    Args:
        sess: An open SQL session.
        study_id: A Study ID in MLWH

    Returns:
        sample: An ML warehouse schema Study.
    """
    query = sess.execute(select(Study).where(Study.id_study_lims == id)).scalar_one()

    return query


def find_sample_by_sample_id(sess: Session, id: String) -> Sample:
    """Return a sample from a sample_id.

    Args:
        sess: An open SQL session.
        sample_id: A Sample ID in MLWH

    Returns:
        sample: An ML warehouse schema Sample.
    """
    query = sess.execute(select(Sample).where(Sample.id_sample_lims == id)).scalar_one()

    return query
