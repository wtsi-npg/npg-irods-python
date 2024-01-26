# -*- coding: utf-8 -*-
#
# Copyright © 2024 Genome Research Ltd. All rights reserved.
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

from pytest import mark as m


@m.describe("PacBio iRODS metadata updates")
class TestPacBioMetadataUpdate:
    @m.context("When the run is from a pre-Revio instrument")
    @m.it("Can create a fixture")
    def test_wombat(self, pacbio_synthetic_mlwh):
        pass  # TODO: add tests here

    @m.context("When the run is from a Revio instrument")
    @m.it("Can create a fixture")
    def test_zombat(self, revio_synthetic_mlwh):
        pass  # TODO: add tests here


# Early PacBio runs are like this:
#
# /seq/pacbio/27857_702/A01_1:
#   m140602_154634_00127_c100650732550000001823117710071455_s1_p0.metadata.xml
#   C- /seq/pacbio/27857_702/A01_1/Analysis_Results
# /seq/pacbio/27857_702/A01_1/Analysis_Results:
#   m140602_154634_00127_c100650732550000001823117710071455_s1_p0.1.bax.h5
#   m140602_154634_00127_c100650732550000001823117710071455_s1_p0.2.bax.h5
#   m140602_154634_00127_c100650732550000001823117710071455_s1_p0.3.bax.h5
#   m140602_154634_00127_c100650732550000001823117710071455_s1_p0.bas.h5
#   m140602_154634_00127_c100650732550000001823117710071455_s1_p0.sts.xml
#
# The h5 files have run: and well: attributes in their metadata.

# Later PacBio runs are like this:
#
#  C- /seq/pacbio/r54097_20161110_145040/1_A01
# /seq/pacbio/r54097_20161110_145040/1_A01:
#   m54097_161110_145910.adapters.fasta
#   m54097_161110_145910.scraps.bam
#   m54097_161110_145910.scraps.bam.pbi
#   m54097_161110_145910.sts.xml
#   m54097_161110_145910.subreads.bam
#   m54097_161110_145910.subreads.bam.pbi
#   m54097_161110_145910.subreadset.xml
#
# The bam files have run: and well: attributes in their metadata.

# Later PacBio runs are like this:

# /seq/pacbio/r64097e_20230309_153535:
#   C- /seq/pacbio/r64097e_20230309_153535/1_A01
# /seq/pacbio/r64097e_20230309_153535/1_A01:
#   demultiplex.bc1012_BAK8A_OA--bc1012_BAK8A_OA.bam
#   demultiplex.bc1012_BAK8A_OA--bc1012_BAK8A_OA.bam.pbi
#   demultiplex.bc1012_BAK8A_OA--bc1012_BAK8A_OA.consensusreadset.xml
#   m64097e_230309_154741.consensusreadset.xml
#   m64097e_230309_154741.hifi_reads.bam
#   m64097e_230309_154741.hifi_reads.bam.pbi
#   m64097e_230309_154741.primary_qc.tar.xz
#   m64097e_230309_154741.sts.xml
#   m64097e_230309_154741.zmw_metrics.json.gz
#   merged_analysis_report.json
#
# The bam files have run: , well: , tag_index: and tag_sequence: attributes in their
# metadata.

# Later PacBio runs are like this:
#
# /seq/pacbio/r84098_20240122_143954/1_A01:
#   m84098_240122_144715_s3.fail_reads.bc2017.bam
#   m84098_240122_144715_s3.fail_reads.bc2017.bam.pbi
#   m84098_240122_144715_s3.fail_reads.bc2017.consensusreadset.xml
#   m84098_240122_144715_s3.fail_reads.consensusreadset.xml
#   m84098_240122_144715_s3.fail_reads.unassigned.bam
#   m84098_240122_144715_s3.fail_reads.unassigned.bam.pbi
#   m84098_240122_144715_s3.fail_reads.unassigned.consensusreadset.xml
#   m84098_240122_144715_s3.hifi_reads.bc2017.bam
#   m84098_240122_144715_s3.hifi_reads.bc2017.bam.pbi
#   m84098_240122_144715_s3.hifi_reads.bc2017.consensusreadset.xml
#   m84098_240122_144715_s3.hifi_reads.consensusreadset.xml
#   m84098_240122_144715_s3.hifi_reads.unassigned.bam
#   m84098_240122_144715_s3.hifi_reads.unassigned.bam.pbi
#   m84098_240122_144715_s3.hifi_reads.unassigned.consensusreadset.xml
#   m84098_240122_144715_s3.primary_qc.tar.xz
#   m84098_240122_144715_s3.sts.xml
#   m84098_240122_144715_s3.zmw_metrics.json.gz
#   merged_analysis_report.json
#
# The bam files have run: , plate_number: , well: , tag_index: and tag_sequence:
# attributes in their metadata.
