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
from yattag import indent

from npg_irods.html_reports import ont_runs_html_report_this_year


@m.describe("HTML Meta-Reports")
class TestHTMLReports:
    @m.context("When an ONT metadata report is generated")
    @m.it("Contains the expected number of links to iRODS objects and collections")
    def test_ont_runs_html_report(self, ont_synthetic_irods):
        doc = ont_runs_html_report_this_year(zone="testZone")

        # Uncomment to write the HTML to a file for manual inspection
        #
        with open("ont_meta_report.html", "w") as f:
            f.write(indent((doc.getvalue())))

        links = [x for x in doc.result if x.startswith('<a href="/testZone/')]

        expected_colls = 40
        expected_rebasecalled_colls = 2
        expected_objs = 3
        expected_rebasecalled_objs = 2

        assert (
            len(links)
            == expected_colls
            + expected_rebasecalled_colls
            + expected_objs
            + expected_rebasecalled_objs
        )
