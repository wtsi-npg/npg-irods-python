# -*- coding: utf-8 -*-
#
# Copyright © 2025 Genome Research Ltd. All rights reserved.
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


from partisan.irods import AC, AVU, Collection, DataObject, Permission
from pytest import mark as m

from npg_irods.metadata.lims import (
    TrackedSample,
    has_consent_withdrawn_metadata,
    has_consent_withdrawn_permissions,
    has_mixed_ownership,
    is_managed_access,
)


class TestLIMS:
    @m.context("When an AC group matches a SequenceScape study token")
    @m.it("Is managed access")
    def test_is_managed_access_ss(self):
        zone = "testZone"
        assert is_managed_access(AC("ss_1000", Permission.READ, zone=zone))

        assert not is_managed_access(AC("1000_ss", Permission.READ, zone=zone))
        assert not is_managed_access(AC("unmanaged", Permission.READ, zone=zone))

    @m.context(
        "When an AC group matches a SequenceScape study's human reads filter token"
    )
    @m.it("Is managed access")
    def test_is_managed_access_ss_human(self):
        zone = "testZone"
        assert is_managed_access(AC("ss_1000_human", Permission.READ, zone=zone))

        assert not is_managed_access(AC("1000_human", Permission.READ, zone=zone))
        assert not is_managed_access(AC("ss_human_1000", Permission.READ, zone=zone))
        assert not is_managed_access(AC("unmanaged_human", Permission.READ, zone=zone))

    @m.context("When an ACL contains managed access controls for multiple groups")
    @m.it("Has mixed ownership")
    def test_has_mixed_ownership(self):
        zone = "testZone"
        assert has_mixed_ownership(
            [
                AC("ss_1000", Permission.READ, zone=zone),
                AC("ss_2000", Permission.READ, zone=zone),
            ]
        )
        assert has_mixed_ownership(
            [
                AC("ss_1000", Permission.READ, zone=zone),
                AC("ss_1000_human", Permission.READ, zone=zone),
            ]
        )

        assert not has_mixed_ownership([AC("ss_1000", Permission.READ, zone=zone)])
        assert not has_mixed_ownership(
            [
                AC("ss_1000", Permission.READ, zone=zone),
                AC("ss_1000", Permission.WRITE, zone="otherZone"),
            ]
        )
        assert not has_mixed_ownership(
            [
                AC("ss_1000", Permission.READ, zone=zone),
                AC("unmanaged", Permission.WRITE, zone=zone),
            ]
        )

    @m.context("When a data object ACL does not contain any managed access controls")
    @m.it("Has consent withdrawn permissions")
    def test_has_consent_withdrawn_permissions_obj(self, simple_data_object):
        zone = "testZone"
        managed_1000 = AC("ss_1000", Permission.READ, zone=zone)
        managed_2000 = AC("ss_2000", Permission.WRITE, zone=zone)
        obj = DataObject(simple_data_object)

        assert obj.add_permissions(AC("unmanaged", Permission.READ, zone=zone)) == 1
        assert has_consent_withdrawn_permissions(obj)

        assert obj.add_permissions(managed_1000) == 1
        assert not has_consent_withdrawn_permissions(obj)

        assert obj.add_permissions(managed_2000) == 1
        assert not has_consent_withdrawn_permissions(obj)

        assert obj.remove_permissions(managed_1000, managed_2000) == 2
        assert has_consent_withdrawn_permissions(obj)

    @m.context(
        "When a collection has an ACL that does not contain any managed access controls"
    )
    @m.it("Has consent withdrawn permissions")
    def test_has_consent_withdrawn_permissions_coll(self, populated_collection):
        zone = "testZone"
        managed_1000 = AC("ss_1000", Permission.READ, zone=zone)
        unmanaged = AC("unmanaged", Permission.READ, zone=zone)
        coll = Collection(populated_collection)

        assert has_consent_withdrawn_permissions(coll, recurse=False)

        assert coll.add_permissions(unmanaged) == 1
        assert has_consent_withdrawn_permissions(coll, recurse=True)

        assert coll.add_permissions(managed_1000) == 1
        assert not has_consent_withdrawn_permissions(coll, recurse=False)

    @m.context(
        "When a collection and its contents recursively, have ACLs "
        "that do not contain any managed access controls"
    )
    @m.it("Has consent withdrawn permissions")
    def test_has_consent_withdrawn_permissions_coll_recur(self, populated_collection):
        zone = "testZone"
        managed_1000 = AC("ss_1000", Permission.READ, zone=zone)
        unmanaged = AC("unmanaged", Permission.READ, zone=zone)
        coll = Collection(populated_collection)

        assert coll.add_permissions(unmanaged) == 1
        for item in coll.iter_contents(recurse=True):
            assert item.add_permissions(unmanaged) == 1

        assert has_consent_withdrawn_permissions(coll, recurse=True)
        assert (
            DataObject(
                populated_collection / "collection" / "sub" / "b.txt"
            ).add_permissions(managed_1000)
            == 1
        )
        assert not has_consent_withdrawn_permissions(coll, recurse=True)

    @m.context("When a a data object has an AVU with an NPG consent withdrawn value")
    @m.it("Has consent withdrawn metadata")
    def test_has_consent_withdrawn_metadata_npg_obj(self, simple_data_object):
        npg_withdrawn = AVU(TrackedSample.CONSENT_WITHDRAWN, 1)
        obj = DataObject(simple_data_object)

        assert not has_consent_withdrawn_metadata(obj)
        obj.add_metadata(npg_withdrawn)
        assert has_consent_withdrawn_metadata(obj)

    @m.context("When a data object has an AVU with a GAPI consent withdrawn value")
    @m.it("Has consent withdrawn metadata")
    def test_has_consent_withdrawn_metadata_gapi_obj(self, simple_data_object):
        gapi_withdrawn = AVU(TrackedSample.CONSENT, 0)
        obj = DataObject(simple_data_object)

        assert not has_consent_withdrawn_metadata(obj)
        obj.add_metadata(gapi_withdrawn)
        assert has_consent_withdrawn_metadata(obj)

        gapi_consented = AVU(TrackedSample.CONSENT, 1)
        obj.supersede_metadata(gapi_consented)
        assert not has_consent_withdrawn_metadata(obj)

    @m.context("When a collection has an AVU with an NPG consent withdrawn value")
    @m.it("Has consent withdrawn metadata")
    def test_has_consent_withdrawn_metadata_npg_coll(self, populated_collection):
        npg_withdrawn = AVU(TrackedSample.CONSENT_WITHDRAWN, 1)
        coll = Collection(populated_collection)

        assert not has_consent_withdrawn_metadata(coll, recurse=False)
        assert coll.add_metadata(npg_withdrawn) == 1
        assert has_consent_withdrawn_metadata(coll, recurse=False)

    @m.context(
        "When a collection and its contents recursively, have an AVU "
        "with an NPG consent withdrawn value"
    )
    @m.it("Has consent withdrawn metadata")
    def test_has_consent_withdrawn_metadata_npg_coll_recur(self, populated_collection):
        npg_withdrawn = AVU(TrackedSample.CONSENT_WITHDRAWN, 1)
        coll = Collection(populated_collection)

        assert not has_consent_withdrawn_metadata(coll, recurse=True)
        assert coll.add_metadata(npg_withdrawn) == 1
        assert not has_consent_withdrawn_metadata(coll, recurse=True)

        for item in coll.iter_contents(recurse=True):
            assert item.add_metadata(npg_withdrawn) == 1
        assert has_consent_withdrawn_metadata(coll, recurse=True)

        assert (
            DataObject(
                populated_collection / "collection" / "sub" / "b.txt"
            ).remove_metadata(npg_withdrawn)
            == 1
        )
        assert not has_consent_withdrawn_metadata(coll, recurse=True)

    @m.context("When a collection has an AVU with a GAPI consent withdrawn value")
    @m.it("Has consent withdrawn metadata")
    def test_has_consent_withdrawn_metadata_gapi_coll(self, populated_collection):
        gapi_withdrawn = AVU(TrackedSample.CONSENT, 0)
        coll = Collection(populated_collection)

        assert not has_consent_withdrawn_metadata(coll, recurse=False)
        assert coll.add_metadata(gapi_withdrawn) == 1
        assert has_consent_withdrawn_metadata(coll, recurse=False)

        gapi_consented = AVU(TrackedSample.CONSENT, 1)
        coll.supersede_metadata(gapi_consented)
        assert not has_consent_withdrawn_metadata(coll)

    @m.context(
        "When a collection and its contents recursively, have an AVU "
        "with a GAPI consent withdrawn value"
    )
    @m.it("Has consent withdrawn metadata")
    def test_has_consent_withdrawn_metadata_gapi_coll_recur(self, populated_collection):
        gapi_withdrawn = AVU(TrackedSample.CONSENT, 0)
        coll = Collection(populated_collection)

        assert not has_consent_withdrawn_metadata(coll, recurse=True)
        assert coll.add_metadata(gapi_withdrawn) == 1
        assert not has_consent_withdrawn_metadata(coll, recurse=True)

        for item in coll.iter_contents(recurse=True):
            assert item.add_metadata(gapi_withdrawn) == 1
        assert has_consent_withdrawn_metadata(coll, recurse=True)

        gapi_consented = AVU(TrackedSample.CONSENT, 1)
        assert DataObject(
            populated_collection / "collection" / "sub" / "b.txt"
        ).supersede_metadata(gapi_consented) == (1, 1)
        assert not has_consent_withdrawn_metadata(coll, recurse=True)
