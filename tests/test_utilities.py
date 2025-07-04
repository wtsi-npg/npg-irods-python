# -*- coding: utf-8 -*-
#
# Copyright © 2022, 2023, 2025 Genome Research Ltd. All rights reserved.
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

import re
import subprocess
from io import StringIO
from pathlib import Path, PurePath

import pytest
from partisan.exception import RodsError
from partisan.irods import AC, AVU, Collection, DataObject, Permission
from pytest import mark as m

from helpers import set_replicate_invalid
from npg_irods.metadata.common import ensure_common_metadata, has_trimmable_replicas
from npg_irods.metadata.lims import (
    TrackedSample,
    TrackedStudy,
    ensure_consent_withdrawn,
)
from npg_irods.utilities import (
    check_checksums,
    check_consent_withdrawn,
    check_replicas,
    copy,
    load_resource,
    repair_checksums,
    repair_replicas,
    update_secondary_metadata,
    withdraw_consent,
    write_safe_remove_commands,
    write_safe_remove_script,
)


def collect_objs(coll: Collection):
    return [
        item for item in coll.contents(recurse=True) if item.rods_type == DataObject
    ]


def collect_obj_paths(coll: Collection):
    return [str(item) for item in collect_objs(coll)]


@m.describe("Resource utilities")
class TestResourceUtilities:
    @m.context("When a resource is loaded")
    @m.it("Returns the resource as a string")
    def test_load_resource(self):
        stylesheet = load_resource("style.css")
        assert stylesheet.lstrip().startswith("html {")
        assert stylesheet.rstrip().endswith("}")


@m.describe("Checksum utilities")
class TestChecksumUtilities:
    @m.context(
        "When data object checksums are checked and "
        "all of the data objects have checksum metadata"
    )
    @m.it("Counts successes correctly")
    def test_checked_checksums_passes(self, annotated_collection_tree):
        obj_paths = collect_obj_paths(Collection(annotated_collection_tree))

        for p in obj_paths:
            ensure_common_metadata(DataObject(p))

        with StringIO("\n".join(obj_paths)) as reader:
            with StringIO() as writer:
                num_processed, num_passed, num_errors = check_checksums(
                    reader, writer, print_pass=True
                )
                assert num_processed == len(obj_paths)
                assert num_passed == len(obj_paths)
                assert num_errors == 0

                passed_paths = writer.getvalue().split()
                assert passed_paths == obj_paths

    @m.context(
        "When data object checksums are checked and "
        "none of the data objects have checksum metadata"
    )
    @m.it("Counts failures correctly")
    def test_check_checksums_failures(self, annotated_collection_tree):
        obj_paths = collect_obj_paths(Collection(annotated_collection_tree))

        with StringIO("\n".join(obj_paths)) as reader:
            with StringIO() as writer:
                num_processed, num_passed, num_errors = check_checksums(
                    reader, writer, print_fail=True
                )
                assert num_processed == len(obj_paths)
                assert num_passed == 0
                assert num_errors == len(obj_paths)

                failed_paths = writer.getvalue().split()
                assert failed_paths == obj_paths

    @m.context(
        "When data object checksums are repaired and "
        "all of the data objects have checksum metadata"
    )
    @m.it("Counts repairs correctly")
    def test_repair_checksums_all(self, annotated_collection_tree):
        obj_paths = collect_obj_paths(Collection(annotated_collection_tree))
        for p in obj_paths:
            ensure_common_metadata(DataObject(p))

        with StringIO("\n".join(obj_paths)) as reader:
            with StringIO() as writer:
                num_processed, num_repaired, num_errors = repair_checksums(
                    reader, writer, print_repair=True
                )
                assert num_processed == len(obj_paths)
                assert num_repaired == 0
                assert num_errors == 0

                repaired_paths = writer.getvalue().split()
                assert repaired_paths == []

    @m.context(
        "When data object checksums are repaired and "
        "none of the data objects have checksum metadata"
    )
    @m.it("Counts repairs correctly")
    def test_repair_checksums_none(self, annotated_collection_tree):
        obj_paths = collect_obj_paths(Collection(annotated_collection_tree))

        with StringIO("\n".join(obj_paths)) as reader:
            with StringIO() as writer:
                num_processed, num_repaired, num_errors = repair_checksums(
                    reader, writer, print_repair=True
                )
                assert num_processed == len(obj_paths)
                assert num_repaired == len(obj_paths)
                assert num_errors == 0

                repaired_paths = writer.getvalue().split()
                assert repaired_paths == obj_paths


@m.describe("Replica utilities")
class TestReplicaUtilities:
    @m.context(
        "When detecting trimmable valid replicas and "
        "a data object no trimmable valid replicas"
    )
    @m.it("Returns false")
    def test_has_trimmable_replicas_valid(self, simple_data_object):
        obj = DataObject(simple_data_object)
        assert not has_trimmable_replicas(obj, num_replicas=2)

    @m.context(
        "When detecting trimmable valid replica and "
        "a data object has trimmable valid replicas"
    )
    @m.it("Returns true")
    def test_has_trimmable_replicas_valid_none(self, simple_data_object):
        obj = DataObject(simple_data_object)
        assert has_trimmable_replicas(obj, num_replicas=1)

    @m.context(
        "When detecting trimmable invalid replicas and "
        "a data object has trimmable invalid replicas"
    )
    @m.it("Returns true")
    def test_has_trimmable_replicas_invalid(self, invalid_replica_data_object):
        obj = DataObject(invalid_replica_data_object)
        assert has_trimmable_replicas(obj, num_replicas=2)

    @m.context(
        "When data object replicas are checked and "
        "all of the data objects have conforming replicas"
    )
    @m.it("Counts successes correctly")
    def test_checked_replicas_none(self, annotated_collection_tree):
        obj_paths = collect_obj_paths(Collection(annotated_collection_tree))
        expected_num_replicas = 2

        with StringIO("\n".join(obj_paths)) as reader:
            with StringIO() as writer:
                num_processed, num_passed, num_errors = check_replicas(
                    reader, writer, num_replicas=expected_num_replicas, print_pass=True
                )
                assert num_processed == len(obj_paths)
                assert num_passed == len(obj_paths)
                assert num_errors == 0

                passed_paths = writer.getvalue().split()
                assert passed_paths == obj_paths

    @m.context(
        "When data object replicas are checked and "
        "none of the data objects have conforming replicas"
    )
    @m.it("Counts failures correctly")
    def test_check_replicas_all(self, annotated_collection_tree):
        obj_paths = collect_obj_paths(Collection(annotated_collection_tree))
        expected_num_replicas = 999  # Cause failure by expecting an impossible number

        with StringIO("\n".join(obj_paths)) as reader:
            with StringIO() as writer:
                num_processed, num_passed, num_errors = check_replicas(
                    reader, writer, num_replicas=expected_num_replicas, print_fail=True
                )
                assert num_processed == len(obj_paths)
                assert num_passed == 0
                assert num_errors == len(obj_paths)

                failed_paths = writer.getvalue().split()
                assert failed_paths == obj_paths

    @m.context(
        "When data object replicas are repaired and "
        "all of the data objects have conforming replicas"
    )
    @m.it("Counts repairs correctly")
    def test_repair_replicas_none(self, annotated_collection_tree):
        obj_paths = collect_obj_paths(Collection(annotated_collection_tree))
        desired_num_replicas = 2

        with StringIO("\n".join(obj_paths)) as reader:
            with StringIO() as writer:
                num_processed, num_repaired, num_errors = repair_replicas(
                    reader, writer, num_replicas=desired_num_replicas, print_repair=True
                )
                assert num_processed == len(obj_paths)
                assert num_repaired == 0
                assert num_errors == 0

                repaired_paths = writer.getvalue().split()
                assert repaired_paths == []

    @m.context(
        "When data object replicas are repaired and "
        "all of the data objects need invalid replicas repaired"
    )
    @m.it("Counts repairs correctly")
    def test_repair_invalid_replicas_all(self, annotated_collection_tree):
        obj_paths = collect_obj_paths(Collection(annotated_collection_tree))
        for p in obj_paths:
            set_replicate_invalid(DataObject(p), replicate_num=1)

        desired_num_replicas = 2  # The test fixture has 2 replicas
        with StringIO("\n".join(obj_paths)) as reader:
            with StringIO() as writer:
                num_processed, num_repaired, num_errors = repair_replicas(
                    reader, writer, num_replicas=desired_num_replicas, print_repair=True
                )
                assert num_processed == len(obj_paths)
                assert num_repaired == len(obj_paths)
                assert num_errors == 0

                repaired_paths = writer.getvalue().split()
                assert repaired_paths == obj_paths

    @m.context(
        "When data object replicas are repaired and "
        "all of the data objects need valid replicas repaired"
    )
    @m.it("Counts repairs correctly")
    def test_repair_valid_replicas_all(self, annotated_collection_tree):
        obj_paths = collect_obj_paths(Collection(annotated_collection_tree))

        desired_num_replicas = 1  # The test fixture has 2 replicas
        with StringIO("\n".join(obj_paths)) as reader:
            with StringIO() as writer:
                num_processed, num_repaired, num_errors = repair_replicas(
                    reader, writer, num_replicas=desired_num_replicas, print_repair=True
                )
                assert num_processed == len(obj_paths)
                assert num_repaired == len(obj_paths)
                assert num_errors == 0

                repaired_paths = writer.getvalue().split()
                assert repaired_paths == obj_paths


@m.describe("Consent utilities")
class TestConsentUtilities:
    @m.context("When a data object's consent is withdrawn")
    @m.it("Has permissions removed, except for the current user and rodsadmins")
    def test_ensure_consent_withdrawn(self, annotated_collection_tree):
        obj_paths = collect_obj_paths(Collection(annotated_collection_tree))
        study_ac = AC("ss_1000", Permission.READ, zone="testZone")
        admin_ac = AC("irods", Permission.OWN, zone="testZone")
        public_ac = AC("public", Permission.READ, zone="testZone")

        for p in obj_paths:
            obj = DataObject(p)
            assert study_ac in obj.permissions()
            assert admin_ac in obj.permissions()
            assert public_ac in obj.permissions()

            ensure_consent_withdrawn(obj)

            assert study_ac not in obj.permissions()
            assert public_ac not in obj.permissions()
            assert admin_ac in obj.permissions()

    @m.context(
        "When data object consent withdrawn state is checked and "
        "all of the data objects have consent withdrawn"
    )
    @m.it("Counts successes correctly")
    def test_checked_consent_withdrawn_passes(self, annotated_collection_tree):
        obj_paths = collect_obj_paths(Collection(annotated_collection_tree))

        for p in obj_paths:
            ensure_consent_withdrawn(DataObject(p))

        with StringIO("\n".join(obj_paths)) as reader:
            with StringIO() as writer:
                num_processed, num_passed, num_errors = check_consent_withdrawn(
                    reader, writer, print_pass=True
                )
                assert num_processed == len(obj_paths)
                assert num_passed == len(obj_paths)
                assert num_errors == 0

                passed_paths = writer.getvalue().split()
                assert passed_paths == obj_paths

    @m.context(
        "When data object consent withdrawn state is checked and "
        "none of the data objects have consent withdrawn"
    )
    @m.it("Counts failures correctly")
    def test_checked_consent_withdrawn_failures(self, annotated_collection_tree):
        obj_paths = collect_obj_paths(Collection(annotated_collection_tree))

        with StringIO("\n".join(obj_paths)) as reader:
            with StringIO() as writer:
                num_processed, num_passed, num_errors = check_consent_withdrawn(
                    reader, writer, print_fail=True
                )
                assert num_processed == len(obj_paths)
                assert num_passed == 0
                assert num_errors == len(obj_paths)

                failed_paths = writer.getvalue().split()
                assert failed_paths == obj_paths

    @m.context(
        "When data objects have their consent withdraw and "
        "all of the data objects need to have their consent withdrawn"
    )
    @m.it("Counts repairs correctly")
    def test_withdraw_consent_all(self, annotated_collection_tree):
        obj_paths = collect_obj_paths(Collection(annotated_collection_tree))

        with StringIO("\n".join(obj_paths)) as reader:
            with StringIO() as writer:
                num_processed, num_withdrawn, num_errors = withdraw_consent(
                    reader, writer, print_withdrawn=True
                )
                assert num_processed == len(obj_paths)
                assert num_withdrawn == len(obj_paths)
                assert num_errors == 0

                withdrawn_paths = writer.getvalue().split()
                assert withdrawn_paths == obj_paths

    @m.context(
        "When data objects have their consent withdrawn and "
        "none of the data objects need to have their consent withdrawn"
    )
    @m.it("Counts repairs correctly")
    def test_withdraw_consent_none(self, annotated_collection_tree):
        obj_paths = collect_obj_paths(Collection(annotated_collection_tree))

        # Make sure the object is already marked as consent withdrawn
        for p in obj_paths:
            ensure_consent_withdrawn(DataObject(p))

        with StringIO("\n".join(obj_paths)) as reader:
            with StringIO() as writer:
                num_processed, num_withdrawn, num_errors = withdraw_consent(
                    reader, writer, print_withdrawn=True
                )
                assert num_processed == len(obj_paths)
                assert num_withdrawn == 0
                assert num_errors == 0

                withdrawn_paths = writer.getvalue().split()
                assert withdrawn_paths == []


@m.describe("Copy utilities")
class TestCopyUtilities:
    @m.context(
        "When any path is copied and " "the source and destination paths are the same"
    )
    @m.it("Raises an error")
    def test_copy_to_self(self, empty_collection, simple_data_object):
        c = Collection(empty_collection)
        with pytest.raises(ValueError):
            copy(c, c)

        d = DataObject(simple_data_object)
        with pytest.raises(ValueError):
            copy(d, d)

    @m.context(
        "When a collection is copied and "
        "there is no collection with that name at the destination"
    )
    @m.it("Creates a copy within the destination collection")
    def test_copy_collection(self, empty_collection):
        x = Collection(PurePath(empty_collection, "x"))
        x.create()
        assert x.exists()
        y = Collection(PurePath(empty_collection, "y"))
        y.create()
        assert y.exists()

        copy(x, y)
        assert Collection(PurePath(empty_collection, "y", "x")).exists()

    @m.context(
        "When a collection is copied recursively and "
        "there is no collection with that name at the destination and "
        "the destination's parent collection exists"
    )
    @m.it("Creates a renamed copy within the destination's parent collection")
    def test_copy_rename_collection(self, empty_collection):
        x = Collection(PurePath(empty_collection, "x"))
        z = Collection(PurePath(empty_collection, "x", "y", "z"))
        z.create(parents=True)
        assert z.exists()

        a = Collection(PurePath(empty_collection, "a"))
        assert not a.exists()
        assert Collection(a.path.parent).exists()

        copy(x, a, recurse=True)
        assert Collection(PurePath(empty_collection, "a", "y", "z")).exists()

    @m.context(
        "When a collection is copied and "
        "there is already a collection with that name at the destination"
    )
    @m.it("Raises an exception, unless exists_ok is True")
    def test_copy_collection_error(self, empty_collection):
        x = Collection(PurePath(empty_collection, "x"))
        x.create()
        y = Collection(PurePath(empty_collection, "y"))
        y.create()
        copy(x, y)

        with pytest.raises(RodsError, match="CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME"):
            copy(x, y)

        num_processed, num_copied = copy(x, y, exist_ok=True)
        assert num_processed == 1
        assert num_copied == 0

    @m.context(
        "When a data object is copie and "
        "the destination path is an existing collection and "
        "there is no data object with that name at the destination"
    )
    @m.it("Creates a copy within the destination collection")
    def test_copy_data_object(self, empty_collection, simple_data_object):
        c = Collection(empty_collection)
        d = DataObject(simple_data_object)
        num_processed, num_copied = copy(d, c)

        assert DataObject(PurePath(c.path, simple_data_object.name)).exists()
        assert num_processed == 1
        assert num_copied == 1

    @m.context(
        "When a data object is copied and "
        "there is no data object with that name at the destination"
    )
    @m.it("Creates a copy at the destination")
    def test_copy_data_object_no_dest(self, empty_collection, simple_data_object):
        c = Collection(empty_collection)
        d = DataObject(simple_data_object)
        dest = PurePath(c.path, "test.txt")
        num_processed, num_copied = copy(d, dest)

        assert DataObject(dest).exists()
        assert num_processed == 1
        assert num_copied == 1

    @m.context(
        "When a data object is copie and "
        "the destination path is an existing collection and "
        "there is already a data object with that name at the destination"
    )
    @m.it("Raises an exception, unless exists_ok is True")
    def test_copy_data_object_error(self, empty_collection, simple_data_object):
        x = Collection(PurePath(empty_collection, "x"))
        x.create()
        d = DataObject(simple_data_object)
        copy(d, x)

        with pytest.raises(RodsError, match="OVERWRITE_WITHOUT_FORCE_FLAG"):
            copy(d, x)

        num_processed, num_copied = copy(d, x, exist_ok=True)
        assert num_processed == 1
        assert num_copied == 0

    @m.context("When a tree is copied")
    @m.it("Copies all collections and objects")
    def test_copy_recurse(self, annotated_collection_tree, empty_collection):
        src = Collection(annotated_collection_tree)
        dest = Collection(empty_collection)

        num_processed, num_copied = copy(src, dest, recurse=True)

        expected = [
            PurePath(dest.path, "tree", p).as_posix()
            for p in [
                "./",
                "./a",
                "./a/m",
                "./a/n",
                "./a/o",
                "./b",
                "./b/p",
                "./b/q",
                "./b/r",
                "./c",
                "./c/s",
                "./c/t",
                "./c/u",
                "./x.txt",
                "./y.txt",
                "./z.txt",
                "./a/h.txt",
                "./a/i.txt",
                "./a/j.txt",
                "./a/m/w.txt",
                "./a/n/x.txt",
                "./a/o/y.txt",
                "./b/p/w.txt",
                "./b/q/x.txt",
                "./b/r/z.txt",
            ]
        ]

        observed = [str(x) for x in dest.contents(recurse=True)]
        assert observed == expected
        assert num_processed == len(expected)
        assert num_copied == len(expected)

    @m.context("When a tree with annotation is copied")
    @m.it("Copies annotation on collections and data objects")
    def test_copy_annotation_recurse(self, annotated_collection_tree, empty_collection):
        src = Collection(annotated_collection_tree)
        dest = Collection(empty_collection)

        copy(src, dest, avu=True, recurse=True)

        for item in dest.contents(avu=True, recurse=True):
            # The path of the source item must appear in the annotation of the dest
            # item for the test to pass
            src_path = re.sub(
                r"simple_collection", "annotated_collection_tree", str(item)
            )
            assert item.metadata() == [AVU("path", src_path)]

    @m.context("When a tree with permissions is copied")
    @m.it("Copies permissions on collections and data objects")
    def test_copy_permissions_recurse(
        self, annotated_collection_tree, empty_collection
    ):
        src = Collection(annotated_collection_tree)
        dest = Collection(empty_collection)

        copy(src, dest, acl=True, recurse=True)

        for item in dest.contents(acl=True, recurse=True):
            assert AC("ss_1000", Permission.READ, zone="testZone") in item.permissions()


@m.describe("Safe remove utilities")
class TestSafeRemoveUtilities:
    @m.context("When passed a hierarchy of collections and data objects")
    @m.it("Writes the expected commands")
    def test_write_safe_remove_commands(self, annotated_collection_tree):
        with StringIO() as writer:
            write_safe_remove_commands(annotated_collection_tree, writer)

            expected = [
                ("irm", "w.txt"),
                ("irm", "x.txt"),
                ("irm", "y.txt"),
                ("irm", "h.txt"),
                ("irm", "i.txt"),
                ("irm", "j.txt"),
                ("irm", "w.txt"),
                ("irm", "x.txt"),
                ("irm", "z.txt"),
                ("irm", "x.txt"),
                ("irm", "y.txt"),
                ("irm", "z.txt"),
                ("irmdir", "u"),
                ("irmdir", "t"),
                ("irmdir", "s"),
                ("irmdir", "c"),  # c contains t, u & s
                ("irmdir", "r"),
                ("irmdir", "q"),
                ("irmdir", "p"),
                ("irmdir", "b"),  # b contains p, q & r
                ("irmdir", "o"),
                ("irmdir", "n"),
                ("irmdir", "m"),
                ("irmdir", "a"),  # a contains m, n & o
                ("irmdir", "tree"),  # The root
            ]
            observed = []
            for line in writer.getvalue().splitlines():
                cmd, path = line.split(maxsplit=1)
                path = path.lstrip("'")
                path = path.rstrip("'")

                observed.append((cmd, PurePath(path).name))
            assert observed == expected

    @m.context("When a generated safe remove script is run")
    @m.it("Removes the expected collections and data objects")
    def test_write_safe_remove_script(self, tmp_path, annotated_collection_tree):
        script = Path(tmp_path, "safe_rm.sh")
        with open(script, "w", encoding="UTF-8") as writer:
            write_safe_remove_script(writer, annotated_collection_tree)
        subprocess.run([script.as_posix()], check=True)

        assert not Collection(annotated_collection_tree).exists()

    @m.context(
        "When passed a hierarchy of collections and data objects and "
        "paths contain spaces and/or quotes"
    )
    @m.it("Writes the expected commands")
    def test_write_safe_remove_commands_special(self, challenging_paths_irods):
        with StringIO() as writer:
            write_safe_remove_commands(challenging_paths_irods, writer)

            expected = [
                ("irm", ("challenging", r"a a", r"w'\''.txt")),
                ("irm", ("challenging", r"a a", "x.txt")),
                ("irm", ("challenging", r"a a", r"y y.txt")),
                ("irm", ("challenging", r"a a", r'z".txt')),
                ("irm", ("challenging", r'b"b', "x.txt")),
                ("irm", ("challenging", r'b"b', r"y y.txt")),
                ("irm", ("challenging", r'b"b', r'z".txt')),
                ("irm", ("challenging", r"w'\''.txt")),
                ("irm", ("challenging", "x.txt")),
                ("irm", ("challenging", "y y.txt")),
                ("irm", ("challenging", r'z".txt')),
                ("irmdir", ("challenging", r'b"b')),
                ("irmdir", ("challenging", "a a")),
                ("irmdir", ("challenging",)),
            ]
            observed = []
            for line in writer.getvalue().splitlines():
                cmd, path = line.split(maxsplit=1)
                path = path.lstrip("'")
                path = path.rstrip("'")

                parts = PurePath(path).parts
                i = parts.index("challenging")
                observed.append((cmd, parts[i:]))
            assert observed == expected

    @m.context(
        "When a generated safe remove script is run and "
        "paths contain spaces and/or quotes"
    )
    @m.it("Removes the expected collections and data objects")
    def test_write_safe_remove_script_special(self, tmp_path, challenging_paths_irods):
        script = Path(tmp_path, "safe_rm.sh")
        with open(script, "w", encoding="UTF-8") as writer:
            write_safe_remove_script(writer, challenging_paths_irods)
        subprocess.run([script.as_posix()], check=True)

        assert not Collection(challenging_paths_irods).exists()


@m.describe("Metadata utilities")
class TestMetadataUtilities:
    @m.context(
        "When an iRODS object has both study and sample ID metadata and "
        "it wants both study and sample metadata enhanced"
    )
    @m.it("Counts successes correctly")
    def test_update_secondary_metadata(
        self, single_study_and_single_sample_data_object, study_and_samples_mlwh
    ):
        obj_path = single_study_and_single_sample_data_object.as_posix()

        with StringIO("\n".join([obj_path])) as reader:
            with StringIO() as writer:
                engine = study_and_samples_mlwh.get_bind()
                num_processed, num_updated, num_errors = update_secondary_metadata(
                    reader, writer, engine, print_update=True
                )
                assert num_processed == 1
                assert num_updated == 1
                assert num_errors == 0

                passed_paths = writer.getvalue().split()
                assert passed_paths == [obj_path]

        expected_avus = [
            AVU(TrackedStudy.ID, "1000"),
            AVU(TrackedStudy.NAME, "Study X"),
            AVU(TrackedStudy.TITLE, "Test Study Title"),
            AVU(TrackedStudy.ACCESSION_NUMBER, "Test Accession"),
            AVU(TrackedSample.ID, "id_sample_lims1"),
            AVU(TrackedSample.LIMS, "LIMS_01"),
            AVU(TrackedSample.ACCESSION_NUMBER, "Test Accession"),
            AVU(TrackedSample.COMMON_NAME, "common_name1"),
            AVU(TrackedSample.DONOR_ID, "donor_id1"),
            AVU(TrackedSample.NAME, "name1"),
            AVU(TrackedSample.PUBLIC_NAME, "public_name1"),
            AVU(TrackedSample.SUPPLIER_NAME, "supplier_name1"),
            AVU(TrackedSample.UUID, "82429892-0ab6-11ee-b5ba-fa163eac3ag7"),
        ]

        obj = DataObject(single_study_and_single_sample_data_object)
        for avu in expected_avus:
            assert avu in obj.metadata()

    @m.context(
        "When an iRODS object has study ID, but not sample ID metadata "
        "and it wants study metadata enhanced"
    )
    @m.it("Counts successes correctly")
    def test_update_secondary_metadata_with_just_study(
        self, single_study_and_single_sample_data_object, study_and_samples_mlwh
    ):
        obj_path = single_study_and_single_sample_data_object.as_posix()
        obj = DataObject(single_study_and_single_sample_data_object)

        assert obj.remove_metadata(AVU(TrackedSample.ID, "id_sample_lims1")) == 1

        with StringIO("\n".join([obj_path])) as reader:
            with StringIO() as writer:
                engine = study_and_samples_mlwh.get_bind()
                num_processed, num_updated, num_errors = update_secondary_metadata(
                    reader, writer, engine, print_update=True
                )
                assert num_processed == 1
                assert num_updated == 1
                assert num_errors == 0

                passed_paths = writer.getvalue().split()
                assert passed_paths == [obj_path]

            expected_avus = [
                AVU(TrackedStudy.ID, "1000"),
                AVU(TrackedStudy.NAME, "Study X"),
                AVU(TrackedStudy.TITLE, "Test Study Title"),
                AVU(TrackedStudy.ACCESSION_NUMBER, "Test Accession"),
            ]

        for avu in expected_avus:
            assert avu in obj.metadata()

    @m.context(
        "When an iRODS object has sample ID, but not study ID metadata and "
        "it wants sample metadata enhanced"
    )
    @m.it("Counts successes correctly")
    def test_update_general_metadata_with_just_study_err(
        self, single_study_and_single_sample_data_object, study_and_samples_mlwh
    ):
        obj_path = single_study_and_single_sample_data_object.as_posix()
        obj = DataObject(single_study_and_single_sample_data_object)

        assert obj.remove_metadata(AVU(TrackedStudy.ID, "1000")) == 1

        with StringIO("\n".join([obj_path])) as reader:
            with StringIO() as writer:
                engine = study_and_samples_mlwh.get_bind()
                num_processed, num_updated, num_errors = update_secondary_metadata(
                    reader, writer, engine, print_update=True
                )
                assert num_processed == 1
                assert num_updated == 1
                assert num_errors == 0

                passed_paths = writer.getvalue().split()
                assert passed_paths == [obj_path]

        expected_avus = [
            AVU(TrackedSample.ACCESSION_NUMBER, "Test Accession"),
            AVU(TrackedSample.COMMON_NAME, "common_name1"),
            AVU(TrackedSample.DONOR_ID, "donor_id1"),
            AVU(TrackedSample.NAME, "name1"),
            AVU(TrackedSample.PUBLIC_NAME, "public_name1"),
            AVU(TrackedSample.SUPPLIER_NAME, "supplier_name1"),
        ]

        for avu in expected_avus:
            assert avu in obj.metadata()

        absent_avus = [
            AVU(TrackedStudy.ID, "1000"),
            AVU(TrackedStudy.NAME, "Study X"),
            AVU(TrackedStudy.TITLE, "Test Study Title"),
            AVU(TrackedStudy.ACCESSION_NUMBER, "Test Accession"),
        ]

        for avu in absent_avus:
            assert avu not in obj.metadata()
