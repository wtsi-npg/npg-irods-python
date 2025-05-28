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
# @author Keith James <kdj@sanger.ac.uk>

from typing import Any


class DataManagementError(Exception):
    """The base class of all exceptions originating in this module."""

    pass


class ChecksumError(DataManagementError):
    """Exception raised when data checksums are in an unexpected state.

    Examples of checksum errors are; fewer or more checksums than expected, checksums
    not matching each other or checksums not matching the data.

    Args:
        args: Optional positional arguments, the first of which should be a message
        string.
        path: The path of the affected data object in iRODS.
        observed: The observed checksum(s), if any.
        expected: The expected checksum(s), if known.
    """

    def __init__(
        self, *args, path: Any = None, observed: Any = None, expected: Any = None
    ):
        super().__init__(*args)
        self.message = args[0] if len(args) > 0 else ""
        self.path = path
        self.expected = expected
        self.observed = observed


class NonUniqueError(DataManagementError):
    """Exception raised when a unique constraint is violated.

    Args:
        args: Optional positional arguments, the first of which should be a message
        string.
        observed: The observed non-unique objects, if any.
    """

    def __init__(self, *args, observed: Any = None):
        super().__init__(*args)
        self.message = args[0] if len(args) > 0 else ""
        self.observed = observed


class CollectionNotFound(DataManagementError):
    """Exception raised when an iRODS collection is expected to exist, but is not found.

    Args:
       args: Optional positional arguments, the first of which should be a message
       string.
       path: The path of the missing collection in iRODS.
    """

    def __init__(self, *args, path: Any = None):
        super().__init__(*args)
        self.message = args[0] if len(args) > 0 else ""
        self.path = path


class DataObjectNotFound(DataManagementError):
    """Exception raised when an iRODS data object is expected to exist, but is not
    found.

    Args:
       args: Optional positional arguments, the first of which should be a message
       string.
       path: The path of the missing data object in iRODS.
    """

    def __init__(self, *args, path: Any = None):
        super().__init__(*args)
        self.message = args[0] if len(args) > 0 else ""
        self.path = path


class PublishingError(DataManagementError):
    """Exception raised when an error occurs during publishing.

    Args:
        args: Optional positional arguments, the first of which should be a message
        string.
        src: The local path of the source data.
        dest: The remote path.
        num_published: The number of items published.
        num_errors: The number of errors encountered.
    """

    def __init__(
        self,
        *args,
        src: Any = None,
        dest: Any = None,
        num_processed: int = 0,
        num_errors: int = 0,
    ):
        super().__init__(*args)
        self.message = args[0] if len(args) > 0 else ""
        self.src = src
        self.dest = dest
        self.num_processed = num_processed
        self.num_errors = num_errors
