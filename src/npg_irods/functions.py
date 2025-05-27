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

"""This module provides free functions that are general purpose and don't fit in
other categories (such as the instrument-specific modules or metadata)."""

import re
from pathlib import Path

from structlog.stdlib import get_logger

log = get_logger(__name__)


def make_path_filter(*patterns: str, flags: int | re.RegexFlag = 0):
    """Return a function that filters paths based on the given regex patterns.

    Args:
        patterns: A list of regex patterns to match against the paths.
        flags: Optional regex flags to use when compiling the patterns.

    Returns:
        A function that accepts a Path and returns True if the path matches any of
        the patterns, False otherwise.
    """
    regexes = [re.compile(p, flags=flags) for p in patterns]

    def path_filter(path: Path) -> bool:
        for r in regexes:
            if r.match(path.as_posix()):
                log.debug("Filtering path", path=path, matched=r, regexes=regexes)
                return True
        return False

    return path_filter
