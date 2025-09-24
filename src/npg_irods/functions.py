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


def make_path_filter(
    include_patterns: list[str],
    exclude_patterns: list[str],
    flags: int | re.RegexFlag = 0,
):
    """Return a function that filters paths based on the given regex patterns.

    Args:
        include_patterns: Include paths matching the given regular expressions.
        exclude_patterns: Exclude paths matching the given regular expressions.
            Exclude applied after include.
        flags: Optional regex flags to use when compiling the patterns.

    Returns:
        A function that accepts a Path and returns True if the path doesn't
        match any of the include patterns or matches any of the exclude
        patterns, False otherwise.
    """
    include_regexes = [re.compile(p, flags=flags) for p in include_patterns]
    exclude_regexes = [re.compile(p, flags=flags) for p in exclude_patterns]

    def path_filter(path: Path) -> bool:
        if include_regexes:
            include = False
            for r in include_regexes:
                if r.search(path.as_posix()):
                    log.debug(
                        "Filtering path",
                        path=path,
                        matched=r,
                        include_regexes=include_regexes,
                    )
                    include = True
                    break
            if not include:
                return True

        for r in exclude_regexes:
            if r.search(path.as_posix()):
                log.debug(
                    "Filtering path",
                    path=path,
                    matched=r,
                    exclude_regexes=exclude_regexes,
                )
                return True

        return False

    return path_filter
