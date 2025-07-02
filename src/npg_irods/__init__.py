# -*- coding: utf-8 -*-
#
# Copyright © 2022, 2024, 2025 Genome Research Ltd. All rights reserved.
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


import importlib.metadata
import sys

import structlog

__version__ = importlib.metadata.version("npg-irods-python")


# If this proves generally useful, it could be moved to npg-python-lib
def add_appinfo_structlog_processor():
    """Add a custom structlog processor reporting executable information to the
    configuration."""

    def _add_executable_info(_logger, _method_name, event: dict):
        """Add the executable name and version to all log entries."""
        event["application"] = "npg-irods-python"
        event["executable"] = sys.argv[0]
        event["version"] = version()
        return event

    c = structlog.get_config()
    c["processors"] = [_add_executable_info] + c["processors"]
    structlog.configure(**c)


def version() -> str:
    """Return the current version."""
    return __version__
