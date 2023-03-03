# -*- coding: utf-8 -*-
#
# Copyright © 2022, 2023 Genome Research Ltd. All rights reserved.
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

from setuptools import find_packages, setup

setup(
    name="npg-irods-python",
    url="https://github.com/wtsi-npg/npg-irods-python",
    license="GPL3",
    author="Keith James",
    author_email="kdj@sanger.ac.uk",
    description=".",
    use_scm_version={"version_scheme": "no-guess-dev"},
    python_requires=">=3.10",
    packages=find_packages("src"),
    package_dir={"": "src"},
    setup_requires=["setuptools_scm"],
    install_requires=["ml-warehouse", "partisan", "rich", "sqlalchemy", "structlog"],
    tests_require=["black", "pytest", "pytest-it"],
    scripts=[
        "scripts/backfill_illumina_locations.py",
        "scripts/check-checksums",
        "scripts/check-common-metadata",
        "scripts/check-replicas",
        "scripts/copy-confirm",
        "scripts/repair-checksums",
        "scripts/repair-common-metadata",
        "scripts/repair-replicas",
        "scripts/safe-remove-script",
        "scripts/update-ont-metadata",
    ],
)
