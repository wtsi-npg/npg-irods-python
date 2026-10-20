# -*- coding: utf-8 -*-
#
# Copyright © 2026 Genome Research Ltd. All rights reserved.
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
import shutil
from pathlib import Path
from typing import Any, Generator

import pytest


@pytest.fixture(scope="function")
def xenium_staging_dir(tmp_path: Path) -> Generator[Path, Any, None]:
    staging_dir = tmp_path / "staging"
    shutil.copytree("./tests/data/xenium/staging", staging_dir)

    yield staging_dir


@pytest.fixture(scope="function")
def xenium_staging_dir_with_error(
    xenium_staging_dir: Path,
) -> Generator[Path, Any, None]:
    (xenium_staging_dir / "error").mkdir()
    (xenium_staging_dir / "error" / "experiment.xenium").touch()
    original_mode = (xenium_staging_dir / "error").stat().st_mode
    (xenium_staging_dir / "error").chmod(0)

    yield xenium_staging_dir

    (xenium_staging_dir / "error").chmod(original_mode)


@pytest.fixture(scope="function")
def xenium_staging_dir_non_instrument_output(
    tmp_path: Path,
) -> Generator[Path, Any, None]:
    non_instrument_output_dir = tmp_path / "non_instrument_output"
    shutil.copytree(
        "tests/data/xenium/synthetic/"
        "output-XETG00000__0000000__synthetic_region_001__20000101__000000",
        non_instrument_output_dir,
    )

    (non_instrument_output_dir / ".DS_Store").touch()
    (non_instrument_output_dir / "Thumbs.db").touch()
    (non_instrument_output_dir / "sub").mkdir()
    (non_instrument_output_dir / "sub" / ".DS_Store").touch()
    (non_instrument_output_dir / "sub" / ".Thumbs.db").touch()

    yield non_instrument_output_dir
