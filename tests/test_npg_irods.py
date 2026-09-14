import argparse

import pytest

from npg_irods import parse_timedelta_from_hours


def test_parse_timedelta_from_hours():
    with pytest.raises(argparse.ArgumentTypeError):
        parse_timedelta_from_hours("a")
