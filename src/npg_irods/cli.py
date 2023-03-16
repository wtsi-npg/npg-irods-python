# -*- coding: utf-8 -*-
#
# Copyright © 2023 Genome Research Ltd. All rights reserved.
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

import logging
from argparse import ArgumentParser

import structlog


def add_logging_arguments(parser: ArgumentParser) -> ArgumentParser:
    """Adds standard CLI logging arguments to a parser.

    - -d/--debug   Enable DEBUG level logging to STDERR.
    - -v/--verbose Enable INFO level logging to STDERR.
    - --json       Use JSON log rendering.
    - --colour     Use coloured log rendering to the console.

    Args:
        parser: An argument parser to modify.

    Returns:
        The parser
    """
    parser.add_argument(
        "-d",
        "--debug",
        help="Enable DEBUG level logging to STDERR.",
        action="store_true",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        help="Enable INFO level logging to STDERR.",
        action="store_true",
    )
    parser.add_argument(
        "--json",
        help="Use JSON log rendering.",
        action="store_true",
    )
    parser.add_argument(
        "--colour",
        help="Use coloured log rendering to the console.",
        action="store_true",
    )

    return parser


def configure_logging(debug=False, verbose=False, colour=False, json=False):
    """Configure structlog logging.

    Sets the log level, enables ANSI colour or JSON structured logging and sets the
    logging timestamp format to UTC.

        Args:
            debug: Set to True for DEBUG logging, Defaults to False.
            verbose: Set to True for INFO logging. Defaults to False and overrides debug.
            colour: Set to True for colour logging. Defaults to False.
            json: Set to True for JSON structured logs. Defaults to False and overrides
              colour.

        Returns:
            Void
    """
    level = logging.ERROR
    if debug:
        level = logging.DEBUG
    elif verbose:
        level = logging.INFO

    logging.basicConfig(level=level, encoding="utf-8")

    log_processors = [
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
    ]
    if json:
        log_processors.append(structlog.processors.JSONRenderer())
    else:
        log_processors.append(structlog.dev.ConsoleRenderer(colors=colour))
    structlog.configure(
        processors=log_processors,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
