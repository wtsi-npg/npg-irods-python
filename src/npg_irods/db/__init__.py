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

import configparser
import os
from urllib import parse


class DBConfig:
    """A database configuration which falls back on environment variables if values are
    not set in the constructor.

    The database URL attribute is currently constructed in MySQL-specific format.
    """

    HOST = "host"
    PORT = "port"
    SCHEMA = "schema"
    USER = "user"
    PASSWORD = "password"

    @classmethod
    def from_file(cls, ini_file, section):
        parser = configparser.ConfigParser()
        parser.read(ini_file)

        return cls(
            parser.get(section, cls.HOST),
            parser.get(section, cls.PORT),
            parser.get(section, cls.SCHEMA),
            parser.get(section, cls.USER),
            parser.get(section, cls.PASSWORD),
        )

    def __init__(self, host=None, port=None, schema=None, user=None, password=None):
        """Creates a new configuration.

        Args:
            host: The host name. Defaults to the value of the
              DB_HOST environment variable.
            port: The host port. Defaults to the value of the
              DB_PORT environment variable.
            schema: The database schema name.A host name. Defaults to the value of the
              DB_SCHEMA environment variable.
            user: The database user. Defaults to the value of the
              DB_USER environment variable.
            password: The user's password. Defaults to the value of the
              DB_PASSWORD environment variable.
        """
        self.host = host if host else os.environ.get("DB_HOST")
        self.port = port if port else os.environ.get("DB_PORT")
        self.schema = schema if schema else os.environ.get("DB_SCHEMA")
        self.user = user if user else os.environ.get("DB_USER")
        self.password = password if password else os.environ.get("DB_PASSWORD")

        for attr, var in {
            DBConfig.HOST: "DB_HOST",
            DBConfig.PORT: "DB_PORT",
            DBConfig.SCHEMA: "DB_SCHEMA",
            DBConfig.USER: "DB_USER",
            DBConfig.PASSWORD: "DB_PASSWORD",
        }.items():
            if getattr(self, attr) is None:
                raise ValueError(
                    f"Database {attr} not set by configuration or "
                    f"{var} environment variable"
                )

        self.url = (
            f"mysql+pymysql://{self.user}:{parse.quote_plus(self.password)}"
            f"@{self.host}:{self.port}/{self.schema}?charset=utf8mb4"
        )
