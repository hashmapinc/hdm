# Copyright © 2020 Hashmap, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import time
from contextlib import contextmanager
from typing import Any
import urllib
import sqlalchemy
import yaml
from sqlalchemy import create_engine, exc

from hdm.core.dao.db_dao import DBDAO
from hdm.core.utils.project_config import ProjectConfig


class AzureSQLServer(DBDAO):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _get_connection(self):
        """
        Obtain a context managed azure sqlserver connection
        Returns: azure sqlserver connection
        Raises:
            ConnectionError: azure sql server connection could not be established
        """

        # Check if connection is valid, and if it isn't, then attempt create it with exponential falloff up to 3 times.
        connection_invalid = True
        connection_attempt_count = 0
        timeout = self._timeout
        connection = None

        if not self._engine:
            self._create_engine()

        while connection_attempt_count < self._max_attempts:
            connection = self._engine.connect()

            # If your connection is valid, then set it so and break from while loop
            if self._test_connection(connection):
                connection_invalid = False
                break

            # Otherwise, you must put program to sleep, wait for next time to obtain connection and carry on.
            connection_attempt_count += 1
            if connection_invalid < self._max_attempts:
                time.sleep(self._timeout)
                timeout *= self._timeout_factor

        if connection_invalid:
            raise ConnectionError('Unable to connection to MsSQL. Please try again.')

        return connection

    def _test_connection(self, connection) -> bool:
        """
        Validate that the connection is valid to MySQL instance
        Returns: True if connection is valid, False otherwise
        """
        result_proxy = None
        result = False

        try:
            result_proxy: sqlalchemy.engine.result.ResultProxy = connection.execute("SELECT 1")
            result = len(result_proxy.keys()) > 0
        except exc.StatementError as e:
            self._logger.debug("Encountered exception: %s", e)
        finally:
            if result_proxy:
                result_proxy.close()
        return result

    def _create_engine(self) -> Any:
        """
        get ODBC connection string from azure portal
        'Driver={ODBC Driver 13 for SQL Server};Server=tcp:yourDBServerName.database.windows.net,1433;
        Database=dbname;Uid=username;Pwd=xxx;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;

        and populate in values in hdm_profile.yml

        Important: {ODBC Driver 17 for SQL Server} works even though in azure portal connection string it say
        ODBC Driver 13 for SQL Server
        """
        with open(f"{ProjectConfig.hdm_home()}/{ProjectConfig.profile_path()}", 'r') as stream:
            conn_conf = yaml.safe_load(stream)[ProjectConfig.hdm_env()][self._connection_name]
        conn = f"Driver={{{conn_conf['driver']}}};Server=tcp:{conn_conf['host']}.database.windows.net,{conn_conf['port']};" \
               f"Database={conn_conf['database']};Uid={conn_conf['user']};Pwd={conn_conf['password']};Encrypt=yes;" \
               f"TrustServerCertificate=no;Connection Timeout=30;"

        params = urllib.parse.quote_plus(conn)
        conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
        return create_engine(conn_str)

    def _validate_configuration(self) -> bool:
        # TODO
        return True
