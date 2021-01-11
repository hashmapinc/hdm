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
import yaml
from sqlalchemy import create_engine, engine, exc

from hdm.core.dao.db_dao import DBDAO
from hdm.core.utils.project_config import ProjectConfig


class SQLServer(DBDAO):

    @contextmanager
    def _get_connection(self):

        connection = None
        connection_invalid = True
        connection_attempt_count = 0

        timeout = self._timeout
        timeout_factor = self._timeout_factor
        max_attempts = self._max_attempts

        if not self._engine:
            self._create_engine()

        while connection_attempt_count < max_attempts:
            connection = self._engine.connect()

            # If your connection is valid, then set it so and break from while loop
            if self._test_connection(connection):
                connection_invalid = False
                break

            # Otherwise, you must put program to sleep, wait for next time to obtain connection and carry on.
            connection_attempt_count += 1
            if connection_invalid < max_attempts:
                time.sleep(timeout)
                self._engine = self._create_engine()
                timeout *= timeout_factor

        if connection_invalid:
            raise ConnectionError('Unable to connection to MsSQL. Please try again.')

        yield connection
        connection.close()

    def _create_engine(self) -> Any:

        with open(f"{ProjectConfig.hdm_home()}/{ProjectConfig.profile_path()}", 'r') as stream:
            conn_conf = yaml.safe_load(stream)[ProjectConfig.hdm_env()][self._connection_name]

        return create_engine(
            f"mssql+pyodbc://{conn_conf['user']}:{conn_conf['password']}@{conn_conf['host']}:{conn_conf['port']}/"
            f"{conn_conf['database']}?driver=SQL+Server")

    def _test_connection(self, connection) -> bool:
        """
        Validate that the connection is valid to MySQL instance
        Returns: True if connection is valid, False otherwise
        """
        result = False

        if connection:
            result_proxy = None
            try:
                result_proxy: engine.result.ResultProxy = connection.execute("SELECT 1")
                result = len(result_proxy.keys()) > 0
            except exc.StatementError as e:
                self._logger.debug("Encountered exception: %s", e)
            finally:
                if result_proxy:
                    result_proxy.close()
        return result

    def _validate_configuration(self) -> bool:
        # TODO
        return True
