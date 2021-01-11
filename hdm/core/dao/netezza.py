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
import yaml
from sqlalchemy import exc

from hdm.core.dao.db_dao import DBDAO
from hdm.core.utils.project_config import ProjectConfig


class Netezza(DBDAO):

    def _get_connection_config(self, config: dict) -> dict:
        raise NotImplementedError("Base Class Method")

    def _connect_by_connector(self, config: dict) -> None:
        raise NotImplementedError("Base Class Method")

    def _create_engine(self):
        super()._create_engine()

    def _validate_configuration(self):
        super()._validate_configuration()

    @contextmanager
    def _get_connection(self):
        """
        Obtain a context managed netezza connection

        Returns: Netezza connection

        Raises:
            ConnectionError: Netezza connection could not be established

        """
        connection = None

        with open(f"{ProjectConfig.hdm_home()}/{ProjectConfig.profile_path()}", 'r') as stream:
            conn_conf = yaml.safe_load(stream)[ProjectConfig.hdm_env()][self._connection_name]

        connection_config = self._get_connection_config(config=conn_conf)

        connection_invalid = True
        connection_attempt_count = 0

        timeout = self._timeout

        while connection_attempt_count < self._max_attempts:
            connection = self._connect_by_connector(connection_config)

            # If your connection is valid, then set it so and break from while loop
            if self._test_connection(connection):
                connection_invalid = False
                break
            # Otherwise, you must put program to sleep, wait for next time to obtain connection and carry on.

            connection_attempt_count += 1
            if connection_invalid < self._max_attempts:
                time.sleep(timeout)
                timeout *= self._timeout_factor

        if connection_invalid:
            raise ConnectionError('Unable to connection to Netezza. Please try again.')

        yield connection
        connection.close()

    def _test_connection(self, connection) -> bool:
        """
        Validate that the connection is valid to Netezza instance

        Returns: True if connection is valid, False otherwise

        """
        result = False
        cursor = None

        if connection:
            try:
                cursor = connection.cursor()
                cursor.execute("SELECT 1")
                result = len(cursor.fetchone()) > 0
            # TODO: Why are we testing sqlalchmey errors here???
            except exc.StatementError as e:
                self._logger.debug("Encountered exception: %s", e)
            finally:
                if cursor:
                    cursor.close()

        return result
