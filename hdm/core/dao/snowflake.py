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
import traceback
import yaml
import snowflake.connector as connector
from contextlib import contextmanager

from hdm.core.dao.db_dao import DBDAO
from hdm.core.utils.project_config import ProjectConfig


class Snowflake(DBDAO):

    def _create_engine(self):
        super()._create_engine()

    def _get_connection(self):
        """
        Obtain a context managed snowflake connection

        Returns: Snowflake connection

        Raises:
            ConnectionError: Snowflake connection could not be established

        """
        # Check if connection is valid, and if it isn't, then attempt create it with exponential fall of up to 3 times.
        connection_invalid = True
        connection_attempt_count = 0
        timeout = self._timeout
        connection = None

        with open(f"{ProjectConfig.hdm_home()}/{ProjectConfig.profile_path()}", 'r') as stream:
            conn_conf = yaml.safe_load(stream)[ProjectConfig.hdm_env()][self._connection_name]

        while connection_attempt_count < self._max_attempts:
            try:
                connection = connector.connect(
                    user=conn_conf['user'],
                    password=conn_conf['password'],
                    account=conn_conf['account'],
                    authenticator=conn_conf['authenticator'],
                    warehouse=conn_conf['warehouse'],
                    database=conn_conf['database'],
                    schema=conn_conf['schema'],
                    role=conn_conf['role'],
                    login_timeout=1
                )
                # If your connection is valid, then set it so and break from while loop
                if self._test_connection(connection):
                    connection_invalid = False
                    break
                # Otherwise, you must put program to sleep, wait for next time to obtain connection and carry on.
                connection_attempt_count = self.__sleep_and_increment_counter(timeout,
                                                                              connection_attempt_count,
                                                                              connection_invalid)

            except Exception:
                connection_attempt_count, connection_invalid = self.__manage_exception(timeout,
                                                                                       connection_attempt_count,
                                                                                       connection_invalid)

        if connection_invalid:
            raise ConnectionError('Unable to connection to Snowflake. Please try again.')

        return connection

    def _test_connection(self, connection) -> bool:
        """
        Validate that the connection is valid to Snowflake instance

        Returns: True if connection is valid, False otherwise

        """
        if not connection:
            return False

        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            if not len(cursor.fetchone()) > 0:
                return False

            return True

    def __manage_exception(self, timeout, connection_attempt_count, connection_invalid):
        if connection_attempt_count < self._max_attempts:
            error_message = f'While attempting to connect to Snowflake the follow error was encountered: ' \
                            f'{traceback.format_exc()}'
            self._logger.error(error_message)
            connection_attempt_count = self.__sleep_and_increment_counter(timeout, connection_attempt_count, connection_invalid)
        else:
            connection_invalid = True
        return connection_attempt_count, connection_invalid

    def __sleep_and_increment_counter(self, timeout, connection_attempt_count, connection_invalid):
        connection_attempt_count += 1
        if connection_invalid < self._max_attempts:
            time.sleep(self._timeout)
            timeout *= self._timeout_factor
        return timeout, connection_attempt_count

    def _validate_configuration(self) -> bool:
        # TODO
        return True
