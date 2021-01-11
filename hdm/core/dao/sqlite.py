#  Copyright © 2020 Hashmap, Inc
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
import os
from typing import Any
import yaml
from sqlalchemy import create_engine, exc
from contextlib import contextmanager

from hdm.core.dao.db_dao import DBDAO
from hdm.core.utils.project_config import ProjectConfig


class SqLite(DBDAO):

    @contextmanager
    def _get_connection(self):
        """
        Obtain a context managed sqllite connection

        Returns: sqllite connection

        Raises:
            ConnectionError: sqllite connection could not be established

        """

        if not self._engine:
            self._engine = self._create_engine()

        connection = self._engine.connect()
        if not self._test_connection(connection):
            raise ConnectionError('Unable to connection to SQLite. Please try again.')

        yield connection
        connection.close()

    def _create_engine(self) -> Any:
        with open(f"{ProjectConfig.hdm_home()}/{ProjectConfig.profile_path()}", 'r') as stream:
            conn_conf = yaml.safe_load(stream)[ProjectConfig.hdm_env()][self._connection_name]

        if not conn_conf.get('dbpath', os.curdir):
            db_path = os.path.abspath(os.curdir)
        else:
            db_path = os.path.abspath(conn_conf.get('dbpath', os.curdir))

        database = str(conn_conf['database'])

        if database.rfind(".db") == -1:
            db_path = os.path.join(db_path, f"{database}.db")

        return create_engine(f"sqlite:///{db_path}")

    def _test_connection(self, connection) -> bool:
        result = False

        # if self._connection:
        if connection:
            result_proxy = None
            try:
                # result_proxy = self._connection.execute("SELECT 1")
                result_proxy = connection.execute("SELECT 1")
                result = bool(result_proxy.fetchone()[0])
            except exc.StatementError as e:
                self._logger.debug("Encountered exception: %s", e)
            finally:
                if result_proxy:
                    result_proxy.close()

        return result

    def _validate_configuration(self) -> bool:
        # TODO
        return True
