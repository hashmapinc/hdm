#  Copyright © 2020 Hashmap, Inc
#  #
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  #
#      http://www.apache.org/licenses/LICENSE-2.0
#  #
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
import sqlalchemy
import unittest
from hdm.core.dao.mysql import MySQL
from tests.hdm_test_case import HDMTestCase


@unittest.skip
class MySqlTestCase(HDMTestCase):
    def setUp(self) -> None:
        super().setUp()
        self._dao = MySQL(connection='state-manager-mysql')

    def test_mysql_connection(self):
        self.assertIsInstance(self._dao.engine, sqlalchemy.engine.base.Engine)
        with self._dao.connection as conn:
            self.assertIsInstance(conn, sqlalchemy.engine.base.Connection)
            result = conn.execute("Select 1+1")
            self.assertTrue(result.fetchone()[0], 2)

    @unittest.skip
    def test_multiple_connections(self):
        with self._dao.connection as conn:
            result = conn.execute("select 1+1")
            self.assertEqual(result.fetchone()[0], 2)

        with self._dao.connection as conn:
            result = conn.execute("select 1+1")
            self.assertEqual(result.fetchone()[0], 2)
