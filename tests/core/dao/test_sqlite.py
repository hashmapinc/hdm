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
import os
import pandas as pd
import sqlalchemy

from hdm.core.dao.sqlite import SqLite
from tests.hdm_test_case import HDMTestCase


class SqlLiteTest(HDMTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.connect_to_db()
        self._path = os.path.dirname(os.path.realpath(__file__))

    def test_engine_is_correct_type(self):
        self.assertIsInstance(self._dao.engine, sqlalchemy.engine.base.Engine)

    def test_connection_is_correct_type(self):
        self.assertIsInstance(self._dao, SqLite)

    def test_create_table(self):
        with self._dao.connection as conn:
            self.assertIsInstance(conn, sqlalchemy.engine.base.Connection)
            conn.execute(f"CREATE TABLE {self._table_name} (\n"
                         "  state_id TEXT DEFAULT NULL,\n"
                         "  job_id TEXT DEFAULT NULL,\n"
                         "  correlation_id TEXT DEFAULT NULL,\n"
                         "  action TEXT DEFAULT NULL,\n"
                         "  status TEXT DEFAULT NULL,\n"
                         "  source_name TEXT DEFAULT NULL,\n"
                         "  source_type TEXT DEFAULT NULL,\n"
                         "  sink_name TEXT DEFAULT NULL,\n"
                         "  sink_type TEXT DEFAULT NULL,\n"
                         "  entity TEXT DEFAULT NULL,\n"
                         "  filter TEXT DEFAULT NULL,\n"
                         "  git_sha TEXT DEFAULT NULL,\n"
                         "  event_time TEXT NOT NULL,\n"
                         "  PRIMARY KEY(state_id)\n"
                         ");")
            conn.execute(f"select * from {self._table_name}")

    def test_query(self):
        with self._dao.connection as conn:
            result = conn.execute("select 1+1")
            self.assertEqual(result.fetchone()[0], 2)

    def test_repeated_query(self):
        with self._dao.connection as conn:
            result = conn.execute("select 1+1")
            self.assertEqual(result.fetchone()[0], 2)

            result = conn.execute("select 1+1")
            self.assertEqual(result.fetchone()[0], 2)

    def test_multiple_connections(self):
        with self._dao.connection as conn:
            result = conn.execute("select 1+1")
            self.assertEqual(result.fetchone()[0], 2)

        with self._dao.connection as conn:
            result = conn.execute("select 1+1")
            self.assertEqual(result.fetchone()[0], 2)

    def test_drop_table(self):
        with self._dao.connection as conn:
            conn.execute(f"DROP TABLE if exists {self._table_name}")
            print(conn.closed)

    def test_execute_with_engine(self):
        engine = self._dao.engine
        expected = {'1+1': {0: 2}, '1+2': {0: 3}}
        df = pd.read_sql(sql='select 1+1, 1+2', con=engine)
        self.assertDictEqual(df.to_dict(), expected)
