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
import unittest
from unittest import TestCase

import pandas as pd

from hdm.core.dao.sqlite import SqLite
from hdm.core.source.fs_source import FSSource
from hdm.core.state_management.state_manager import StateManager
from tests.core.Sagas.testenv_utils import TestEnvUtils


class TestFsSource(TestCase):
    """
      Unit Tests for Filesystem Source
      Creates a TEST_DIR with some sample data in following various file formats:
      Supported formats
      1. CSV
      """

    # TODO Verify values of Sqlite Table

    def setUp(self) -> None:
        # Set any environment variables required for running unit-tests
        TestEnvUtils.set_environment_variables()
        # Create samples files
        TestEnvUtils.create_test_env()

        # Required for Sqlite based State Manager
        self.__conn = SqLite(connection='state-manager-sqlite')
        self.assertIsInstance(self.__conn, SqLite)
        self.__sm_table_name = 'state_manager'
        TestEnvUtils.create_sm_table_sqlite(self.__conn, self.__sm_table_name)
        self.__remove_sqlite_db = True
        job_id = StateManager.generate_id()
        correlation_id_out = StateManager.generate_id()
        self.__state_manager = TestEnvUtils.get_state_manager_details(job_id, self._testMethodName, 'FSSource.__name__',
                                                                      None, None)

        # Configuration required for Source
        self.__test_directory = TestEnvUtils.TEST_DIR
        # Failure Tests
        self.__test_nulldir_conf = dict(source=dict(conf=dict(directory=None)))
        self.__test_nonexistentdir_conf = dict(source=dict(conf=dict(directory=f'{self.__test_directory}_NA')))
        # Success Tests
        self.__test_csv_conf = {'source': {'conf': {'directory': self.__test_directory, 'file_format': 'csv'}}}

    def tearDown(self) -> None:
        TestEnvUtils.delete_sm_table_sqlite(self.__conn, self.__sm_table_name, self.__remove_sqlite_db)
        TestEnvUtils.cleanup(self.__test_directory, keep_testing_directory=TestEnvUtils.KEEP_TESTS_DIR)

    def __produce_df(self, conf: dict, skipped=False):
        src = FSSource(**conf['source']['conf'])
        # self._set_correlation_id_in('123')
        for df_dict in src.consume(**conf['source']['conf']):
            df = df_dict.get('data_frame')
            file_name = df_dict.get('file_name')
            is_skipped = df_dict.get('skipped', False)
            if not skipped:
                self.assertIsInstance(df, pd.DataFrame)
                self.assertEqual(is_skipped, False)
                print(f"{file_name}:\n{df.head(2)}")
            else:
                self.assertEqual(is_skipped, True)

    def test_source_null_source_directory(self):
        self.__test_nulldir_conf['source']['conf']['state_manager'] = self.__state_manager
        with self.assertRaises(NotADirectoryError):
            self.__produce_df(self.__test_nulldir_conf)

    def test_source_nonexistent_source_directory(self):
        self.__test_nonexistentdir_conf['source']['conf']['state_manager'] = self.__state_manager
        with self.assertRaises(NotADirectoryError):
            self.__produce_df(self.__test_nonexistentdir_conf)

    @unittest.skip
    def test_source_csv(self):
        # TODO: failing in pipeline only. Need to revisit.
        self.__test_csv_conf['source']['conf']['state_manager'] = self.__state_manager
        self.__produce_df(self.__test_csv_conf)

    @unittest.skip
    def test_source_skip_processed_csv(self):
        # will not work as the logic changed to look for sink_entity to get the get_processing_history
        # instead of source_entity in state_management
        self.__test_csv_conf['source']['conf']['state_manager'] = self.__state_manager
        self.__produce_df(self.__test_csv_conf)
        # Try reprocessing and verify if skipped or not
        self.__produce_df(self.__test_csv_conf, skipped=True)
