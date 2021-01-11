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
import fnmatch
import os
import re
import unittest
from unittest import TestCase

from hdm.core.dao.sqlite import SqLite
from hdm.core.sink.fs_sink import FSSink
from hdm.core.state_management.state_manager import StateManager
from tests.core.Sagas.testenv_utils import TestEnvUtils


class TestFsSink(TestCase):
    """
    Unit Tests for Filesystem Sink
    Takes a sample dataframe and creates output files in TEST_DIR
    """

    # TODO Verify values of Sqlite Table
    FS_LANDING_DIR = 'fslanding'

    def setUp(self) -> None:
        # Set any environment variables required for running unit-tests
        TestEnvUtils.set_environment_variables()
        # Create samples dataframe to be loaded to file in landing location defined by __test_directory
        self.__df = TestEnvUtils.get_test_df()
        self.__test_directory = os.path.abspath(os.path.join(os.path.curdir, self.FS_LANDING_DIR))
        if not os.path.exists(self.__test_directory):
            os.mkdir(self.__test_directory)
        self.assertTrue(os.path.exists(self.__test_directory))

        # Required for Sqlite based State Manager
        self.__conn = SqLite(connection='state-manager-sqlite')
        self.assertIsInstance(self.__conn, SqLite)
        # TODO: Move to CI pipeline variables?
        self.__sm_table_name = 'state_manager'
        TestEnvUtils.create_sm_table_sqlite(self.__conn, self.__sm_table_name)
        self.__remove_sqlite_db = True
        job_id = StateManager.generate_id()
        self.__state_manager = TestEnvUtils.get_state_manager_details(job_id, None, None,
                                                                      self._testMethodName, FSSink.__name__)

    def tearDown(self) -> None:
        TestEnvUtils.delete_sm_table_sqlite(self.__conn, self.__sm_table_name,
                                            self.__remove_sqlite_db)
        TestEnvUtils.cleanup(self.__test_directory, keep_testing_directory=TestEnvUtils.KEEP_TESTS_DIR)

    def __consume_df(self, conf: dict):
        sink = FSSink(**conf['sink']['conf'])
        sink.produce(**conf['sink']['conf'])

    def test_sink_no_landing_directory(self):
        conf = dict(sink=dict(conf=dict(directory=f'{self.__test_directory}_NA')))
        conf['sink']['conf']['state_manager'] = self.__state_manager
        with self.assertRaises(NotADirectoryError):
            self.__consume_df(conf)

    @unittest.skip
    def test_sink_unsupported_file_format(self):
        # TODO: revisit later
        conf = dict(sink=dict(conf=dict(directory=self.__test_directory, data_frame=self.__df, file_format='parquet')))
        conf['sink']['conf']['state_manager'] = self.__state_manager
        with self.assertRaises(ValueError):
            self.__consume_df(conf)

    def test_sink_no_data_frame(self):
        conf = dict(sink=dict(conf=dict(directory=self.__test_directory, file_format='parquet')))
        with self.assertRaises(KeyError):
            self.__consume_df(conf)

    def test_sink_csv_default_filename(self):
        conf = dict(sink=dict(conf=dict(directory=self.__test_directory, data_frame=self.__df)))
        print(self.__state_manager)
        conf['sink']['conf']['state_manager'] = self.__state_manager
        self.__consume_df(conf)
        self.assertGreaterEqual(len(
            [vals[0] for vals in [(x, fnmatch.fnmatch(x, 'hdm*')) for x in os.listdir(self.__test_directory)] if
             re.match(r"hdm_[0-9a-fA-F]{32}.csv", vals[0])]), 1)

    def test_sink_csv_with_filename(self):
        conf = dict(
            sink=dict(
                conf=dict(directory=self.__test_directory, data_frame=self.__df, file_name='test_file_no_chunk.csv')))
        conf['sink']['conf']['state_manager'] = self.__state_manager
        self.__consume_df(conf)
        self.assertEqual(len(fnmatch.filter(os.listdir(self.__test_directory), conf['sink']['conf']['file_name'])), 1)
