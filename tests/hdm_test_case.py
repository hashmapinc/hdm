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
import os
from unittest import TestCase
from hdm.core.dao.sqlite import SqLite
from tests.core.Sagas.testenv_utils import TestEnvUtils


class HDMTestCase(TestCase):
    local_path = os.path.dirname(__file__)
    os.environ['HDM_HOME'] = os.path.abspath(os.path.join(local_path, 'test_assets'))
    os.environ['HDM_ENV'] = 'unit-test'
    os.environ['HDM_DATA_STAGING'] = os.path.abspath(os.path.join(local_path, 'test_assets/data_staging'))
    os.environ['HDM_MANIFESTS'] = os.path.abspath(os.path.join(local_path, 'test_assets/manifests'))

    def setUp(self) -> None:
        self.set_environment_variables()
        self._path = None
        self._dao = None
        self._table_name = None

    @staticmethod
    def set_environment_variables():
        conf_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'test_assets'))
        if 'HDM_HOME' not in os.environ.keys():
            os.environ['HDM_HOME'] = conf_path
        else:
            os.environ.update({'HDM_HOME': conf_path})

        if 'HDM_ENV' not in os.environ.keys():
            os.environ['HDM_ENV'] = 'unit-test'
        else:
            os.environ.update({'HDM_ENV': 'unit-test'})

    def connect_to_db(self):
        self._dao = SqLite(connection='state-manager-sqlite')
        self._table_name = 'state_manager_test'
        os.environ['TEST_DB_NAME'] = 'hdm'

    @staticmethod
    def create_test_staging_folder():
        if not os.path.exists(os.getenv('HDM_DATA_STAGING')):
            os.makedirs(os.getenv('HDM_DATA_STAGING'))

    def tearDown(self) -> None:
        TestEnvUtils.cleanup(os.getenv('HDM_DATA_STAGING'), keep_testing_directory=TestEnvUtils.KEEP_TESTS_DIR)

        if self._path and os.getenv('TEST_DB_NAME'):
            db_path = os.path.join(self._path, f"{os.getenv('TEST_DB_NAME')}.db")
            if os.path.exists(db_path):
                try:
                    os.unlink(db_path)
                except Exception:
                    raise ResourceWarning
