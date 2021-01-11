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

from hdm.core.utils.project_config import ProjectConfig


class TestProjectConfig(TestCase):

    def test_hdm_home(self):
        os.environ['HDM_HOME'] = os.getcwd()
        self.assertEqual(os.getcwd(), ProjectConfig.hdm_home())

    def test_hdm_env(self):
        self.assertEqual('unit-test', ProjectConfig.hdm_env())
        os.environ['HDM_ENV'] = 'prod'
        self.assertEqual('prod', ProjectConfig.hdm_env())

    def test_profile_path(self):
        self.assertEqual(ProjectConfig.profile_path(), ".hashmap_data_migrator/hdm_profiles.yml")

    def test_file_prefix(self):
        self.assertEqual('hdm', ProjectConfig.file_prefix())

    def test_state_manager_table_name(self):
        self.assertEqual('state_manager', ProjectConfig.state_manager_table_name())

    def test_archive_folder(self):
        self.assertEqual('archive', ProjectConfig.archive_folder())

    def test_connection_max_attempts(self):
        self.assertEqual(3, ProjectConfig.connection_max_attempts())

    def test_connection_timeout(self):
        self.assertEqual(3, ProjectConfig.connection_timeout())
