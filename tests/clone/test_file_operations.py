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

from unittest import TestCase
from unittest.mock import MagicMock
from clone.config.netezza_config import NetezzaConfig
import fileinput


class TestFileOperations(TestCase):

    def setUp(self) -> None:
        config = NetezzaConfig()
        config.file_list = MagicMock(return_value=['hdm/core/errors/__init__.py',
                                                   'hdm/core/sagas/sink/netezza_sink.py',
                                                   'setup.py'])

        self.source_repo_url = config.git_source_repo_url()
        self.local_gitlab_runner_repo = config.git_local_gitlab_runner_repo()
        self.file_list = config.file_list_to_copy()._mock_return_value
        self.text_to_search = config.file_text_to_search()
        self.replacement_text = config.file_replacement_text()

        self.expected_folder_name = ['/hdm/core/errors/',
                                     '/hdm/core/sagas/sink/',
                                     '/']
        self.expected_file_name = ['__init__.py',
                                   'netezza_sink.py',
                                   'setup.py']

    def test_folder_and_filename(self):
        index = 0
        for file_to_copy in self.file_list:
            file_name = file_to_copy.split('/')[-1]
            folder_name = "/" + file_to_copy.replace(file_name, "")

            self.assertEqual(file_name, self.expected_file_name[index])
            self.assertEqual(folder_name, self.expected_folder_name[index])
            index += 1

    def test_text_replacement(self):
        input_file = "./build_setup.py"

        # Prep the test file for the current test run
        with fileinput.FileInput(input_file, inplace=True) as file:
            for line in file:
                print(line.replace(self.replacement_text, self.text_to_search), end='')

        # Replace text in the file
        with fileinput.FileInput(input_file, inplace=True) as file:
            for line in file:
                print(line.replace(self.text_to_search, self.replacement_text), end='')

        # Assert
        with open(input_file) as f:
            self.assertTrue(self.replacement_text in f.read(), "true")
