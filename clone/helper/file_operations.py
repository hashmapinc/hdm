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
import os.path
import shutil
import fileinput
from clone.config.netezza_config import NetezzaConfig


class FileOperations:

    def __init__(self, config: NetezzaConfig):
        self.source_repo_url = config.git_source_repo_url()
        self.local_gitlab_runner_repo = config.git_local_gitlab_runner_repo()
        self.file_list = config.file_list_to_copy()
        self.text_to_search = config.file_text_to_search()
        self.replacement_text = config.file_replacement_text()

    def copy_folders_files_conditional(self):
        """
        This method prepares for copying files between source repo and Gitlab runner (build agent) local repo
        based on file_list configuration variable
        params:
        """
        print("create folder and copy files - starts ....")
        for file_to_copy in self.file_list:
            print("Copy file: " + file_to_copy)
            file_name = file_to_copy.split('/')[-1]
            folder_name = "/" + file_to_copy.replace(file_name, "")
            print("file_name: " + file_name)
            print("folder_name: " + folder_name)

            if file_name == 'setup.py':
                full_filename = self.source_repo_url + "/" + file_to_copy
                # print(full_filename)
                with fileinput.FileInput(full_filename, inplace=True) as file:
                    for line in file:
                        print(line.replace(self.text_to_search, self.replacement_text), end='')

            source_directory = self.source_repo_url + folder_name
            target_directory = self.local_gitlab_runner_repo + folder_name
            self.create_folder(target_directory, source_directory, file_name)
        print("create folder and copy files - ends ....")

    def create_folder(self, target_directory, source_directory, file_name: None):
        """
        This method create directory if not available and calls CopyFiles method
        params: targetDirectory, sourceDirectory, fileName
        """
        if not os.path.exists(target_directory):
            # print('Folder not found. Creating one')
            os.makedirs(target_directory)

        self.copy_files(target_directory, source_directory, file_name)

    @staticmethod
    def copy_files(target_directory, source_directory, file_name):
        """
        This method copy file from source to destination
        params: targetDirectory, sourceDirectory, fileName
        """
        file_path = source_directory + file_name
        _ = shutil.copy(file_path, target_directory)
