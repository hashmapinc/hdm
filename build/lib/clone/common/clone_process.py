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

# TODO: This text seems wrong.
################################################################################################################
# VERSION: 1.0
# NAME: test-config.py
# LOCATION: config folder
# PURPOSE: The purpose of this script is to copy folders and files from one Repo to another Repo.
# ASSUMPTION: The assumption is that both source and Target repos exist. The target repo can be a empty one.
# ADDITIONAL INFORMATION: To access the remote Repo from pipeline.yml file we need to generate a
#                         Gitlab Personal Access Token
#####################################################################################################################

from clone.helper.file_operations import FileOperations
from clone.helper.git_operations import GitOperations


class CloneProcess:
    def __init__(self, config):
        self.config = config

    def clone_repo(self):
        try:
            # Clone Target Repo
            git_operations = GitOperations(self.config)
            git_operations.git_clone_target_repo()

            # Copy Files
            file_operations = FileOperations(self.config)
            file_operations.copy_folders_files_conditional()

            # Commit and push changes to remote
            git_operations.git_commit_and_push()

        except Exception as e:
            print(str(e))
