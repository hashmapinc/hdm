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
import git
from clone.config.netezza_config import NetezzaConfig


class GitOperations:

    def __init__(self, config: NetezzaConfig):
        self.target_repo_url = config.git_target_repo_url()
        self.local_gitlab_runner_repo = config.git_local_gitlab_runner_repo()
        self.repo = ""

    def git_clone_target_repo(self):
        """
        This method clone a remote target repo to a local repo in Gitlab runner (build agent)
        params: target_repo_url, local_gitlab_runner_repo
        """
        self.repo = git.Repo.clone_from(self.target_repo_url, self.local_gitlab_runner_repo)
        print("Cloning Repo - completed ....")

    def git_commit_and_push(self):
        """
        This method adds, commits and push changes to remote repo
        params: repo
        """
        self.repo.git.add('--all')
        self.repo.index.commit("committed changes")
        origin = self.repo.remote('origin')
        origin.push('master')
        self.repo.git.add(update=True)
        print("Commit and push changes - completed....")
