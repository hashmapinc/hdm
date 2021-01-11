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
import platform
import os


class ProjectConfig:
    @classmethod
    def hdm_home(cls):
        if not os.getenv('HDM_HOME'):
            if platform.system().lower() != 'windows':
                os.environ['HDM_HOME'] = os.getenv('HOME')
            else:
                os.environ['HDM_HOME'] = os.getenv('USERPROFILE')
        return os.getenv('HDM_HOME')

    @classmethod
    def hdm_env(cls):
        env = os.getenv('HDM_ENV')
        if not env:
            env = 'dev'
            os.environ['HDM_ENV'] = env
        return env

    @classmethod
    def profile_path(cls):
        return ".hashmap_data_migrator/hdm_profiles.yml"

    @classmethod
    def file_prefix(cls):
        return 'hdm'

    @classmethod
    def state_manager_table_name(cls):
        return 'state_manager'

    @classmethod
    def archive_folder(cls):
        return 'archive'

    @classmethod
    def connection_max_attempts(cls):
        return 3

    @classmethod
    def connection_timeout(cls):
        return 3

    @classmethod
    def query_limit(cls):
        return '250'
