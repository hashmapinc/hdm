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
import itertools


class NetezzaConfig:
    def __init__(self):
        # Note: Need to figure out a alternate way to access remote git. The personal_access_token
        # is per user. This is just for initial development and testing only.

        self.source_repo_url = "/builds/hashmapinc/ctso/accelerators/data-engineering/hashmap_data_migrator"
        self.target_repo_url = "https://gitlab-ci-token:" + \
                               os.environ['GIT_PERSONAL_ACCESS_TOKEN'] + \
                               "@gitlab.com/hashmapinc/ctso/accelerators/data-engineering/hashmap_data_migrator-" \
                               "netezza.git"

        self.local_gitlab_runner_repo = "temp-repo"
        self.text_to_search = 'name="hashmap-data-migrator"'
        self.replacement_text = 'name="hashmap-data-migrator-netezza"'

        self.hdm_files = []
        self.manifests_files = []
        self.root_files = []

        self.__get_root()
        self.__get_hdm()
        self.__get_manifests()

        self.file_list = list(itertools.chain(self.root_files,
                                              self.manifests_files,
                                              self.hdm_files))

    def git_source_repo_url(self):
        return self.source_repo_url

    def git_target_repo_url(self):
        return self.target_repo_url

    def git_local_gitlab_runner_repo(self):
        return self.local_gitlab_runner_repo

    def file_list_to_copy(self):
        return self.file_list

    def file_text_to_search(self):
        return self.text_to_search

    def file_replacement_text(self):
        return self.replacement_text

    def __get_root(self):
        self.root_files = ['setup.py', 'requirements.txt', '.gitignore', 'Pipfile', 'Pipfile.lock', 'log_settings.yml', 'README.md']

    def __get_manifests(self):
        self.manifests_files = ['manifests/manual_example.yml', 'manifests/manual_example_external_table.yml', 'manifests/batch_example.yml',
                                'manifests/batch_example_external_table.yml']

    def __get_hdm(self):
        hdm_root_files = ['hdm/__init__.py', 'hdm/data_link.py', 'hdm/data_link.py', 'hdm/data_link_builder.py', 'hdm/hashmap_data_migrator.py',
                          'hdm/migration_driver.py']

        hdm_core_root_files = ['hdm/core/__init__.py']
        hdm_core_catalog_files = ['hdm/core/catalog/__init__.py',
                                  'hdm/core/catalog/crawler/__init__.py',
                                  'hdm/core/catalog/crawler/netezza_crawler.py',
                                  'hdm/core/catalog/ddl_writer/__init__.py',
                                  'hdm/core/catalog/ddl_writer/snowflake_ddl_writer.py',
                                  'hdm/core/catalog/mapper/__init__.py',
                                  'hdm/core/catalog/mapper/netezza_to_snowflake_mapper.py']

        hdm_core_dao_files = ['hdm/core/dao/__init__.py',
                              'hdm/core/dao/dao.py',
                              'hdm/core/dao/netezza.py',
                              'hdm/core/dao/netezza_jdbc.py',
                              'hdm/core/dao/netezza_odbc.py',
                              'hdm/core/dao/azure_blob.py',
                              'hdm/core/dao/mysql.py',
                              'hdm/core/dao/snowflake.py',
                              'hdm/core/dao/snowflake_azure_copy.py',
                              'hdm/core/dao/snowflake_copy.py',
                              'hdm/core/dao/sqlite.py',
                              'hdm/core/dao/sql_server.py',
                              'hdm/core/dao/azure_sql_server.py']

        hdm_core_error_files = ['hdm/core/error/__init__.py', 'hdm/core/error/hdm_error.py']

        hdm_core_orchestrator_files = ['hdm/core/orchestrator/__init__.py',
                                       'hdm/core/orchestrator/auto_orchestrator.py',
                                       'hdm/core/orchestrator/batch_orchestrator.py',
                                       'hdm/core/orchestrator/declared_orchestrator.py',
                                       'hdm/core/orchestrator/orchestrator.py']

        hdm_core_sink_files = ['hdm/core/sink/__init__.py',
                               'hdm/core/sink/azure_blob_sink.py',
                               'hdm/core/sink/dummy_sink.py',
                               'hdm/core/sink/fs_sink.py',
                               'hdm/core/sink/sink.py',
                               'hdm/core/sink/snowflake_copy_sink.py',
                               'hdm/core/sink/snowflake_azure_copy_sink.py',
                               'hdm/core/sink/rdbms_sink.py']

        hdm_core_source_files = ['hdm/core/source/__init__.py',
                                 'hdm/core/source/fs_chunk_source.py',
                                 'hdm/core/source/fs_source.py',
                                 'hdm/core/source/netezza_externaltable_source.py',
                                 'hdm/core/source/netezza_source.py',
                                 'hdm/core/source/source.py',
                                 'hdm/core/source/azure_blob_source.py',
                                 'hdm/core/source/rdbms_source.py']

        hdm_state_management_files = ['hdm/core/state_management/__init__.py',
                                      'hdm/core/state_management/mysql_state_manager.py',
                                      'hdm/core/state_management/sqlite_state_manager.py',
                                      'hdm/core/state_management/sql_server_state_manager.py',
                                      'hdm/core/state_management/azure_sql_server_state_manager.py',
                                      'hdm/core/state_management/state_manager.py',
                                      'hdm/core/state_management/sql_schemas/state_manager_ddl.sql',
                                      'hdm/core/state_management/sql_schemas/sqlserver_state_manager_ddl.sql',
                                      'hdm/core/state_management/sql_schemas/sqlite_state_manager_ddl.sql']

        hdm_core_utils_files = ['hdm/core/utils/__init__.py',
                                'hdm/core/utils/generic_functions.py',
                                'hdm/core/utils/parse_config.py',
                                'hdm/core/utils/project_config.py']

        self.hdm_files = list(itertools.chain(hdm_root_files,
                                              hdm_core_root_files,
                                              hdm_core_catalog_files,
                                              hdm_core_dao_files,
                                              hdm_core_error_files,
                                              hdm_core_orchestrator_files,
                                              hdm_core_sink_files,
                                              hdm_core_source_files,
                                              hdm_state_management_files,
                                              hdm_core_utils_files))
