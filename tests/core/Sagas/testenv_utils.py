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

# Name of testing directory created for ingesting files
# from unittest import TestCase
import os
from distutils.util import strtobool
from typing import Union
import pandas as pd
from sqlalchemy import schema, Table, Column, Text
from hdm.core.state_management.state_manager import StateManager
from hdm.data_link_builder import DataLinkBuilder


class TestEnvUtils:
    PATH = os.path.dirname(os.path.realpath(__file__))
    CONF_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../test_assets")
    ENV = 'unit-test'

    TEST_DIR = os.getenv('TEST_DIR')  # 'tmptestsdir'
    if not TEST_DIR:
        TEST_DIR = os.path.join(os.getcwd(), 'test_data')
    # Delete tests directory after running unit tests

    KEEP_TESTS_DIR = os.getenv('KEEP_TESTS_DIR')  # False
    if not KEEP_TESTS_DIR:
        KEEP_TESTS_DIR = False
    else:
        KEEP_TESTS_DIR = strtobool(KEEP_TESTS_DIR.lower())

    # Number of Valid files in the testing directory
    NUM_VALID_FILES = os.getenv('NUM_VALID_FILES')  # 1
    if not NUM_VALID_FILES:
        NUM_VALID_FILES = 1
    else:
        NUM_VALID_FILES = int(NUM_VALID_FILES)

    # Number of records in each valid file to be processed
    MAX_NUMROWS = os.getenv('MAX_NUMROWS')  # 100
    if not MAX_NUMROWS:
        MAX_NUMROWS = 100
    else:
        MAX_NUMROWS = int(MAX_NUMROWS)

    # Columns in our test Dataframe object
    DF_MAXCOLS = os.getenv('DF_MAXCOLS')  # 5
    if not DF_MAXCOLS:
        DF_MAXCOLS = 5
    else:
        DF_MAXCOLS = int(DF_MAXCOLS)

    TEST_DB_NAME = os.getenv('TEST_DB_NAME')  # 'hdm'
    if not TEST_DB_NAME:
        TEST_DB_NAME = 'hdm'

    @classmethod
    def create_test_env(cls):
        test_dir = os.path.abspath(cls.TEST_DIR)
        if os.path.exists(test_dir):
            cls.cleanup(test_dir)

        os.mkdir(test_dir)
        csv_file_types = ['.csv', '.txt']
        valid_file_types = []
        valid_file_types.extend(csv_file_types)

        for valid_files in [[f"tmp_file_{i}{file_type}" for file_type in valid_file_types] for i in
                            range(1, cls.NUM_VALID_FILES + 1)]:
            for (index, file) in enumerate(valid_files):
                dupe_cols = True if (index % 3 == 0) else False
                cls.__create_csv_file(test_dir, file, dupe_cols)

    @classmethod
    def __create_csv_file(cls, directory: str, filename: str, dupe_col_names=False):
        with open(os.path.join(directory, filename), "w+") as fp:
            if dupe_col_names:
                header = 'Col-1,Col-2,Col-3\n'
            else:
                header = 'Col-1,Col-1,Col-1\n'
            fp.write(header)
            for i in range(0, cls.MAX_NUMROWS):
                col = 1
                data = f'{i}-{col},Data@ [{i}][{col + 1}],Data@ [{i}][{col + 2}]\n'
                fp.write(data)

    @classmethod
    def cleanup(cls, directory: str, keep_testing_directory=False):
        if not keep_testing_directory:
            dir_to_delete = os.path.abspath(directory)
            if os.path.exists(dir_to_delete):
                for file in os.listdir(dir_to_delete):
                    unlink_path = os.path.abspath(os.path.join(dir_to_delete, file))
                    if os.path.isdir(unlink_path):
                        cls.cleanup(unlink_path)
                    elif os.path.isfile(unlink_path):
                        os.unlink(unlink_path)
                os.rmdir(dir_to_delete)

    @classmethod
    def get_test_df(cls):
        df_dict = {}
        for col in range(0, cls.DF_MAXCOLS):
            df_dict.setdefault(f"Col-{col + 1}", [f"Data @{i}, {col + 1}" for i in range(0, cls.MAX_NUMROWS)])
        return pd.DataFrame(df_dict)

    @classmethod
    def set_environment_variables(cls):
        if 'HDM_HOME' not in os.environ.keys():
            os.environ['HDM_HOME'] = cls.CONF_PATH
        else:
            os.environ.update({'HDM_HOME': cls.CONF_PATH})

        if 'HDM_ENV' not in os.environ.keys():
            os.environ['HDM_ENV'] = cls.ENV
        else:
            os.environ.update({'HDM_ENV': cls.ENV})

    @classmethod
    def create_sm_table_sqlite(cls, sqlite_connection, table_name):
        metadata = schema.MetaData(bind=sqlite_connection)
        Table(table_name, metadata,
              Column('state_id', Text()),
              Column('run_id', Text()),
              Column('job_id', Text()),
              Column('correlation_id_in', Text()),
              Column('correlation_id_out', Text()),
              Column('action', Text()),
              Column('status', Text()),
              Column('source_name', Text()),
              Column('source_type', Text()),
              Column('sink_name', Text()),
              Column('sink_type', Text()),
              Column('source_entity', Text()),
              Column('source_filter', Text()),
              Column('sink_entity', Text()),
              Column('sink_filter', Text()),
              Column('first_record_pulled', Text()),
              Column('last_record_pulled', Text()),
              Column('git_sha', Text()),
              Column('sourcing_start_time', Text()),
              Column('sourcing_end_time', Text()),
              Column('sinking_start_time', Text()),
              Column('sinking_end_time', Text()),
              Column('updated_on', Text()),
              Column('row_count', Text()),
              Column('manifest_name', Text())
              )
        metadata.create_all(sqlite_connection.engine)

    @classmethod
    def delete_sm_table_sqlite(cls, sqlite_connection, table_name, remove_sqlite_db=True):
        try:
            with sqlite_connection.connection as conn:
                conn.execute(f"DROP TABLE if exists {table_name}")
        finally:
            db_path = os.path.join(cls.PATH, f"{cls.TEST_DB_NAME}.db")
            if remove_sqlite_db and os.path.exists(db_path):
                try:
                    os.unlink(db_path)
                except Exception:
                    raise ResourceWarning

    @classmethod
    def get_state_manager_details(cls,
                                  job_id: str,
                                  source_name: Union[str, None],
                                  source_type: Union[str, None],
                                  sink_name: Union[str, None],
                                  sink_type: Union[str, None]) -> dict:
        sm_conf = {'name': 'state_manager', 'type': 'SqLiteStateManager',
                   'conf': {'connection': 'state-manager-sqlite'}}
        link_state = cls.generate_state_manager(sm_conf)
        link_state.job_id = job_id
        link_state.source = {
            'name': source_name,
            'type': source_type,
        }
        link_state.sink = {
            'name': sink_name,
            'type': sink_type,
        }
        return link_state

    @classmethod
    def generate_state_manager(cls, sm_conf) -> StateManager:
        state_manager = DataLinkBuilder.build_state_manager(configuration=sm_conf)
        return state_manager
