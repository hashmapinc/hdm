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
from hdm.core.sink.sink import Sink
from hdm.core.dao.snowflake import Snowflake
from pandas import DataFrame
import os
from tempfile import TemporaryDirectory


# pandas_tools in the snowflake python connector has bugs prevent it from being used, so have to upload to stages and
# copy into tables manually
# TODO : check and Remove this. not needed anymore
class SnowflakeInternalStageSink(Sink):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.__env = kwargs['env']
        self.__dao = Snowflake(connection=self.__env)
        self.__stage_name = kwargs['stage_name']
        self.__table_name = kwargs['table_name']
        self._entity = f"{self.__stage_name}.{self.__table_name}"

    def produce(self, **kwargs):
        self._run(**kwargs)

    # takes a dictionary of {'data_frame': DataFrame}
    def _set_data(self, **kwargs) -> dict:
        df = kwargs['data_frame']
        # write dataframe to temp file. does not handle large data well. see pandas_tools.py in the snowflake
        # connector for how to chunk properly
        with TemporaryDirectory() as tmp_folder:
            file_path = os.path.join(tmp_folder, f'{self.__table_name}.csv.gz')
            self.__create_temp_file(df, file_path)

            with self.__dao.connection as conn:
                cursor = conn.cursor()

                # create stage
                create_stage_sql = f"CREATE STAGE IF NOT EXISTS {self.__stage_name}"
                cursor.execute(create_stage_sql)

                # run put command
                upload_sql = f"PUT file://{file_path} @{self.__stage_name} OVERWRITE = TRUE"
                cursor.execute(upload_sql)
        return dict(record_count=df.shape[0])

    def __create_temp_file(self, df: DataFrame, file_path: str):
        # csv file created has issues with timetz column types, removed for now
        df.to_csv(file_path, index=False)
