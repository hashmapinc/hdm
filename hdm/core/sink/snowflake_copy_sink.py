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
from hdm.core.sink.rdbms_sink import RDBMSSink


class SnowflakeCopySink(RDBMSSink):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Query content
        self._file_format = kwargs['file_format']
        self._stage_name = kwargs['stage_name']
        self._query = ""
        # self._count_query = ""

    def produce(self, **kwargs) -> None:
        self._run(**kwargs)

    def _set_data(self, **kwargs) -> dict:
        file_name = kwargs['file_name']
        table_name = kwargs['table_name']
        df = kwargs['data_frame']
        with self._dao.connection as conn:
            self._entity = f"{self._stage_name}.{table_name}"
            self._entity_filter = None
            self._generate_query(file_name, table_name)

            cursor = conn.cursor()
            self._logger.info("Executing copy from @%s into %s", self._stage_name, table_name)
            cursor.execute(self._query)

            # cursor.execute(self._count_query)
            # records = cursor.fetchone()
        return {'record_count': str(df.shape[0])}

    def _generate_query(self, table_name) -> None:
        raise NotImplementedError
