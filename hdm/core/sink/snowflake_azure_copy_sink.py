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
from hdm.core.dao.snowflake_azure_copy import SnowflakeAzureCopy
from hdm.core.sink.snowflake_copy_sink import SnowflakeCopySink


class SnowflakeAzureCopySink(SnowflakeCopySink):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._dao = SnowflakeAzureCopy(connection=kwargs['env'], stage_directory=kwargs['stage_directory'],
                                       stage_name=kwargs['stage_name'])

    def _generate_query(self, file_name, table_name) -> None:
        text = f"'.*{file_name}.*'"
        self._query = f"COPY INTO {table_name} " \
                      f"FROM @{self._stage_name} " \
                      f"FILE_FORMAT = (TYPE = {self._file_format} SKIP_HEADER = 1) " \
                      f"PATTERN = {text}" \
                      f"PURGE = TRUE "

        # self._count_query = f"SELECT COUNT(*) FROM {table_name} "
