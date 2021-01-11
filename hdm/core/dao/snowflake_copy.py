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
from hdm.core.dao.snowflake import Snowflake


class SnowflakeCopy(Snowflake):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._stage_directory = kwargs['stage_directory']
        self._stage_name = kwargs['stage_name']

    @property
    def connection(self):

        connection = self._get_connection()

        self._create_stage(self._stage_name, self._stage_directory, connection)

        return connection

    def _create_stage(self, stage_name, source_directory, connection):
        raise NotImplementedError(f'Method not implemented for {type(self).__name__}.')

    def _stage_not_exist(self):
        cursor = self._connection.cursor()
        self._logger.info("Check for available stage: %s", self._stage_directory)
        show_stage_sql = f"SHOW STAGES LIKE '{self._stage_name}'"
        cursor.execute(show_stage_sql)
        record = cursor.fetchone()
        cursor.close()
        if record:
            return True
        return False
