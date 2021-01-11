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
import pandas as pd

from hdm.core.dao.mysql import MySQL
from hdm.core.source.rdbms_source import RDBMSSource


# TODO: This is most likely out of date.
class MySQLSource(RDBMSSource):

    def consume(self, **kwargs) -> dict:
        self._entity_filter = self.__table
        yield self._run(**kwargs)

    def _get_data(self, **kwargs) -> dict:
        with MySQL(connection=self.__connection_choice).connection as conn:
            cursor = conn.cursor()
            cursor.execute(self.__query)
            df = pd.DataFrame.from_records(cursor, columns=cursor.column_names)

        return {'data_frame': df,
                'record_count': df.shape[0]}

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.__connection_choice = kwargs['env']
        
        self.__database = kwargs['database']
        self.__table = kwargs['params']['table']
        self.__watermark = kwargs['params']['watermark']

        #TODO: Need to use a query builder for safety.
        self.__query = f"SELECT * FROM `{self.__database}`.`{self.__table}`"
        if self.__watermark:
            self.__query = " ".join(
                [
                    self.__query,
                    f"WHERE {self.__watermark['column']} > {self.__watermark['offset']}"
                ]
            )
