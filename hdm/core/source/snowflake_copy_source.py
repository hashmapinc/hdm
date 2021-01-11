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
import snowflake.connector as connector

from hdm.core.source.rdbms_source import RDBMSSource


# TODO : check and Remove this. not needed anymore
class SnowflakeCopySource(RDBMSSource):

    # TODO: This isn't secure
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.conn = connector.connect(
            user=kwargs['user'],
            password=kwargs['password'],
            account=kwargs['account'],
            authenticator=kwargs['authenticator'],
            warehouse=kwargs['warehouse'],
            database=kwargs['database']
        )

        self.cursor = self.conn.cursor()
        self._entity = f"{kwargs['warehouse']}.{kwargs['database']}"

    def register(self, **kwargs):
        """
        Parameters needed will be: Warehouse, Database, Schema, Table  and the query to grab the data and load in
        memory (small data) or save off as csv/parquet files (big data) -> Need to pass on to the sink the variable
        with the data or the location of the data to the sink so that it can be extracted.

        Can pass in some optional SQL to generate the table, database, schema, warehouse on the fly.

        Args:
            **kwargs:

        Returns:

        """

        snow_sql_list = kwargs['snow_sql_list']
        _ = [self.cursor.execute(sql) for sql in snow_sql_list]

    def copy_data(self, **kwargs):
        """
        Copies data in memory from source, runs the sql query as written in the yml file,
        # todo: should also provide a place for the user to pass in a sql file.

        Args:
            **kwargs:

        Returns:

        """
        snow_sql_query = kwargs['snow_sql_query']
        data = [self.cursor.execute(sql) for sql in snow_sql_query]
        return data
