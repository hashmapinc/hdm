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
import logging

from providah.factories.package_factory import PackageFactory as pf


class NetezzaCrawler:
    @classmethod
    def _get_logger(cls):
        return logging.getLogger(cls.__name__)

    def __init__(self, **kwargs):
        self._logger = self._get_logger()
        self._connection_name = kwargs.get('connection_name')

    def run(self) -> tuple:
        databases = self.__get_database_names()
        schemas = []
        tables = {}
        for db in databases:
            schemas.extend(self.__get_schema_names_by_db(db))
            tables.update(self.__get_tables_by_db(db))
        return databases, schemas, tables

    def __get_database_names(self) -> list:
        dao = pf.create(key=self._connection_name, configuration={'connection': self._connection_name})
        query_string = "SELECT DATABASE FROM _V_DATABASE WHERE DATABASE <> 'SYSTEM'"
        databases = []
        try:
            with dao.connection as conn:
                cursor = conn.cursor()
                cursor.execute(query_string)
                result = cursor.fetchall()
                for row in result:
                    databases.append(row[0])
            return databases
        finally:
            if cursor:
                cursor.close()

    def __get_schema_names_by_db(self, database) -> list:
        dao = pf.create(key=self._connection_name, configuration={'connection': self._connection_name})
        query_string = f"SELECT DISTINCT SCHEMA FROM {database}.._V_SCHEMA"  # WHERE OBJTYPE = 'TABLE'"
        schemas = []
        try:
            with dao.connection as conn:
                cursor = conn.cursor()
                cursor.execute(query_string)
                result = cursor.fetchall()
                for row in result:
                    schemas.append(row[0])
            return schemas
        finally:
            if cursor:
                cursor.close()
    
    def __get_tables_by_db(self, database) -> dict:
        dao = pf.create(key=self._connection_name, configuration={'connection': self._connection_name})
        query_string = f"SELECT DATABASE, SCHEMA, NAME, ATTNAME, FORMAT_TYPE, ATTLEN, ATTNOTNULL, COLDEFAULT " \
                       f"FROM {database}.._V_RELATION_COLUMN " \
                       f"WHERE DATABASE <> 'SYSTEM' AND TYPE = 'TABLE' ORDER BY SCHEMA, NAME, ATTNUM ASC"
        tables = {}

        with dao.connection as conn:
            cursor = conn.cursor()
            cursor.execute(query_string)
            result = cursor.fetchall()
            for row in result:
                table_name = f"{row[0]}.{row[1]}.{row[2]}"  # ignoring name collisions across multiple db's for now
                if table_name not in tables:
                    tables[table_name] = []
                
                column = {
                    'database': row[0],
                    'schema': row[1],
                    'name': row[2],
                    'columnName': row[3],
                    'columnType': row[4],
                    'columnSize': row[5],
                    'notNull': row[6],
                    'default': row[7]
                }
                tables[table_name].append(column)
        
        return tables
