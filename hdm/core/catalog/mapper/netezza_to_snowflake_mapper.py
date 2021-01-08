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


class NetezzaToSnowflakeMapper:
    @classmethod
    def _get_logger(cls):
        return logging.getLogger(cls.__name__)

    def __init__(self, **kwargs):
        self._logger = self._get_logger()
        self.__databases = kwargs['databases']
        self.__schemas = kwargs['schemas']
        self.__tables = kwargs['tables']
    
    def __map_databases(self) -> list:
        result = []
        for database in self.__databases:
            result.append(f"CREATE DATABASE IF NOT EXISTS {database}")
        return result

    def __map_schemas(self) -> list:
        result = []
        for schema in self.__schemas:
            result.append(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        return result

    @staticmethod
    def __map_column_type(column_type: str) -> str:
        result = column_type
        result = result.replace("CHARACTER VARYING", "VARCHAR")
        result = result.replace("DOUBLE PRECISION", "DOUBLE")
        result = result.replace("TIME WITH TIME ZONE", "TIMESTAMP_TZ")
        return result

    def __map_tables(self) -> list:
        result = []
        mapped_columns = {}
        for table, columnList in self.__tables.items():
            if table not in mapped_columns:
                mapped_columns[table] = []
            for column in columnList:
                column_name = column["columnName"]
                column_type = self.__map_column_type(column["columnType"])
                # ignore default for now, complicated to map
                not_null = " NOT NULL" if column["notNull"] else ""
                mapped_columns[table].append(f"{column_name} {column_type}{not_null}")
        
        for table, columnList in mapped_columns.items():
            # find out if this needs to be "OR REPLACE
            table_sql = f"CREATE TABLE IF NOT EXISTS {table} ("
            table_sql += ",".join(columnList)
            table_sql += ", CK_SUM INTEGER "
            table_sql += ")"
            result.append(table_sql)
        
        return result
    
    def map(self) -> tuple:
        database_sql = self.__map_databases()
        schema_sql = self.__map_schemas()
        table_sql = self.__map_tables()
        
        return database_sql, schema_sql, table_sql
