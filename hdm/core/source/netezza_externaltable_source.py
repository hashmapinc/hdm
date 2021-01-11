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
import csv
import os
import time
import uuid

import jaydebeapi as connector
import enum

from hdm.core.dao.netezza_jdbc import NetezzaJDBC
from hdm.core.dao.netezza_odbc import NetezzaODBC
from hdm.core.source.source import Source
from hdm.core.utils.generic_functions import GenericFunctions
from hdm.core.utils.project_config import ProjectConfig
import pandas as pd


class HashFunction(enum.Enum):
    HASH8 = "hash8"
    HASH4 = "hash4"
    HASH = "hash"


class NetezzaExternalTableSource(Source):
    """
    Used when Netezza external table is used for data offload.
        - get column names and type from data table
        - create external table using column names
    Note: before each run if external table exist it is dropped and created.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.__dest_path = os.path.abspath(kwargs.get('directory'))
        if not os.path.exists(self.__dest_path):
            raise NotADirectoryError("Unable to create Sink landing at %s" % self.__dest_path)
        # TODO - More file types for later
        self.__chunk = kwargs.get('chunk', None)
        self.__checksum = kwargs.get('checksum_method', None)
        self.__file_format = kwargs.get('file_format', 'csv')
        self.__connection_choice = kwargs['env']
        self.__table = kwargs['table_name']
        self.__watermark = kwargs.get('watermark', [])
        self.__unload_file_name = ""
        self.__external_table_name = f"{self.__table}_{ProjectConfig.file_prefix().upper()}_EXT"
        self.__drop_query = f"DROP TABLE {self.__external_table_name} IF EXISTS;"
        self.__query = ""
        self._entity = self.__table
        self._entity_filter = self.__watermark
        self._correlation_id_out = uuid.uuid4().hex
        if self.__file_format == 'csv':
            self.__unload_file_name = kwargs.get("file_name", f"{ProjectConfig.file_prefix()}"
                                                              f"_{self._correlation_id_out}.csv")
        else:
            raise NotImplementedError("Unknown output type: %s" % self.__file_format)
        self._sink_entity = self.__unload_file_name

        file_path = os.path.abspath(os.path.join(self.__dest_path, self._sink_name, GenericFunctions.table_to_folder(self.__table)))
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        self.__unload_file = os.path.abspath(os.path.join(file_path, self.__unload_file_name))
        self.__driver_type = ""
        self.__external_table_query = ""

    def consume(self, **kwargs) -> dict:
        yield self._run(**kwargs)

    def _get_data(self, **kwargs) -> dict:
        self.__build_query()
        try:
            with NetezzaJDBC(connection=self.__connection_choice).connection as conn:
                self.__driver_type = 'JDBC' if isinstance(conn, connector.Connection) else 'ODBC'
                self.__generate_ext_table_by_columns(conn)

                cursor = conn.cursor()
                cursor.execute(self.__drop_query)
                cursor.execute(self.__external_table_query)
                cursor.execute(self.__query)
                df = pd.read_csv(self.__unload_file)
                return {'data_frame': df,
                        'record_count': df.shape[0],
                        'source_type': 'database'}
        except Exception:
            try:
                with NetezzaODBC(connection=self.__connection_choice).connection as conn:
                    self.__driver_type = 'JDBC' if isinstance(conn, connector.Connection) else 'ODBC'
                    self.__generate_ext_table_by_columns(conn)

                    cursor = conn.cursor()
                    cursor.execute(self.__drop_query)
                    cursor.execute(self.__external_table_query)
                    cursor.execute(self.__query)
                    df = pd.read_csv(self.__unload_file)
                    return {'data_frame': df,
                            'record_count': df.shape[0],
                            'source_type': 'database'}
            except Exception:
                raise ValueError("Unknown configuration: %s" % self.__connection_choice)

    def __build_query(self) -> None:
        """
        builds query.
        Default is no check_sum column
        checksum_methods:
        default method: generates random numbers as checksum value
        hash method:
            ** Important: Hash function needs IBM Netezza SQL Extensions toolkit installed
            checksum_method : hash
            hash_column  is the column to be hashed
            hash_function supported are :
                hash4 (returns the 32 bit checksum hash of the input data.)
                hash8 (returns the 64 bit hash of the input data)
                hash (returns hashed input data)
            ** Important: hash() function is much slower to calculate than hash4() and hash8()

        Returns: none

        """
        self.__query = f"INSERT INTO {self.__external_table_name} " \
                       f"SELECT * , CAST(random()* 100000 AS INT) as ck_sum FROM {self.__table}"

        # Checksum
        if self.__checksum and self.__checksum['function'] and self.__checksum['column']:
            self.__query = self._generate_checksum_select_query(checksum=self.__checksum,
                                                                table_name=self.__table,
                                                                external_table_name=self.__external_table_name)

        # Watermark
        if self.__watermark and self.__watermark['column'] and self.__watermark['offset']:
            where_clause = self._generate_watermarked_where_clause(watermark=self.__watermark,
                                                                   last_data_pulled=self._last_record_pulled)
            self.__query = " ".join(
                [
                    self.__query,
                    where_clause
                ]
            )
        if ProjectConfig.query_limit():
            self.__query += f" LIMIT {ProjectConfig.query_limit()}"

    def __generate_ext_table_by_columns(self, conn) -> None:
        cursor = conn.cursor()
        qry = f"SELECT COLUMN_NAME, DATA_TYPE " \
              f"FROM INFORMATION_SCHEMA.COLUMNS WHERE " \
              f"TABLE_SCHEMA ||'.'||TABLE_NAME  = '{self.__table}'"
        cursor.execute(qry)
        result = cursor.fetchall()
        cols = ""
        for row in result:
            cols += "," + row[0] + ' ' + row[1]
        cols += "," + "CK_SUM INTEGER"
        self.__external_table_query = f"CREATE EXTERNAL TABLE {self.__external_table_name}({cols[1:]})" \
                                      f" USING ( DATAOBJECT ('{self.__unload_file}')  DELIMITER ',' " \
                                      f"REMOTESOURCE '{self.__driver_type}' INCLUDEHEADER);"

    @classmethod
    def _generate_checksum_select_query(self, checksum: dict, table_name, external_table_name) -> str:
        # TODO: checksum is either on all or selection of columns - not a single column.
        select_query = f"INSERT INTO {external_table_name} SELECT *, " \
                       f"{checksum['function'].lower()}('{checksum['column'].lower()}',0) as ck_sum " \
                       f"FROM {table_name}"
        if checksum['function'].lower() == HashFunction.HASH8.value:
            select_query = f"INSERT INTO {external_table_name} SELECT *, " \
                           f"{checksum['function'].lower()}('{checksum['column'].lower()}') as ck_sum " \
                           f"FROM {table_name}"

        return select_query

    @classmethod
    def _generate_watermarked_where_clause(self, watermark: dict, last_data_pulled) -> str:
        where_clause = f"WHERE {watermark['column']} > {watermark['offset']}"
        if last_data_pulled:
            column_arr = last_data_pulled.split(',')
            for key_value in column_arr:
                key_value_arr = key_value.split(":")
                if watermark['column'] == key_value_arr[0]:
                    where_clause += f" AND {watermark['column']} > {key_value_arr[1]}"

        where_clause += f" ORDER BY {watermark['column']}"
        return where_clause

    """
    # per discussion , considering only 1 watermark column and greater than relationship
    @classmethod
    def _generate_watermarked_where_clause(self, watermark: dict) -> str:
        relation = watermark['relation']
        if relation == 'eq':
            where_clause = f"WHERE {watermark['column']} = {watermark['offset']}"
        elif relation == 'gt':
            where_clause = f"WHERE {watermark['column']} > {watermark['offset']}"
        elif relation == 'gte':
            where_clause = f"WHERE {watermark['column']} >= {watermark['offset']}"
        elif relation == 'lt':
            where_clause = f"WHERE {watermark['column']} < {watermark['offset']}"
        elif relation == 'lte':
            where_clause = f"WHERE {watermark['column']} lte {watermark['offset']}"
        else:
            raise ValueError(f'relation "{relation}" is not a valid relationship')
        return where_clause
    """
    """
    @classmethod
    def _generate_watermarked_order_clause(self, watermark: dict) -> str:
        order_clause = f"ORDER BY {watermark['column']}"
        return order_clause
    """
