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
import json

import pandas as pd
import enum

from hdm.core.dao.netezza_jdbc import NetezzaJDBC
from hdm.core.dao.netezza_odbc import NetezzaODBC
from hdm.core.source.rdbms_source import RDBMSSource
from hdm.core.utils.project_config import ProjectConfig


class HashFunction(enum.Enum):
    HASH8 = "hash8"
    HASH4 = "hash4"
    HASH = "hash"


class NetezzaSource(RDBMSSource):

    def consume(self, **kwargs) -> dict:
        self._logger.info("Retrieving data from %s", self.__table)
        yield self._run(**kwargs)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.__connection_choice = kwargs['env']
        self.__table = kwargs['table_name']
        self.__watermark = kwargs.get('watermark', [])
        self.__driver_type = kwargs.get('driver_type')
        self._entity = self.__table
        self._entity_filter = kwargs.get('watermark', None)
        self.__checksum = kwargs.get('checksum', [])
        self.__query = ""

    def _get_data(self, **kwargs) -> dict:
        self.__build_query()
        try:
            with NetezzaJDBC(connection=self.__connection_choice).connection as conn:
                df = pd.read_sql(self.__query, conn)
                return {'data_frame': df,
                        'record_count': df.shape[0],
                        'source_type': 'database',
                        'table_name': self.__table}
        except Exception:
            try:
                with NetezzaODBC(connection=self.__connection_choice).connection as conn:
                    df = pd.read_sql(self.__query, conn)
                    return {'data_frame': df,
                            'record_count': df.shape[0],
                            'source_type': 'database',
                            'table_name': self.__table}
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
        self.__query = f"SELECT * , CAST(random()* 100000 AS INT) as ck_sum FROM {self.__table}"

        # Checksum
        if self.__checksum and self.__checksum['function'] and self.__checksum['column']:
            self.__query = self._generate_checksum_select_query(checksum=self.__checksum, table_name=self.__table)

        # Watermark
        if self.__watermark and self.__watermark['column'] and self.__watermark['offset']:
            where_clause = self._generate_watermarked_where_clause(watermark=self.__watermark,
                                                                   last_data_pulled=self._last_record_pulled
                                                                   )
            self.__query = " ".join(
                [
                    self.__query,
                    where_clause
                ]
            )

        if ProjectConfig.query_limit():
            self.__query += f" LIMIT {ProjectConfig.query_limit()}"

    @classmethod
    def _generate_checksum_select_query(self, checksum: dict, table_name) -> str:
        # TODO: checksum is either on all or selection of columns - not a single column.
        select_query = f"SELECT * , " \
                       f"{checksum['function'].lower()}('{checksum['column'].lower()}',0) as ck_sum  " \
                       f"FROM {table_name}"
        if checksum['function'].lower() == HashFunction.HASH8.value:
            select_query = f"SELECT * , " \
                           f"{checksum['function'].lower()}('{checksum['column'].lower()}') as ck_sum  " \
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
    def _generate_watermarked_where_clause(self, watermark: dict, last_data_pulled, split_column) -> str:
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

        if last_data_pulled:
            res = last_data_pulled.split(',')
            for val in res:
                arr = val.split(":")
                if split_column == arr[0]:
                    where_clause += f" AND {split_column} > {arr[1]}"

        return where_clause
    """
    """
    @classmethod
    def _generate_watermarked_order_clause(self, watermark: dict) -> str:
        order_clause = f"ORDER BY {watermark['column']}"
        return order_clause
    """
