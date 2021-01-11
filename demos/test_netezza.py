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
import os
import argparse
import logging.config

from hdm.core.catalog.crawler.netezza_crawler import NetezzaCrawler
from hdm.core.catalog.ddl_writer.snowflake_ddl_writer import SnowflakeDDLWriter
from hdm.core.catalog.mapper.netezza_to_snowflake_mapper import NetezzaToSnowflakeMapper
from hdm.core.utils.parse_config import ParseConfig

os.environ['KNERRIR_ENV'] = 'dev'

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--log_settings", type=str, default="log_settings.yml", help="log settings path")
    args = parser.parse_args()
    log_settings = ParseConfig.parse(config_path=args.log_settings)
    logging.config.dictConfig(log_settings)

    x = NetezzaCrawler(env='netezza')
    data_tuple = x.run()
    y = NetezzaToSnowflakeMapper(databases=data_tuple[0], schemas=data_tuple[1], tables=data_tuple[2])
    sql_tuple = y.map()
    z = SnowflakeDDLWriter(env='snowflake_knerrir_db',database_sql=sql_tuple[0], schema_sql=sql_tuple[1], table_sql=sql_tuple[2])
    z.execute()
