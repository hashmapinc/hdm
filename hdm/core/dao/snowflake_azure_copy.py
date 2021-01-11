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
from hdm.core.dao.snowflake_copy import SnowflakeCopy
from hdm.core.utils.project_config import ProjectConfig
import yaml


class SnowflakeAzureCopy(SnowflakeCopy):

    def _create_stage(self, stage_name, source_directory, connection):
        # TODO: Check with John. How to get azure connection info- hardcoded below? Manefist or profile yml
        with open(f"{ProjectConfig.hdm_home()}/{ProjectConfig.profile_path()}", 'r') as stream:
            conn_conf = yaml.safe_load(stream)[ProjectConfig.hdm_env()]['azure'] # Fai: Pull from manifest "azure_env" the azure env name here instead of hardcode

        url = conn_conf['url'].replace("https","azure") + self._stage_directory
        self.__external_stage_params = f"URL = '{url}' CREDENTIALS = ( AZURE_SAS_TOKEN = '{conn_conf['sas']}')"
        cursor = connection.cursor()
        # create stage
        self._logger.info("Creating stage: %s", self._stage_name)
        create_stage_sql = f"CREATE OR REPLACE STAGE {self._stage_name}"
        create_stage_sql += f" {self.__external_stage_params}"
        cursor.execute(create_stage_sql)
        cursor.close()
