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
import pyodbc
import yaml

from hdm.core.dao.netezza import Netezza
from hdm.core.utils.project_config import ProjectConfig


class NetezzaODBC(Netezza):

    def _validate_configuration(self) -> bool:
        with open(f"{ProjectConfig.hdm_home()}/{ProjectConfig.profile_path()}", 'r') as stream:
            conn_conf = yaml.safe_load(stream)[ProjectConfig.hdm_env()][self._connection_name]

        required_keys = ['user', 'password', 'host', 'port', 'database', 'driver']
        is_valid = all([key in conn_conf.keys() for key in required_keys])
        """
        if is_valid:
            required_keys = ['connection_string']
            return all([key in conn_conf['driver'].keys() for key in required_keys])
        """
        return is_valid

    def _get_connection_config(self, config: dict) -> dict:
        return dict(driver=config['driver'],
                    connection_string=f"DRIVER={config['driver']};SERVER={config['host']};"
                                      f"PORT={config['port']};DATABASE={config['database']};"
                                      f"UID={config['user']};PWD={config['password']};")

    def _connect_by_connector(self, config: dict) -> None:
        return pyodbc.connect(config['connection_string'])
