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
import argparse
import os
import logging.config

from hdm.core.utils.parse_config import ParseConfig
from hdm.migration_driver import MigrationDriver


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest", type=str, help="path of manifest to run")
    parser.add_argument("-l", "--log_settings", type=str, default="log_settings.yml", help="log settings path")
    parser.add_argument("-e", "--env", type=str, default="prod", help="environment to take connection information "
                                                                      "from in hdm_profiles.yml")
    args = parser.parse_args()

    os.environ['HMD_ENV'] = args.env
    log_settings = ParseConfig.parse(config_path=args.log_settings)
    logging.config.dictConfig(log_settings)

    os.environ['HDM_MANIFEST'] = args.manifest
    director = MigrationDriver()
    director.run()
