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
import os

from providah.factories.package_factory import PackageFactory as pf

from hdm.core.orchestrator.orchestrator import Orchestrator
from hdm.core.utils.parse_config import ParseConfig


class MigrationDriver:
    __logger = logging.getLogger(__name__)

    def __init__(self, **kwargs):

        # Retrieve configuration path
        self._config_path = kwargs.get('config_path')

        # Set up orchestration engine
        self._initiate_orchestrator()

    def _initiate_orchestrator(self):
        orchestrator_config = ParseConfig.parse(config_path=os.getenv('HDM_MANIFEST'))['orchestrator']
        self._orchestrator: Orchestrator = pf.create(key=orchestrator_config['type'], configuration=orchestrator_config['conf'])

    def run(self):
        self._orchestrator.run_pipelines()
