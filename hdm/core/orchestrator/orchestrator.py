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
import uuid

from hdm.core.error.hdm_error import HDMError
from hdm.core.state_management.state_manager import StateManager
from hdm.data_link_builder import DataLinkBuilder


class Orchestrator:

    @classmethod
    def _get_logger(cls):
        return logging.getLogger(cls.__name__)

    def __init__(self):
        self._logger = self._get_logger()
        self._data_links = []
        self._build_data_links()

    def run_pipelines(self):
        """Execute data transport DataLinks."""
        raise NotImplementedError(f'Method not implemented for {type(self).__name__}.')

    def _generate_state_manager(self, state_manager_config: dict, data_link_config: dict, manifest_name: str, run_id) -> StateManager:
        """
        Generate a StateManager for each
        Args:
            state_manager_config:
            data_link_config:

        Returns:

        """

        state_manager = DataLinkBuilder.build_state_manager(configuration=state_manager_config)

        if not isinstance(state_manager, StateManager):
            error = f'State manager {type(state_manager)} is not a StateManager.'
            self._logger.error(error)
            raise HDMError(error)

        state_manager.run_id = run_id
        state_manager.job_id = StateManager.generate_id()
        state_manager.manifest_name = manifest_name
        state_manager.source = {
            'name': data_link_config['source']['name'],
            'type': data_link_config['source']['type'],
        }
        state_manager.sink = {
            'name': data_link_config['sink']['name'],
            'type': data_link_config['sink']['type'],
        }

        return state_manager

    def _build_data_links(self):
        raise NotImplementedError(f'Method not implemented for {type(self).__name__}.')
