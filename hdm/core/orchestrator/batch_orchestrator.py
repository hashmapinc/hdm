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
import uuid
from copy import deepcopy

from hdm.core.error.hdm_error import HDMError
from hdm.core.orchestrator.orchestrator import Orchestrator
from hdm.core.sink.sink import Sink
from hdm.core.source.source import Source
from hdm.core.utils.parse_config import ParseConfig
from hdm.data_link import DataLink
from hdm.data_link_builder import DataLinkBuilder


class BatchOrchestrator(Orchestrator):

    def __init__(self, **kwargs):
        self._back_pressure_factor = kwargs.get('back_pressure_factor')
        super().__init__()

    def run_pipelines(self):

        # Run the declared data links - these are links that will always run.
        _ = [hdm.run() for hdm in self._data_links if hdm.pressureless]
        pressurable_links = [hdm for hdm in self._data_links if not hdm.pressureless]

        # Track the number of pipelines running
        running_count = 0

        # This are the pipelines that
        running_links = []
        for link in pressurable_links:
            if running_count < self._back_pressure_factor:
                link.run()
                running_links.append(link)

            # If a pipeline has finished, then remove it. This makes room for another pipeline.
            while running_count == self._back_pressure_factor:
                _ = [running_links.pop(running_link) for running_link in running_links if not running_link.is_running]

    def _build_data_links(self):
        config = ParseConfig.parse(config_path=os.getenv('HDM_MANIFEST'))
        state_manager_config = config['state_manager']
        manifest_name = os.getenv('HDM_MANIFEST')[os.getenv('HDM_MANIFEST').rindex("/")+1:]
        run_id = uuid.uuid4().hex

        if 'template_data_links' in config.keys():
            template_data_link_configs = config['template_data_links']
            self._generate_templated_data_links(state_manager_config=state_manager_config, template_data_link_configs=template_data_link_configs['templates'],
                                                manifest_name=manifest_name, run_id=run_id)

        if 'declared_data_links' in config.keys():
            declared_data_link_configs = config['declared_data_links']
            self._generate_data_links(link_configs=declared_data_link_configs['stages'], state_manager_config=state_manager_config, manifest_name=manifest_name, run_id=run_id)

    def _generate_templated_data_links(self, state_manager_config: dict, template_data_link_configs: dict, manifest_name: str, run_id):
        for link_config in template_data_link_configs:
            self._generate_data_links(link_configs=self._generate_configs(link_config), state_manager_config=state_manager_config, manifest_name=manifest_name, run_id=run_id,
                                      pressure=True)

    def _generate_data_links(self, state_manager_config: dict, link_configs: list, manifest_name: str, run_id, pressure=True):
        for link_config in link_configs:

            # Create New State Manager
            link_state = self._generate_state_manager(state_manager_config=state_manager_config,
                                                      data_link_config=link_config,
                                                      manifest_name=manifest_name,
                                                      run_id=run_id)

            # Add the state manager to the sink and source
            link_config['source']['conf']['state_manager'] = link_state
            link_config['sink']['conf']['state_manager'] = link_state

            source = DataLinkBuilder.build_source(link_config['source'])
            if not isinstance(source, Source):
                error = f'Source {type(source)} is not a Source.'
                self._logger.error(error)
                raise HDMError(error)

            sink = DataLinkBuilder.build_sink(link_config['sink'])
            if not isinstance(sink, Sink):
                error = f'Sink {type(sink)} is not a Sink.'
                self._logger.error(error)
                raise HDMError(error)

            self._data_links.append(
                DataLink(
                    source=source,
                    sink=sink,
                    pressureless=pressure
                )
            )

    def _generate_configs(self, configuration: dict) -> list:
        batch_config = configuration['batch_definition']
        data_links_configurations = []

        template_link_identifier = batch_config['source_name']
        template_scenarios = batch_config['scenarios']

        for scenario in template_scenarios:
            source = deepcopy(configuration['source'])
            sink = deepcopy(configuration['sink'])

            for scenario_key in scenario:
                if isinstance(scenario[scenario_key], dict):
                    for data_key in scenario[scenario_key]:
                        # TODO: currently goes one level down to sub_field, what if sub_field of sub_field case in future?
                        template_sub_field = data_key
                        template_field = scenario_key
                        value = scenario[scenario_key][data_key]
                        if template_link_identifier == source['name']:
                            if template_field not in source['conf'].keys():
                                error = f'Specified template_field not found in identified source/sink {template_link_identifier}'
                                self._logger.error(error)
                                raise HDMError(error)
                            source['conf'][template_field][template_sub_field] = value

                        if template_link_identifier == sink['name']:
                            if template_field not in sink['conf'].keys():
                                error = f'Specified template_field not found in identified source/sink {template_link_identifier}'
                                self._logger.error(error)
                                raise HDMError(error)
                            sink['conf'][template_field][template_sub_field] = value
                else:
                    template_field = scenario_key
                    value = scenario[scenario_key]
                    if template_link_identifier == source['name']:
                        if template_field not in source['conf'].keys():
                            error = f'Specified template_field not found in identified source/sink {template_link_identifier}'
                            self._logger.error(error)
                            raise HDMError(error)
                        source['conf'][template_field] = value

                    if template_link_identifier == sink['name']:
                        if template_field not in sink['conf'].keys():
                            error = f'Specified template_field not found in identified source/sink {template_link_identifier}'
                            self._logger.error(error)
                            raise HDMError(error)
                        sink['conf'][template_field] = value

            data_links_configurations.append(
                dict(
                    source=source,
                    sink=sink
                )
            )

        return data_links_configurations
