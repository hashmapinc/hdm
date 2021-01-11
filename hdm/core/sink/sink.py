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
import traceback
from datetime import datetime


class Sink:
    @classmethod
    def _get_logger(cls):
        return logging.getLogger(cls.__name__)

    def __init__(self, **kwargs):
        self._logger = self._get_logger()
        self._entity: str = ""
        self._entity_filter: str = ""
        self._state_manager = kwargs['state_manager']
        self._state_manager_values = {}
        self._sink_name = ""
        self._is_running = False

    @property
    def is_running(self):
        return self._is_running

    def _insert_and_get_current_state(self) -> dict:
        self._correlation_id_in = self._state_manager_values.get('correlation_id_in')
        self._correlation_id_out = self._state_manager_values.get('correlation_id_out')

        self._state_manager_values = self._state_manager.insert_state(
            source_entity=None,
            source_filter=None,
            sink_entity=self._entity,
            sink_filter=self._entity_filter,
            action='sinking pre-push',
            correlation_id_in=self._correlation_id_in,
            correlation_id_out=self._correlation_id_in,
            status='in_progress',
            sinking_start_time=datetime.utcnow()
        )
        return self._state_manager_values

    # update this to handle failures
    def _update_state(self, current_state: dict) -> None:
        self._state_manager_values = self._state_manager.update_state(
            source_entity=current_state['source_entity'],
            source_filter=current_state['source_filter'],
            sink_entity=self._entity,
            sink_filter=self._entity_filter,
            action='sinking post-push',
            state_id=current_state['state_id'],
            correlation_id_in=current_state['correlation_id_in'],
            correlation_id_out=current_state['correlation_id_out'],
            status=current_state['status'],
            record_count=current_state['record_count'],
            sourcing_start_time=current_state['sourcing_start_time'],
            sourcing_end_time=current_state['sourcing_end_time'],
            sinking_start_time=current_state['sinking_start_time'],
            sinking_end_time=datetime.utcnow(),
            first_record_pulled=current_state['first_record_pulled'],
            last_record_pulled=current_state['last_record_pulled']
        )

    def produce(self, **kwargs) -> None:
        raise NotImplementedError(f'Method not implemented for {type(self).__name__}.')

    def _run(self, **kwargs) -> None:
        # Set that it is running
        self._is_running = True

        # Get the current state
        file_name = kwargs.get("file_name", None)
        parent_file_name = kwargs.get("parent_file_name", None)
        source_filter = kwargs.get("source_filter", None)
        if parent_file_name:
            # for chunk source where parent file name is the source_entity.
            current_state = self._get_current_state(entity=parent_file_name, entity_filter=source_filter)
        else:
            current_state = self._get_current_state(entity=file_name, entity_filter=source_filter)

        if not current_state:
            current_state = self._insert_and_get_current_state()
        try:
            kwargs['current_state'] = current_state
            data_dict = self._set_data(**kwargs)
            current_state['status'] = 'success'
            current_state['record_count'] = data_dict['record_count']

            self._logger.info("PUT DATA: Entity: %s; TotalRecords: %s",
                              self._entity,
                              data_dict.get('record_count') if data_dict.get('record_count') else "0"
                              )

        except Exception:
            current_state['status'] = 'failure'
            current_state['record_count'] = None
            self._error_handler(traceback.format_exc())
        finally:
            self._update_state(current_state)

            # Set that it is no longer running
            self._is_running = False

    def _get_current_state(self, entity, entity_filter) -> dict:
        current_state = self._state_manager.get_current_state(entity, entity_filter)
        if not current_state:
            return current_state
        self._sink_name = current_state['sink_name']
        current_state_after_update = self._state_manager.update_state(
            source_entity=current_state['source_entity'],
            source_filter=current_state['source_filter'],
            sink_entity=self._entity,
            sink_filter=self._entity_filter,
            action='sinking pre-push',
            state_id=current_state['state_id'],
            correlation_id_in=current_state['correlation_id_in'],
            correlation_id_out=current_state['correlation_id_out'],
            status='in_progress',
            record_count=int(current_state['record_count']) if current_state['record_count'] else 0,
            sourcing_start_time=current_state['sourcing_start_time'],
            sourcing_end_time=current_state['sourcing_end_time'],
            sinking_start_time=datetime.utcnow(),
            first_record_pulled=current_state['first_record_pulled'],
            last_record_pulled=current_state['last_record_pulled']
        )
        return current_state_after_update

    def _set_data(self, **kwargs) -> dict:
        raise NotImplementedError(f'Method not implemented for {type(self).__name__}.')

    def _error_handler(self, error: str) -> None:
        self._logger.exception("An error has occurred in Sink: %s", error)
