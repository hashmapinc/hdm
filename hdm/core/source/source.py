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
from datetime import datetime
import logging
import traceback


class Source:

    @classmethod
    def _get_logger(cls):
        return logging.getLogger(cls.__name__)

    def __init__(self, **kwargs):
        self._logger = self._get_logger()
        self._entity: str = ""
        self._entity_filter: str = ""
        self._sink_entity = None
        self._sink_filter = None
        self._first_record_pulled = None
        self._last_record_pulled = None
        self._state_manager = kwargs['state_manager']
        self._state_manager_values = {}
        self._correlation_id_in = None
        self._processed_history_list = self._state_manager.get_processing_history()
        self._is_running = False
        self._correlation_id_out = None
        self._source_name = self._state_manager.get_source_name()
        self._sink_name = self._state_manager.get_sink_name()

    @property
    def is_running(self):
        return self._is_running

    def _pre_consume(self) -> dict:
        self._current_state = self._state_manager.insert_state(
            source_entity=self._entity,
            source_filter=self._entity_filter,
            action='sourcing pre-pull',
            status='in_progress',
            correlation_id_in=self._correlation_id_in,
            correlation_id_out=self._correlation_id_out,
            sourcing_start_time=datetime.utcnow()
        )
        return self._current_state

    # update this to handle failures
    def _post_consume(self, current_state: dict) -> None:
        self._state_manager_values = self._state_manager.update_state(
            source_entity=self._entity,
            source_filter=self._entity_filter,
            sink_entity=self._sink_entity,
            sink_filter=self._sink_filter,
            first_record_pulled=self._first_record_pulled,
            last_record_pulled=self._last_record_pulled,
            action='sourcing post-pull',
            state_id=current_state['state_id'],
            correlation_id_in=current_state['correlation_id_in'],
            correlation_id_out=current_state['correlation_id_out'],
            status=current_state['status'],
            record_count=current_state['record_count'],
            sourcing_start_time=current_state['sourcing_start_time'],
            sourcing_end_time=datetime.utcnow()
        )

    def consume(self, **kwargs) -> dict:
        raise NotImplementedError(f'Method not implemented for {type(self).__name__}.')

    def _run(self, **kwargs) -> dict:
        self._last_record_pulled = self._get_last_record()
        current_state = self._pre_consume()
        self._sink_name = current_state['sink_name']
        self._source_name = current_state['source_name']
        try:
            data_dict = self._get_data(**kwargs)
            current_state['status'] = 'success'
            try:
                current_state['record_count'] = data_dict['record_count']
            except Exception as e:
                current_state['record_count'] = 0

            self._logger.info("FETCH DATA: Table: %s; TotalRecords: %s",
                              self._entity,
                              current_state['record_count'] if current_state['record_count'] else "0"
                              )
            self._set_first_last_record(data_dict)

        except Exception:
            self._error_handler(traceback.format_exc())
            current_state['status'] = 'failure'
            current_state['record_count'] = None
        finally:
            self._post_consume(current_state)

        # TODO: We need to figure this out.
        return data_dict

    def _get_record_data(self, record) -> str:
        # TODO: Fix this code
        data_list = json.loads(record.reset_index().to_json(orient='records'))
        str_text = ""
        for key in data_list[0]:
            if key != 'index':
                str_text += key + ":" + str(data_list[0][key]) + ","
        return str_text[:-1]

    def _get_data(self, **kwargs) -> dict:
        raise NotImplementedError(f'Method not implemented for {type(self).__name__}.')

    def _get_last_record(self) -> dict:
        return self._state_manager.get_last_record(self._entity)

    def _set_first_last_record(self, data_dict):
        # TODO: discuss with John - only for db or files too. Then will have to read files.
        dict_val = ('data_frame', 'source_type')
        if all(key in data_dict for key in dict_val):
            # Get first and last records
            if data_dict['source_type'] == "database":
                try:
                    self._first_record_pulled = self._get_record_data(data_dict['data_frame'].iloc[:1])
                    self._last_record_pulled = self._get_record_data(data_dict['data_frame'].iloc[-1:])
                except Exception:
                    self._first_record_pulled = None
                    self._last_record_pulled = None

    def _error_handler(self, exception_message: str) -> None:
        error_message = f"Error occured in Source: {exception_message}"
        self._logger.exception(error_message)
