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
from contextlib import contextmanager


class DAO:
    @classmethod
    def _get_logger(cls):
        return logging.getLogger(cls.__name__)

    def __init__(self, **kwargs):
        self._logger = self._get_logger()
        self._connection_name = kwargs.get('connection')
        if not self._validate_configuration():
            raise ConnectionError(f'Method not implemented for {type(self).__name__}.')

    @property
    def connection(self):
        return self._get_connection()

    def _get_connection(self):
        raise NotImplementedError(f'Method not implemented for {type(self).__name__}.')

    def _test_connection(self, connection) -> bool:
        raise NotImplementedError(f'Method not implemented for {type(self).__name__}.')

    def _validate_configuration(self) -> bool:
        raise NotImplementedError(f'Method not implemented for {type(self).__name__}.')
