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
from contextlib import contextmanager
from typing import Any

from hdm.core.dao.dao import DAO
from hdm.core.utils.project_config import ProjectConfig


class DBDAO(DAO):

    def __init__(self, **kwargs):
        self._engine = None
        # Connection timeout management
        self._max_attempts = ProjectConfig.connection_max_attempts()
        self._timeout = ProjectConfig.connection_timeout()
        self._timeout_factor = self._timeout
        super().__init__(**kwargs)

    @property
    def engine(self) -> Any:
        if not self._engine:
            self._engine = self._create_engine()
        return self._engine

    def _get_connection(self):
        return super()._get_connection()

    def _test_connection(self, connection) -> bool:
        return super()._test_connection(connection)

    def _create_engine(self):
        raise NotImplementedError(f'Method not implemented for {type(self).__name__}.')

    def _validate_configuration(self) -> bool:
        raise NotImplementedError(f'Method not implemented for {type(self).__name__}.')