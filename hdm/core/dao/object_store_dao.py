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

from hdm.core.dao.dao import DAO


class ObjectStoreDAO(DAO):

    @contextmanager
    def _get_connection(self):
        return super()._get_connection()

    def _test_connection(self, connection) -> bool:
        return super()._test_connection(connection)

    def _validate_configuration(self) -> bool:
        return super()._validate_configuration()
