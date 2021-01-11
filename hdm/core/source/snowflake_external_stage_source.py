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
from hdm.core.source.source import Source
from hdm.core.dao.snowflake import Snowflake


# TODO : Remove this. not needed anymore
class SnowflakeExternalStageSource(Source):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.__env = kwargs['env']
        self.__dao = Snowflake(connection=self.__env)
        self.__stage_name = kwargs['stage_name']

        self.__source_type = kwargs['source_type']
        self.__source_env = kwargs['source_env']
        self.__source_directory = kwargs['source_directory']

        self.__external_stage_params = ""

    def consume(self, **kwargs) -> dict:
        yield self._run(**kwargs)

    def _get_data(self, **kwargs) -> dict:
        pass
