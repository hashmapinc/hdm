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


class DummySource(Source):
    """
      dummy Source

      used in configurations where we don't need a source.
      This is used for a tailless sink when using a Snowflake Copy Sink
      """

    # create external stage, so we don't have to pass credentials to the copy sink
    def consume(self, **kwargs) -> dict:
        self._entity = None
        self._entity_filter = None
        yield self._run(**kwargs)

    def _get_data(self, **kwargs) -> dict:
        return dict(data_frame=None,
                    file_name=None)
