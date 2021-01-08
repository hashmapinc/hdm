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
import pandas as pd

from hdm.core.dao.azure_blob import AzureBLOB
from hdm.core.source.source import Source
from hdm.core.utils.generic_functions import GenericFunctions
from hdm.core.utils.project_config import ProjectConfig


class AzureBlobSource(Source):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.__container_name = kwargs['container']
        self.__env = kwargs['env']

        self.__azure_dao = AzureBLOB(connection=self.__env)
        self.__container = None

    def consume(self, **kwargs) -> dict:
        with self.__azure_dao.connection as client:
            self.__container = client.get_container_client(self.__container_name)
            for blob_prop in self.__container.list_blobs():
                kwargs['blob_prop'] = blob_prop
                try:
                    self._correlation_id_in = blob_prop.name.split("/")[1].split(f'{ProjectConfig.file_prefix()}_', 1)[1][0:-4]
                except Exception:
                    self._correlation_id_in = None
                yield self._run(**kwargs)

    def _get_data(self, **kwargs) -> dict:
        blob_prop = kwargs['blob_prop']
        return self.__get_file(blob_prop)

    def __get_file(self, blob_prop) -> dict:
        blob_client = self.__container.get_blob_client(blob_prop)
        file_url = blob_client.url
        df = pd.read_csv(file_url)
        self._entity = blob_prop.name.split("/")[1]
        self._entity_filter = blob_prop.name.split("/")[0]
        return {'data_frame': df,
                'file_name': blob_prop.name.split("/")[1],
                'record_count': df.shape[0],
                'table_name': GenericFunctions.folder_to_table(blob_prop.name.split("/")[0])}
