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
        self.__file_format = kwargs.get('file_format', 'csv')

    def consume(self, **kwargs) -> dict:
        with self.__azure_dao.connection as client:
            self.__container = client.get_container_client(self.__container_name)
            for blob_prop in self.__container.list_blobs():
                self._entity = blob_prop.name.split(self._source_name + "/")[1].split("/")[1]
                self._entity_filter = None

                kwargs['file'] = self._entity
                kwargs['path'] = blob_prop
                kwargs['table_name'] = GenericFunctions.folder_to_table(blob_prop.name.split(self._source_name + "/")[1].split("/")[0])
                try:
                    self._correlation_id_in = blob_prop.name.split(self._source_name + "/")[1].split("/")[1].split(f'{ProjectConfig.file_prefix()}_', 1)[1][0:-4]
                except Exception:
                    self._correlation_id_in = None

                yield self._run(**kwargs)

    def _get_data(self, **kwargs) -> dict:
        file = kwargs['file']
        path = kwargs['path']
        table_name = kwargs['table_name']
        blob_client = self.__container.get_blob_client(path)
        file_url = blob_client.url
        df = self.__process_file(file_url)

        return {'data_frame': df,
                'file_name': file,
                'record_count': df.shape[0],
                'table_name': table_name}

    def __process_file(self, filename: str) -> pd.DataFrame:
        """
        Return Dataframe for a file.
        Args:
            filename: Filename to be loaded to a dataframe
        """
        df = None
        if self.__file_format.lower() == 'csv':
            df = pd.read_csv(filename)
        return df
