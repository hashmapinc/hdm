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
import io
import os
import time
import pandas as pd
from azure.storage.blob import BlobClient

from hdm.core.utils.generic_functions import GenericFunctions
from hdm.core.utils.project_config import ProjectConfig
from hdm.core.dao.azure_blob import AzureBLOB
from hdm.core.sink.sink import Sink


class AzureBlobSink(Sink):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.__container_name = kwargs['container']
        self.__env = kwargs['env']

        self.__azure_dao = AzureBLOB(connection=self.__env, container=self.__container_name)
        self.__container = None

    def produce(self, **kwargs) -> None:
        self._run(**kwargs)

    def _set_data(self, **kwargs) -> dict:
        df = kwargs['data_frame']
        table_name: str = kwargs.get("table_name")

        self._entity = kwargs.get('file_name', f"{ProjectConfig.file_prefix()}_{str(time.time_ns())}.csv")
        self._entity_filter = os.path.join(self._sink_name, GenericFunctions.table_to_folder(table_name))
        with self.__azure_dao.connection as client:
            self.__container = client.get_container_client(self.__container_name)
            blob_client = self.__container.get_blob_client(os.path.join(self._sink_name, GenericFunctions.table_to_folder(table_name), self._entity))
            self.__putFile(df, blob_client)
        return dict(record_count=df.shape[0])

    def __putFile(self, df: pd.DataFrame, blob_client: BlobClient) -> dict:
        self._logger.info("Putting file: %s", blob_client.blob_name)
        with io.StringIO() as buf:
            df.to_csv(buf, index=False)
            buf.seek(0)
            # upload_blob can only take bytesio or file io, not stringio
            with io.BytesIO(buf.read().encode('utf8')) as byte_buf:
                blob_client.upload_blob(byte_buf, overwrite=True)
