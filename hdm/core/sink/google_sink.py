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
import os
import time
import traceback
from pathlib import Path

from google.cloud.storage import Blob

from google.cloud import storage

from hdm.core.sink.sink import Sink


class GoogleSink(Sink):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._client_connect()
        self.__bucket = self._create_bucket(**kwargs)
        self.__blob_name = kwargs['blob_name']
        self.__file_format = kwargs['file_format']

    def _client_connect(self):
        if os.path.isdir(os.path.join(Path.home(), ".hdm")):
            self.client = storage.Client.from_service_account_json(f"{Path.home()}/.hdm/gcp_service.json")
        else:
            self.client = storage.Client()

    def produce(self, **kwargs) -> None:
        self._run(**kwargs)

    def _set_data(self, **kwargs) -> dict:
        df = kwargs.get('data_frame')
        ts = time.time_ns()
        blob_name_parts = os.path.splitext(self.__blob_name)
        blob_name = blob_name_parts[0] + '_' + str(ts) + blob_name_parts[1]
        blob = Blob(blob_name, self.__bucket)
        blob.upload_from_string(df.to_csv(), self.__file_format)
        return dict(record_count=df.shape[0])

    def _create_bucket(self, **kwargs):

        bucket = kwargs['bucket']
        try:
            self.client.create_bucket(bucket)
        except Exception:
            print(traceback.format_exc())

        return self.client.get_bucket(kwargs['bucket'])
