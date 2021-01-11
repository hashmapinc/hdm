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
import azure
from azure.storage.blob import BlobServiceClient
from contextlib import contextmanager
import yaml

from hdm.core.dao.object_store_dao import ObjectStoreDAO
from hdm.core.utils.project_config import ProjectConfig


class AzureBLOB(ObjectStoreDAO):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._container = None

    def _get_connection(self):
        """
        Obtain a context managed azure connection

        Returns: azure connection

        Raises:
            ConnectionError: azure connection could not be established

        """
        with open(f"{ProjectConfig.hdm_home()}/{ProjectConfig.profile_path()}", 'r') as stream:
            conn_conf = yaml.safe_load(stream)[ProjectConfig.hdm_env()][self._connection_name]

        self._container = conn_conf['container_name']
        connection = BlobServiceClient(account_url=conn_conf['url'], credential=conn_conf['sas'])
        self._test_blob_container_existence(connection)
        return connection

    def _test_connection(self, connection) -> bool:
        """
        Validate that the connection is valid to azure blob storage account
        Returns: True if connection is valid and container exists, False otherwise
        """
        result = False
        if connection:
            try:
                # Try to connect to azure blob storage account
                result = bool(connection.get_service_properties())
            except Exception:
                return False

        if not self._test_blob_container_existence(connection):
            result = False

        return result

    def _test_blob_container_existence(self, connection) -> bool:
        """
        check if blob container exists

        Returns: True if container is exists, False otherwise

        """
        if not connection:
            return False

        try:
            # Throws exception if container not available
            return bool(connection.get_container_client(self._container))

        except azure.core.exceptions.ResourceNotFoundError:
            self._create_blob_container(connection)
            return True

    def _create_blob_container(self, connection):
        """
        creates a blob container

        Returns:

        """
        connection.create_container(self._container)

    def _validate_configuration(self) -> bool:
        # TODO
        return True
