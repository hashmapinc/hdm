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
import boto3
import yaml

from hdm.core.dao.object_store_dao import ObjectStoreDAO
from hdm.core.utils.project_config import ProjectConfig


class S3(ObjectStoreDAO):
    """
    S3 Resource
    Expected environment variables:
    HOME: User home having the file specified in PROFILE_FILE
    HDM_ENV: environment for dev/stage/testing.Should match the one in PROFILE_FILE
    PROFILE_FILE: YAML file having configurations based on environments
    """

    def _get_connection(self, **kwargs):
        """
        Gets a S3 Session based on env variables.
        Connection is made using credentials specified in .aws/ or as per the profile values specified in:
            aws_access_key_id
            aws_secret_access_key
            region_name
        """
        with open(f"{ProjectConfig.hdm_home()}/{ProjectConfig.profile_path()}", 'r') as stream:
            conn_conf = yaml.safe_load(stream)[ProjectConfig.hdm_env()][kwargs.get('connection')]

        # TODO - Validation needed for all the correct keys present or not
        if 'profile' in conn_conf.keys():
            connection = boto3.session.Session(profile_name=conn_conf.get('profile'))
        else:
            connection = boto3.session.Session(aws_access_key_id=conn_conf.get('aws_access_key_id'),
                                               aws_secret_access_key=conn_conf.get('aws_secret_access_key'),
                                               region_name=conn_conf.get('region_name'))

        return connection

    def _test_connection(self, connection) -> bool:
        # TODO
        result = False
        return result

    def _validate_configuration(self) -> bool:
        # TODO
        return True
