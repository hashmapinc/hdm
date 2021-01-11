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
import unittest
from unittest import TestCase

import botocore.errorfactory
import pandas as pd
import logging

# Name of testing directory created for ingesting files
from hdm.core.source.s3_source import S3Source

# Bucket name for test cases
S3_BUCKET_NAME = 'knerrir-testbucket'


@unittest.skip
class TestS3(TestCase):
    __logger = logging.getLogger(__name__)
    """
    Unit Tests for S3 Source
    Reads from a S3 bucket with some sample data to pandas dataframe
    """

    def setUp(self) -> None:
        os.environ['HDM_ENV'] = 'dev'

    def test_source_invalid_conf(self):
        conf1 = dict(source=dict(conf=(dict(connection=dict()))))
        conf2 = dict(source=dict(conf=(dict(connection=dict(), bucket_name=""))))
        conf3 = dict(source=dict(conf=(dict(connection=dict(), bucket_name=f'{S3_BUCKET_NAME}_NA'))))
        for conf in [conf1, conf2, conf3]:
            with self.assertRaises((RuntimeError, AttributeError)):
                source = S3Source(**conf['source']['conf'])
                for df in source.consume(**conf['source']['conf']):
                    continue

    def test_source_valid_conf(self):
        conf = dict(source=dict(conf=(dict(connection=dict(), bucket_name=S3_BUCKET_NAME))))
        try:
            source = S3Source(**conf['source']['conf'])
            for df in source.consume(**conf['source']['conf']):
                self.assertTrue(isinstance(df['data_frame'], pd.DataFrame))
        except botocore.errorfactory.ClientError as e:
            self.__logger.error(f"Error occurred: {e}")
