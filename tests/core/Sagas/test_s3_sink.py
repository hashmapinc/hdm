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

import logging

from hdm.core.sink.s3_sink import S3Sink
from tests.core.Sagas.testenv_utils import TestEnvUtils

# Bucket name where CSV's will be created. Will be prefixed with 'hdm-'
S3_BUCKET_NAME = 'testbucket'


@unittest.skip
class TestS3Sink(TestCase):
    __logger = logging.getLogger(__name__)
    """
    Unit Tests for S3 Sink
    Creates a S3 bucket and writes csv files from the passed dataframe
    """

    def setUp(self) -> None:
        os.environ['HDM_ENV'] = 'dev'
        self.__df = TestEnvUtils.get_test_df()

    def test_sink_invalid_conf(self):
        conf = dict(sink=dict(conf=(dict(connection=dict()))))
        with self.assertRaises((RuntimeError, AttributeError)):
            sink = S3Sink(**conf['sink'])
            sink.produce(**conf['sink'])

    def test_sink_csv(self):
        conf1 = dict(sink=dict(conf=(dict(connection=dict(), data_frame=self.__df))))
        conf2 = dict(sink=dict(conf=(dict(connection=dict(), bucket_name=S3_BUCKET_NAME, data_frame=self.__df))))
        conf3 = dict(sink=dict(conf=(dict(connection=dict(), file_name="test1.csv", data_frame=self.__df))))
        conf4 = dict(
            sink=dict(conf=(
                dict(connection=dict(), bucket_name=S3_BUCKET_NAME, file_name="test2.csv", data_frame=self.__df))))
        for conf in [conf1, conf2, conf3, conf4]:
            sink = S3Sink(**conf['sink']['conf'])
            try:
                sink.produce(**conf['sink']['conf'])
            except Exception as e:
                self.__logger.error(f"Encountered exception: {e}")
