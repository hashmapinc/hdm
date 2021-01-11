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
import time
from io import StringIO
import boto3

from hdm.core.dao.s3 import S3
from hdm.core.sink.sink import Sink
from hdm.core.utils.project_config import ProjectConfig


class S3Sink(Sink):
    """
    S3 Source
    Expected protocol for configuration:
    connection: Connection choice to connect to different profiles.
    bucket_name: Bucket name to scan for loading files into dataframes
    """
    # Prefix for the created Sink bucket
    __bucket_prefix = 'hdm-'
    # Default bucket name to be used if not specified
    __default_bucket_name = 'defbucket'
    # Default bucket name for the object be used if not specified
    # All the objects are prefixed with Epoch time

    def __init__(self, **kwargs):
        """
        Construct an instance of the S3Source.
        Args:
            **kwargs:
                connection: connection name to connect to a specific profile
                bucket_name: bucket name to used for storing the object passed in the dataframe.
                object_name: name of the created object
        """
        # Connections are lazy - they are only created when being used
        super().__init__(**kwargs)
        self.__connection_choice = kwargs.get('connection')

        # Name of the bucket where objects will be written
        # If not passed, we will use a default bucket name.
        # All our default S3 buckets for sink are prefixed with 'hdm-'
        self.__bucket_name = kwargs.get('bucket_name', self.__get_bucket_name(S3Sink.__default_bucket_name))
        # TODO: Setting the bucket name as entity for now. Need to verify
        self._entity = self.__bucket_name

    def __get_bucket_name(self, bucket_name: str) -> str:
        """
        Create and return the bucket name with the provided prefix and user provided name for the bucket
        """
        # TODO - Make sure this is a valid bucket name
        # eg: The generated bucket name must be between 3 and 63 chars long
        return ''.join([self.__bucket_prefix, bucket_name])

    def __bucket_exists(self, s3, bucket_name):
        """
        Checks if the bucket exists or not and returns True/False
        """
        # All created buckets have a creation date. If it is None, create a new
        self.__bucket = s3.Bucket(bucket_name)
        return bool(self.__bucket.creation_date)

    def __create_bucket(self, s3) -> None:
        """
        Creates a bucket if it does not exist
        """
        if not self.__bucket_exists(s3, bucket_name=self.__bucket_name):
            try:
                self.__bucket = s3.create_bucket(Bucket=self.__bucket_name)
                # Waiting till Resource available
                self.__bucket.wait_until_exists()
            except Exception as e:
                error_msg = f"Exception during bucket creation:{e}"
                self.__logger.debug("%s", error_msg)
                raise RuntimeError(error_msg)
        else:
            self.__logger.info("Bucket %s already exists.", self.__bucket_name)

    def produce(self, **kwargs):
        self._run(**kwargs)

    def _set_data(self, **kwargs) -> dict:
        """
        Creates CSV files for the passed dataframe.
        Args:
            **kwargs:
                data_frame: dataframe required to be written to S3 bucket
        """
        #  This is the dataframe that was passed
        df = kwargs.get('data_frame')

        # We want to assume for now that we are writing to CSV
        # TODO Later add support for parquet and others
        # Base name of the object
        # TODO Make these orderable and more easily iterated over (think restart or CDC)
        # If not passed, we will use a default object name. default object names are prefixed with current time in NS.
        object_name = kwargs.get('file_name', f"{ProjectConfig.file_prefix()}_{str(time.time_ns())}.csv")

        # Creating a buffer to read the CSV and write it to S3.
        # TODO May need to change to BytesIO
        buffer = StringIO()
        df.to_csv(buffer, sep=',', index=False)
        self.__logger.debug("Buffer len: %d", len(buffer.getvalue()))

        # Create a connection to S3
        s3: boto3.session.Session.resource = S3(connection=self.__connection_choice).connection

        # Make sure that the bucket exists and create it if it does not exist.
        self.__create_bucket(s3=s3)
        self._entity_filter = object_name
        # Create the object in S3
        self.__logger.debug("Creating object %s in %s", object_name, self.__bucket_name)
        s3.Object(self.__bucket_name, object_name).put(Body=buffer.getvalue())

        return dict(record_count=df.shape[0])
