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
from io import BytesIO
import boto3
import pandas as pd

from hdm.core.dao.s3 import S3
from hdm.core.source.source import Source


class S3Source(Source):
    """
    S3 Source

    Expected protocol for configuration:
    connection: Connection choice to connect to different profiles.
    bucket_name: Bucket name to scan for loading files into dataframes
    """

    def __init__(self, **kwargs):
        """
        Construct an instance of the S3Source.
        Args:
            **kwargs:
                connection: connection name to connect to a specific profile
                bucket_name: bucket name used for scanning various files to be loaded to dataframes.
        Raises:
            RuntimeError: If bucket_name is null
        """
        # Connections are lazy - they are only created when being used
        super().__init__(**kwargs)
        self.__connection_choice = kwargs.get('connection')

        # Name of the bucket where objects will be written
        self.__bucket_name = kwargs.get('bucket_name')
        if not self.__bucket_name:
            raise RuntimeError("Null Bucket name passed.")
        self._entity = self.__bucket_name

    def __bucket_exists(self, s3: boto3.session.Session.resource, bucket_name: str) -> bool:
        """
        Checks if a bucket exists or not
        Args:
            s3: S3 resource.
            bucket_name: bucket name to create a S3 bucket object.
        """
        # All created buckets have a creation date. If it is None, create a new
        self.__bucket = s3.Bucket(bucket_name)
        return bool(self.__bucket.creation_date)

    def __get_bucket(self, s3: boto3.session.Session.resource):
        """
        Returns a bucket object if it exists else None
        Args:
            s3: S3 resource.
        Raises:
            RuntimeError if the bucket does not exist or exception during retrieval
        """
        # TODO: If it doesn't exist, create it
        # TODO: Our S3 Sink buckets are created with 'hdm-' as a prefix
        # Should we follow the same protocol in S3 Source too ??
        if not self.__bucket_exists(s3, bucket_name=self.__bucket_name):
            msg = f"Bucket {self.__bucket_name} does not exist."
            self._logger.error(msg)
            raise RuntimeError(msg)

        try:
            return s3.Bucket(self.__bucket_name)
        except Exception as e:
            error_msg = f"Exception during bucket retrieval:{e}"
            self._logger.debug("%s", error_msg)
            raise RuntimeError(error_msg)

    def consume(self, **kwargs) -> dict:
        """
        Yields a dataframe for each object from a S3 bucket
        Args:
            **kwargs
        """
        # Create a connection to S3
        s3: boto3.session.Session.resource = S3(connection=self.__connection_choice).connection

        # Make sure that the bucket exists and create it if it does not exist.
        bucket = self.__get_bucket(s3)

        # TODO - This is iterating over all object. We may want to filter the objects in this call. See filter
        #  documentation TODO - here (https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3
        #   .html#S3.Bucket.objects) TODO - for bucket.objects.filter. The idea would be to filter from Marker and
        #    also to pass a prefix for the table. TODO - for now reading only csv's to keep it simple
        for obj in bucket.objects.all():
            if str(obj.key).rsplit('.')[-1] == 'csv':
                self._entity_filter = obj.key
                kwargs['obj'] = obj
                yield self._run(**kwargs)

    def _get_data(self, **kwargs) -> dict:
        obj = kwargs['obj']
        df = pd.read_csv(BytesIO(obj.get()['Body'].read()), encoding='utf8')
        return {'data_frame': df,
                'file_name': obj.key,
                'record_count': df.shape[0]}