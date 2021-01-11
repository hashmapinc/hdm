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
import pandas as pd
from hdm.core.sink.sink import Sink
from hdm.core.utils.generic_functions import GenericFunctions
from hdm.core.utils.project_config import ProjectConfig


class FSSink(Sink):
    """
      Filesystem Sink

      Expected protocol for configuration:
      data_frame: Dataframe to be written to filesystem
      dest_path: Path where the file will be written
      file_format: csv | parquet
      """

    def __init__(self, **kwargs):
        """
        Construct an instance of the FSSink.
        Args:
            **kwargs: Configurations required for FS Sink
                      directory: Directory path used for offloading files from dataframe. Must be present
                      file_name: Will be used to create the landing file with this name
                      file_format: File format to be written csv | parquet
        Raises:
            FileNotFoundError: If temp. directory for landing files is not created
        """
        super().__init__(**kwargs)
        self.__dest_path = os.path.abspath(kwargs.get('directory'))

        if not os.path.exists(self.__dest_path):
            raise NotADirectoryError("Unable to create Sink landing at %s" % self.__dest_path)

        # If file format not specified hard coding it as csv for now
        # TODO - More file types for later
        self.__file_format = kwargs.get('file_format', 'csv')
        # self._entity = self.__dest_path

    def produce(self, **kwargs) -> None:
        self._run(**kwargs)

    def _set_data(self, **kwargs) -> dict:
        """
        Creates a csv | parquet file from a Dataframe.
        Args:
            **kwargs: data_frame: Pandas dataframe which will be written to the filepath specified in configuration
        Raises:
            NotImplementedError: For file_format other than text/csv
        """
        df: pd.DataFrame = kwargs.get("data_frame")
        table_name: str = kwargs.get("table_name")

        # File name which will be used to create the sink file.
        # Create file name if not provided by the user
        file_name = kwargs.get("file_name", f"{ProjectConfig.file_prefix()}_{kwargs.get('current_state')['correlation_id_out']}.csv")

        self._entity = file_name
        self._entity_filter = None

        # TODO Check for valid df
        # TODO: Assumption made to create only CSV's for now
        # State management should record that as failure for other file types.
        if not self.__file_format == 'csv':
            raise ValueError("Unknown output type: %s" % self.__file_format)

        self.__write_file(file_name, df, table_name)
        return dict(record_count=df.shape[0])

    def __write_file(self, file_name, df, table_name):
        """
        saves files to sink
        Returns: none
        """
        if table_name:
            destination_directory = os.path.join(self.__dest_path, self._sink_name, GenericFunctions.table_to_folder(table_name))
        else:
            destination_directory = self.__dest_path

        if not os.path.exists(destination_directory):
            os.makedirs(destination_directory)
        self._entity_filter = destination_directory
        csv_to_create = os.path.abspath(os.path.join(destination_directory, file_name))
        self._logger.info("Writing file: %s" % csv_to_create)
        df.to_csv(csv_to_create, index=False)

    # TODO Fix this!
    def _error_handler(self, e: Exception) -> None:
        self._logger.exception("Error in FSSink")
        if isinstance(e, NotImplementedError):
            raise e
