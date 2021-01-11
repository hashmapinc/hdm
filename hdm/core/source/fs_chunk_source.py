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
import shutil
import uuid
from distutils.util import strtobool
from itertools import chain
import pandas as pd

from hdm.core.utils.generic_functions import GenericFunctions
from hdm.core.utils.project_config import ProjectConfig
from hdm.core.source.source import Source


class FSChunkSource(Source):
    """
     Filesystem Source

     Expected protocol for configuration:
     directory: Source directory containing files to be processed.
     """

    def __init__(self, **kwargs):
        """
        Construct an instance of the FSSource.
        Performs checks to verify if the source directory exists or not.
        Args:
            **kwargs: must have a source_dir
        Raises:
            NotADirectoryError: If directory specified in directory does not exist
        """
        super().__init__(**kwargs)
        # Check if location for files to process exists
        self.__source_base_path = kwargs.get('directory')
        if not (self.__source_base_path and os.path.exists(self.__source_base_path)):
            raise NotADirectoryError("Directory %s not present" % self.__source_base_path)

        self.__source_path = os.path.abspath(os.path.join(self.__source_base_path, self._source_name))
        self._entity = self.__source_path
        self.__file_format = kwargs.get('file_format', 'csv')
        self.__chunk = kwargs.get('chunk', None)

    def consume(self, **kwargs) -> dict:
        """
        Iterates over the source directory and processes files.
        Args:
            **kwargs
        """
        # TODO: Currently assuming directory only has CSV files
        # TODO: Add support for other files types later
        for root, dirs, files in os.walk(self.__source_path):
            for file in set(files).difference(set(chain(*self._processed_history_list))):
                self._logger.debug("Yielding file: %s", file)
                to_process = str(os.path.join(root, file))
                self._logger.info("Processing %s...", to_process)

                if not bool(os.path.isfile(to_process)):
                    continue

                # Get table name from file path
                table_name = GenericFunctions.folder_to_table(root.split(self.__source_path)[1][1:])

                # Loop through each chunk file and generate dataframes for fs_sink to consume
                cnt = 0
                for chunked_file in self.__chunk_file(file, root, self._sink_name, table_name):
                    cnt += 1
                    self._entity = file
                    if self.__chunk:
                        self._entity_filter = [{'chunk': f'{self.__chunk}', 'seq': f'{cnt}'}]

                    kwargs['file'] = chunked_file
                    kwargs['source_file'] = self._entity
                    kwargs['source_filter'] = self._entity_filter
                    kwargs['table_name'] = table_name
                    kwargs['path'] = os.path.join(self.__source_base_path, self._sink_name, GenericFunctions.table_to_folder(table_name))

                    try:
                        self._correlation_id_in = file.split(f'{ProjectConfig.file_prefix()}_', 1)[1][0:-4]
                        self._correlation_id_out = chunked_file.split(f'{ProjectConfig.file_prefix()}_', 1)[1][0:-4]
                    except Exception:
                        self._correlation_id_in = None
                        self._correlation_id_out = None

                    yield self._run(**kwargs)

    def _get_data(self, **kwargs) -> dict:
        file = kwargs['file']
        source_file = kwargs['source_file']
        source_filter = kwargs['source_filter']
        path = kwargs['path']
        table_name = kwargs['table_name']

        df = self.__process_file(str(os.path.join(path, file)))

        return dict(data_frame=df,
                    file_name=file,
                    parent_file_name=source_file,
                    record_count=df.shape[0],
                    table_name=table_name,
                    source_filter=source_filter)

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

    def __chunk_file(self, file_name, root, sink_name, table_name) -> list:
        """
        chunk csv file type
        Return list
        Args:
            file_name: file name to be chunked
        """
        chunk_file_lst = []
        try:
            to_process = os.path.abspath(os.path.join(root, file_name))

            with open(to_process, 'r') as file:
                csv_file = file.readlines()
            header = csv_file[0]
            csv_file.pop(0)
            idx = 0

            target_directory = os.path.join(self.__source_base_path, sink_name, GenericFunctions.table_to_folder(table_name))
            if not os.path.exists(target_directory):
                # print('Folder not found. Creating one')
                os.makedirs(target_directory)

            if len(csv_file) > self.__chunk:
                for j in range(len(csv_file)):
                    if j % self.__chunk == 0:
                        write_file = csv_file[j:j + self.__chunk]
                        write_file.insert(0, header)
                        try:
                            chunked_file_name = f"{ProjectConfig.file_prefix()}_{uuid.uuid4().hex}.csv"
                            with open(os.path.abspath(os.path.join(target_directory,
                                                                   chunked_file_name)), 'w+') as file:
                                file.writelines(write_file)
                            idx += 1

                            chunk_file_lst.append(chunked_file_name)
                        except Exception as e:
                            self._logger.error("Got exception: %s while creating %s", e, chunked_file_name)
            else:
                # still copy the file to target_folder, as next stage will pick file from sink_name folder
                _ = shutil.copy(to_process, target_directory)

            return chunk_file_lst

        except Exception as e:
            self._logger.error("Got exception: %s in chunking %s", e, to_process)
