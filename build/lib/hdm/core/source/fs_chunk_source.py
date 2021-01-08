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
from distutils.util import strtobool
from itertools import chain
import pandas as pd

from hdm.core.utils.generic_functions import GenericFunctions
from hdm.core.utils.project_config import ProjectConfig
from hdm.core.source.source import Source
from hdm.core.chunk.csv_chunk import CSVChunk


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
        self.__source_path = kwargs.get('directory')
        self.__overwrite = kwargs.get('overwrite', strtobool('false'))
        if not (self.__source_path and os.path.exists(self.__source_path)):
            raise NotADirectoryError("Directory %s not present" % self.__source_path)

        self.__source_path = os.path.abspath(self.__source_path)
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
        # TODO: technical debt- currently it gets all files in the folder and check if already processed
        #  (using state management)
        for root, dirs, files in os.walk(self.__source_path):
            for file in files:
                if file not in chain(*self._processed_history_list) or self.__overwrite:
                    self._logger.debug("Yielding file: %s", file)
                    to_process = str(os.path.join(root, file))
                    self._logger.info("Processing %s...", to_process)

                    if not bool(os.path.isfile(to_process)):
                        continue
                    if ProjectConfig.archive_folder() in to_process:
                        continue

                    # Loop through each chunk file and generate dataframes for fs_sink to consume
                    for chunked_file in self.__chunk_file(file, root):
                        self._entity = chunked_file
                        if self.__chunk:
                            self._entity_filter = [{'chunk': f'{self.__chunk}'}]

                        kwargs['file'] = chunked_file
                        kwargs['path'] = root
                        kwargs['table_name'] = GenericFunctions.folder_to_table(root.split(self.__source_path)[1][1:])

                        try:
                            self._correlation_id_in = file.split(f'{ProjectConfig.file_prefix()}_', 1)[1][0:-4]
                            self._correlation_id_out = chunked_file.split(f'{ProjectConfig.file_prefix()}_', 1)[1][0:-4]
                        except Exception:
                            self._correlation_id_in = None
                            self._correlation_id_out = None
                        yield self._run(**kwargs)
                else:
                    self._logger.info("Skipping processed file: %s", file)
                    yield dict(file_name=file, skipped=True)

    def _get_data(self, **kwargs) -> dict:
        file = kwargs['file']
        path = kwargs['path']
        table_name = kwargs['table_name']
        df = pd.read_csv(str(os.path.join(path, file)))
        return dict(data_frame=df,
                    file_name=file,
                    record_count=df.shape[0],
                    table_name=table_name)

    def __chunk_file(self, file_name, root) -> list:
        """
        call chunk_file method for the file type
        Return list
        Args:
            file_name: file name to be chunked
        """
        chunk_file_lst = []
        if self.__chunk and self.__file_format.lower() == 'csv':
            file_chunk = CSVChunk(file_name=file_name,
                                  dest_path=root,
                                  chunk=self.__chunk,
                                  file_prefix=ProjectConfig.file_prefix())
            chunk_file_lst = file_chunk.chunk_file()
        return chunk_file_lst
