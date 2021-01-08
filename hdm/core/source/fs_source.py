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


class FSSource(Source):
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
        # self._entity = self.__source_path
        self.__file_format = kwargs.get('file_format', 'csv')

    def consume(self, **kwargs) -> dict:
        """
        Iterates over the source directory and processes files.
        Args:
            **kwargs
        """
        # TODO: Currently assuming directory only has CSV files
        # TODO: Add support for other files types later
        for root, dirs, files in os.walk(self.__source_path):
            for file in files:
                # TODO: technical debt- get all files in the folder and check if already processed
                #  (using state management)
                if file not in chain(*self._processed_history_list) \
                        or self.__overwrite:
                    self._logger.debug("Yielding file: %s", file)
                    to_process = str(os.path.join(root, file))
                    self._logger.info("Processing %s...", to_process)

                    if not bool(os.path.isfile(to_process)):
                        continue
                    if ProjectConfig.archive_folder() in to_process:
                        continue

                    kwargs['file'] = file
                    kwargs['path'] = root
                    kwargs['table_name'] = GenericFunctions.folder_to_table(root.split(self.__source_path)[1][1:])

                    self._entity = file
                    self._entity_filter = None
                    try:
                        self._correlation_id_in = file.split(f'{ProjectConfig.file_prefix()}_', 1)[1][0:-4]
                    except Exception:
                        self._correlation_id_in = None
                    yield self._run(**kwargs)
                else:
                    self._logger.info("Skipping processed file: %s", file)
                    yield dict(file_name=file, skipped=True)

    def _get_data(self, **kwargs) -> dict:
        file = kwargs['file']
        path = kwargs['path']
        table_name = kwargs['table_name']
        df = self.__process_file(str(os.path.join(path, file)))
        return dict(data_frame=df,
                    file_name=file,
                    record_count=df.shape[0],
                    table_name=table_name)

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
