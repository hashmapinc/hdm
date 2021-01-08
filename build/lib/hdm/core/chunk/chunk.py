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
import os.path
import shutil
import logging
from hdm.core.utils.project_config import ProjectConfig


class Chunk:
    # Application Logger
    _logger = logging.getLogger(__name__)

    def __init__(self, **kwargs):
        self._file_name = kwargs.get('file_name')
        self._dest_path = kwargs.get('dest_path')
        self._file_prefix = kwargs.get('file_prefix')
        self._chunk = kwargs.get('chunk')
        self._chunked_file_list = []

    def chunk_file(self) -> list:
        """
        chunk file. If chunking successful then archive the file.
        Return None
        Args:
        """
        if self._chunk_and_save_files():
            self._archive_file()

        return self._chunked_file_list

    def _chunk_and_save_files(self) -> bool:
        raise NotImplementedError(f'Method not implemented for {type(self).__name__}.')

    def _archive_file(self) -> None:
        """
        moves the file to archive folder after processing
        Return None
        Args:
        """
        try:
            to_process = os.path.abspath(os.path.join(self._dest_path, self._file_name))
            target_directory = os.path.join(self._dest_path, ProjectConfig.archive_folder())
            if not os.path.exists(target_directory):
                os.makedirs(target_directory)
            shutil.move(to_process, target_directory)
        except Exception as e:
            self._logger.error("Got exception: %s while archiving %s", e, to_process)
