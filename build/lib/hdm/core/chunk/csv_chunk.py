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
import uuid

from hdm.core.chunk.chunk import Chunk
from hdm.core.utils.project_config import ProjectConfig


class CSVChunk(Chunk):

    def _chunk_and_save_files(self) -> bool:
        """
        Return None
        Args:
        """
        try:
            to_process = os.path.abspath(os.path.join(self._dest_path, self._file_name))

            with open(to_process, 'r') as file:
                csv_file = file.readlines()
            header = csv_file[0]
            csv_file.pop(0)
            idx = 0
            processing_status = False

            if len(csv_file) > self._chunk:
                for j in range(len(csv_file)):
                    if j % self._chunk == 0:
                        write_file = csv_file[j:j + self._chunk]
                        write_file.insert(0, header)
                        try:
                            chunked_file_name = f"{self._file_prefix}_{uuid.uuid4().hex}.csv"
                            with open(os.path.abspath(os.path.join(self._dest_path,
                                                                   chunked_file_name)), 'w+') as file:
                                file.writelines(write_file)
                            idx += 1

                            self._chunked_file_list.append(chunked_file_name)
                            processing_status = True
                        except Exception as e:
                            # set to not move the large file to archive folder
                            processing_status = False
                            self._logger.error("Got exception: %s while creating %s", e, chunked_file_name)

            return processing_status

        except Exception as e:
            self._logger.error("Got exception: %s in chunking %s", e, to_process)
