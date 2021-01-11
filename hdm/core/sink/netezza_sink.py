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
from sqlalchemy import create_engine

from hdm.core.sink.sink import Sink


class NetezzaSink(Sink):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.__user = kwargs['user']
        self.__password = kwargs['password']
        self.__host = kwargs['host']
        self.__database = kwargs['database']
        self.__table = kwargs['table']
        self.__query = None
        self._entity = f"{self.__database}.{self.__table}"

    def produce(self, **kwargs) -> None:
        self._run(**kwargs)

    def _set_data(self, **kwargs) -> dict:
        kwargs.get('data_frame').to_sql(f"{self.__table}",
                                        create_engine(
                                            f'mysql+pymysql://{self.__user}:{self.__password}@{self.__host}/'
                                            f'{self.__database}',
                                            pool_recycle=3600),
                                        if_exists='append')
        return dict(record_count=kwargs.get('data_frame').shape[0])
