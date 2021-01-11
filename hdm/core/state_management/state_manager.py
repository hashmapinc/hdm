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
import json
import uuid
from datetime import datetime
import git
import pandas as pd
import logging
from sqlalchemy import schema

from providah.factories.package_factory import PackageFactory as pf

from hdm.core.utils.project_config import ProjectConfig


class SourceSinkDescriptor:
    type: str
    name: str


class StateManager:

    @classmethod
    def _get_logger(cls):
        return logging.getLogger(cls.__name__)

    # ----------------------------------------------------------- #
    # ------------------------ Constructor ---------------------- #
    # ----------------------------------------------------------- #
    def __init__(self, **kwargs):

        self._logger = self._get_logger()
        self._format_date = kwargs.get('format_date')
        self._source: SourceSinkDescriptor = SourceSinkDescriptor()
        self._sink: SourceSinkDescriptor = SourceSinkDescriptor()
        self._job_id = None
        self._run_id = None
        self._manifest_name = None
        self._ddl_file = kwargs.get('ddl_file')

        self._env = kwargs.get('connection', 'state_manager')
        self._conn = pf.create(key=kwargs.get('dao'), configuration={'connection': kwargs.get('connection',
                                                                                              'state_manager')})
        # TODO: Pass Table name as args
        metadata = schema.MetaData(bind=self._conn.engine)
        metadata.reflect()
        if ProjectConfig.state_manager_table_name() not in metadata.tables.keys():
            with open(self._ddl_file, 'r') as stream:
                ddl = stream.read()

            with self._conn.connection as conn:
                conn.execute(ddl)

            metadata = schema.MetaData(bind=self._conn.engine)
            metadata.reflect()

        self._table: schema.Table = metadata.tables[ProjectConfig.state_manager_table_name()]

    # ----------------------------------------------------------- #
    # ------------------------ Properties ----------------------- #
    # ----------------------------------------------------------- #
    # Setter only source
    def source(self, value: dict):
        self._source.type = value['type']
        self._source.name = value['name']

    source = property(None, source)

    # Setter only sink
    def sink(self, value: dict):
        self._sink.type = value['type']
        self._sink.name = value['name']

    sink = property(None, sink)

    # Setter only job_id
    def job_id(self, value: str):
        self._job_id = value

    job_id = property(None, job_id)

    # Setter only manifest_name
    def manifest_name(self, value: str):
        self._manifest_name = value

    manifest_name = property(None, manifest_name)

    # Setter only run_id
    def run_id(self, value: str):
        self._run_id = value

    run_id = property(None, run_id)

    # ----------------------------------------------------------- #
    # --------------------- Public Methods ---------------------- #
    # ----------------------------------------------------------- #
    def insert_state(self, source_entity: str, source_filter: str, action: str,
                     status: str = None, correlation_id_in: str = None, correlation_id_out: str = None,
                     sink_entity: str = None, sink_filter: str = None,
                     sourcing_start_time=None, sourcing_end_time=None, sinking_start_time=None, sinking_end_time=None,
                     first_record_pulled=None, last_record_pulled=None) -> dict:
        """
        Insert the state of a pipeline into a control table for use along the pipeline, for auditability and to
        enable pull based CDC

                Args:
                    source_entity: FS location, database.schema.table, etc...
                    source_filter: filename, object name, filter_control_parameters (watermark start, limit, ...)
                    sink_entity:
                    sink_filter:
                    sourcing_start_time: when sourcing started
                    sourcing_end_time: when sourcing ended
                    sinking_start_time: when sinking started
                    sinking_end_time: when sinking ended
                    first_record_pulled:
                    last_record_pulled:
                    action: sourcing pre-pull | sourcing post-pull | sinking pre-push | sinking post-push
                    status: success | failure
                    correlation_id_in: UUID tracking each packet of data as it passes through the system
                    correlation_id_out: UUID tracking each packet of data as it passes through the system

                Returns: dict with job_id and correlation_id

        """
        self._logger.debug("insert_state: Source:%s; Entity: %s; Filter: %s",
                           self._source.name, source_entity, source_filter)
        state_id = self.generate_id()
        if not correlation_id_out:
            correlation_id_out = self.generate_id()

        state = self._build_state(action=action,
                                  source_entity=source_entity,
                                  status=status,
                                  source_filter=source_filter,
                                  state_id=state_id,
                                  correlation_id_in=correlation_id_in,
                                  correlation_id_out=correlation_id_out,
                                  sink_entity=sink_entity,
                                  sink_filter=sink_filter,
                                  sourcing_start_time=sourcing_start_time,
                                  sourcing_end_time=sourcing_end_time,
                                  sinking_start_time=sinking_start_time,
                                  sinking_end_time=sinking_end_time,
                                  first_record_pulled=first_record_pulled,
                                  last_record_pulled=last_record_pulled)

        query = self._table.insert().values(state_id=state_id)
        return self.__run_query(query, state)

    def update_state(self, source_entity: str, source_filter: str, action: str, state_id: str,
                     status: str = None, correlation_id_in: str = None, correlation_id_out: str = None,
                     sink_entity: str = None, sink_filter: str = None,
                     sourcing_start_time=None, sourcing_end_time=None, sinking_start_time=None, sinking_end_time=None,
                     record_count: int = None, first_record_pulled=None, last_record_pulled=None) -> dict:
        self._logger.debug("update_state: Source:%s; Entity: %s; Filter: %s", self._source.name,
                           source_entity, source_filter)
        state = self._build_state(action=action,
                                  state_id=state_id,
                                  source_entity=source_entity,
                                  source_filter=source_filter,
                                  status=status,
                                  correlation_id_in=correlation_id_in,
                                  correlation_id_out=correlation_id_out,
                                  record_count=record_count,
                                  sink_entity=sink_entity,
                                  sink_filter=sink_filter,
                                  sourcing_start_time=sourcing_start_time,
                                  sourcing_end_time=sourcing_end_time,
                                  sinking_start_time=sinking_start_time,
                                  sinking_end_time=sinking_end_time,
                                  first_record_pulled=first_record_pulled,
                                  last_record_pulled=last_record_pulled
                                  )
        query = self._table.update().where(getattr(self._table.c, 'state_id') == state_id)

        return self.__run_query(query, state)

    def get_current_state(self, entity: str = None, entity_filter: str = None) -> dict:
        with self._conn.connection as conn:
            query = f"SELECT * FROM {self._table.name} WHERE job_id='{self._job_id}'"
            if entity:
                query += f" and source_entity='{entity}'"
                if entity_filter:
                    query += f" and source_filter='{json.dumps(entity_filter)}'"
            df = pd.read_sql(sql=query, con=conn)

            # TODO: discuss with John
            if df.empty:
                return None

        return {
            'state_id': df['state_id'][0],
            'job_id': self._job_id,
            'correlation_id_in': df['correlation_id_in'][0],
            'correlation_id_out': df['correlation_id_out'][0],
            'source_entity': df['source_entity'][0],
            'source_filter': df['source_filter'][0],
            'sourcing_start_time': df['sourcing_start_time'][0],
            'sourcing_end_time': df['sourcing_end_time'][0],
            'sinking_start_time': df['sinking_start_time'][0],
            'sinking_end_time': df['sinking_end_time'][0],
            'first_record_pulled': df['first_record_pulled'][0],
            'last_record_pulled': df['last_record_pulled'][0],
            'record_count': df['row_count'][0],
            'sink_name': self._sink.name,
            'source_name': self._source.name,
            'manifest_name': self._manifest_name,
            'run_id': self._run_id
        }

    def get_last_record(self, entity: str = None) -> dict:
        with self._conn.connection as conn:
            query = f"SELECT * FROM {self._table.name} WHERE manifest_name='{self._manifest_name}' " \
                    f"and source_entity='{entity}' ORDER BY updated_on desc"
            df = pd.read_sql(sql=query, con=conn)
            if df.empty:
                return None

        return df['last_record_pulled'][0]

    def get_sink_name(self) -> str:
        return self._sink.name

    def get_source_name(self) -> str:
        return self._source.name

    def get_processing_history(self) -> list:
        self._logger.debug("get_processing_history: %s", self._source.name)
        with self._conn.connection as conn:
            df = pd.read_sql(
                sql=f"SELECT distinct source_entity FROM {self._table.name} WHERE "
                    f"source_name='{self._source.name}' and manifest_name='{self._manifest_name}'",
                con=conn)
        return df.values.tolist()

    @classmethod
    def generate_id(cls) -> str:
        return uuid.uuid4().hex

    # ----------------------------------------------------------- #
    # --------------------- Private Methods --------------------- #
    # ----------------------------------------------------------- #
    def _build_state(self,
                     action: str,
                     source_entity: str,
                     source_filter: str,
                     sink_entity: str,
                     sink_filter: str,
                     sourcing_start_time,
                     sourcing_end_time,
                     sinking_start_time,
                     sinking_end_time,
                     status: str = None,
                     state_id: str = None,
                     correlation_id_in: str = None,
                     correlation_id_out: str = None,
                     record_count: int = None,
                     first_record_pulled=None,
                     last_record_pulled=None) -> dict:
        """
        Note: use of self.__formatdate:
        SQL Server datetime columns are only able to store fractional seconds to millisecond precision
        (3 decimal places). When you do strftime('%Y-%m-%d %H:%M:%S.%f')you get a string formatted to the precision
        of Python's datetime, which is microseconds (6 decimal places).
        The solution is to not format the datetime as a string, just pass the datetime value itself.
        """
        source_filter = source_filter
        if source_filter and (isinstance(source_filter, dict) or isinstance(source_filter, list)):
            source_filter = json.dumps(source_filter)

        sink_filter = sink_filter
        if sink_filter and (isinstance(sink_filter, dict) or isinstance(sink_filter, list)):
            sink_filter = json.dumps(sink_filter)

        if self._format_date and sourcing_start_time:
            sourcing_start_time = sourcing_start_time.strftime('%Y-%m-%d %H:%M:%S.%f')
        else:
            sourcing_start_time = sourcing_start_time

        if self._format_date and sourcing_end_time:
            sourcing_end_time = sourcing_end_time.strftime('%Y-%m-%d %H:%M:%S.%f')
        else:
            sourcing_end_time = sourcing_end_time

        if self._format_date and sinking_start_time:
            sinking_start_time = sinking_start_time.strftime('%Y-%m-%d %H:%M:%S.%f')
        else:
            sinking_start_time = sinking_start_time

        if self._format_date and sinking_end_time:
            sinking_end_time = sinking_end_time.strftime('%Y-%m-%d %H:%M:%S.%f')
        else:
            sinking_end_time = sinking_end_time

        state = {
            'state_id': state_id,
            'correlation_id_in': correlation_id_in,
            'correlation_id_out': correlation_id_out,
            'action': action,
            'status': status,
            'entity': source_entity,
            'entity_filter': source_filter,
            'sink_entity': sink_entity,
            'sink_filter': sink_filter,
            'git_sha': self.__git_sha(),
            'record_count': record_count,
            'sourcing_start_time': sourcing_start_time,
            'sourcing_end_time': sourcing_end_time,
            'sinking_start_time': sinking_start_time,
            'sinking_end_time': sinking_end_time,
            'first_record_pulled': first_record_pulled,
            'last_record_pulled': last_record_pulled
        }

        return state

    def __run_query(self, query, state) -> dict:

        query = query.values(action=state['action'],
                             status=state['status'],
                             job_id=self._job_id,
                             correlation_id_in=state['correlation_id_in'],
                             correlation_id_out=state['correlation_id_out'],
                             source_name=self._source.name,
                             source_type=self._source.type,
                             sink_name=self._sink.name,
                             sink_type=self._sink.type,
                             source_entity=state['entity'],
                             source_filter=state['entity_filter'],
                             sink_entity=state['sink_entity'],
                             sink_filter=state['sink_filter'],
                             git_sha=self.__git_sha(),
                             sourcing_start_time=state['sourcing_start_time'],
                             sourcing_end_time=state['sourcing_end_time'],
                             sinking_start_time=state['sinking_start_time'],
                             sinking_end_time=state['sinking_end_time'],
                             row_count=state['record_count'],
                             first_record_pulled=state['first_record_pulled'],
                             last_record_pulled=state['last_record_pulled'],
                             manifest_name=self._manifest_name,
                             run_id=self._run_id)

        with self._conn.connection as conn:
            self._logger.debug("%s - action is %s - status is %s", query, state['action'], state['status'])
            self._logger.debug("job_id is %s - correlation_id is %s", self._job_id, state['correlation_id_out'])
            conn.execute(query)

        if self._format_date and state['sourcing_start_time']:
            sourcing_start_time = datetime.strptime(state['sourcing_start_time'], '%Y-%m-%d %H:%M:%S.%f')
        else:
            sourcing_start_time = state['sourcing_start_time']

        if self._format_date and state['sourcing_end_time']:
            sourcing_end_time = datetime.strptime(state['sourcing_end_time'], '%Y-%m-%d %H:%M:%S.%f')
        else:
            sourcing_end_time = state['sourcing_end_time']

        if self._format_date and state['sinking_start_time']:
            sinking_start_time = datetime.strptime(state['sinking_start_time'], '%Y-%m-%d %H:%M:%S.%f')
        else:
            sinking_start_time = state['sinking_start_time']

        if self._format_date and state['sinking_end_time']:
            sinking_end_time = datetime.strptime(state['sinking_end_time'], '%Y-%m-%d %H:%M:%S.%f')
        else:
            sinking_end_time = state['sinking_end_time']

        return {
            'state_id': state['state_id'],
            'job_id': self._job_id,
            'correlation_id_in': state['correlation_id_in'],
            'correlation_id_out': state['correlation_id_out'],
            'source_entity': state['entity'],
            'source_filter': state['entity_filter'],
            'sourcing_start_time': sourcing_start_time,
            'sourcing_end_time': sourcing_end_time,
            'sinking_start_time': sinking_start_time,
            'sinking_end_time': sinking_end_time,
            'first_record_pulled': state['first_record_pulled'],
            'last_record_pulled': state['last_record_pulled'],
            'record_count': state['record_count'],
            'sink_name': self._sink.name,
            'source_name': self._source.name,
            'manifest_name': self._manifest_name,
            'run_id': self._run_id
        }

    @classmethod
    def __git_sha(cls) -> str:
        repo = git.Repo(search_parent_directories=True)
        short_sha = repo.git.rev_parse(repo.head.object.hexsha, short=True)
        return short_sha
