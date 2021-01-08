--Copyright © 2020 Hashmap, Inc
--Licensed under the Apache License, Version 2.0 (the "License");
--you may not use this file except in compliance with the License.
--You may obtain a copy of the License a
--http://www.apache.org/licenses/LICENSE-2.
--Unless required by applicable law or agreed to in writing, software
--distributed under the License is distributed on an "AS IS" BASIS,
--WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--See the License for the specific language governing permissions and
--limitations under the License.

--CREATE DATABASE `hdm`;
--DROP TABLE IF EXISTS `state_manager`;
CREATE TABLE IF NOT EXISTS state_manager (
  state_id TEXT,
  job_id TEXT,
  correlation_id_in TEXT,
  correlation_id_out TEXT,
  action TEXT,
  status TEXT,
  source_name TEXT,
  source_type TEXT,
  sink_name TEXT,
  sink_type TEXT,
  source_entity TEXT,
  source_filter TEXT,
  sink_entity TEXT,
  sink_filter TEXT,
  first_record_pulled TEXT,
  last_record_pulled TEXT,
  git_sha TEXT,
  sourcing_start_time INTEGER,
  sourcing_end_time INTEGER,
  sinking_start_time INTEGER,
  sinking_end_time INTEGER,
  updated_on INTEGER,
  row_count INTEGER,
  created_on INTEGER
);