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

 --NOTE: The below is applicable for both SQLServer and Azure SQLServer

CREATE DATABASE `hdm`;
DROP TABLE IF EXISTS `state_manager`;
--changes in status amd update_on columns
CREATE TABLE `state_manager` (
  `state_id` varchar(32) DEFAULT NULL,
  `run_id` varchar(32) DEFAULT NULL,
  `job_id` varchar(32) DEFAULT NULL,
  `correlation_id_in` varchar(32) DEFAULT NULL,
  `correlation_id_out` varchar(32) DEFAULT NULL,
  `action` varchar(20) DEFAULT NULL,
  `status` varchar(11) CHECK(status IN ('success','failure','in_progress')) DEFAULT NULL,
  `source_name` varchar(50) DEFAULT NULL,
  `source_type` varchar(50) DEFAULT NULL,
  `sink_name` varchar(50) DEFAULT NULL,
  `sink_type` varchar(50) DEFAULT NULL,
  `source_entity` varchar(256) DEFAULT NULL,
  `source_filter` varchar(256) DEFAULT NULL,
  `sink_entity` varchar(256) DEFAULT NULL,
  `sink_filter` varchar(256) DEFAULT NULL,
  `first_record_pulled` varchar(256) DEFAULT NULL,
  `last_record_pulled` varchar(256) DEFAULT NULL,
  `git_sha` char(40) DEFAULT NULL,
  `sourcing_start_time` datetime,
  `sourcing_end_time` datetime,
  `sinking_start_time` datetime,
  `sinking_end_time` datetime,
  `row_count` int DEFAULT NULL,
  `updated_on` datetime NULL,
  `manifest_name` varchar(256) DEFAULT NULL
);

--create trigger to update updated_on
CREATE TRIGGER dbo.trgAfterUpdate ON state_manager
AFTER INSERT, UPDATE
AS
  UPDATE f set updated_on=GETDATE()
  FROM
  state_manager AS f
  INNER JOIN inserted
  AS i
  ON f.state_id = i.state_id;