<!---
Copyright © 2020 Hashmap, Inc

Licensed under the Apache License, Version 2.0 the \("License"\);
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
--->
# Hashmap Data Migrator

Table of Contents

* [About](#about)
* [Using hdm](#using-hashmap-data-migrator)
* [State Management](#state-management)
* [Pipeline YAML](#pipeline-yaml)
* [Connection Profile YAML](#connection-profile-yaml)
* [Logging](#logging)
* [User Documentation](#user-documentation)
    * [Sources](#sources)
        * [NetezzaSource](#netezzasource)
        * [NetezzaExternalTableSource](#netezzaexternaltablesource)
        * [FSSource](#fssource)
        * [FSChunkSource](#fschunksource)
        * [AzureBlobSource](#azureblobsource)
        * [DummySource](#dummysource) 
    * [Sinks](#sinks)
        * [FSSink](#fssink)
        * [AzureBlobSink](#azureblobsink)
        * [SnowflakeAzureCopySink](#snowflakeazurecopysink)
        * [DummySink](#dummysink)
   * [StateManagement](#state-management-1)
        * [AzureSQLServerStateManager](#azuresqlserverstatemanager)
        * [SQLServerStateManager](#sqlserverstatemanager)
        * [MySQLStateManager](#mysqlstatemanager)
        * [SqLiteStateManager](#sqlitestatemanager)
        * [StateManager- Base class](#statemanager)
   * [Data Access Object (DAO)](#data-access-object)
        * [NetezzaJDBC](#netezzajdbc)
        * [NetezzaODBC](#netezzaodbc)
        * [SnowflakeAzureCopy](#snowflakeazurecopy)
        * [MySQL](#mysql)
        * [SqLiteState](#sqlitestate)
        * [SQLServer](#sqlserver)
        * [AzureSQLServer](#azuresqlserver)
   * [Catalog](#catalog)
        * [NetezzaCrawler](#netezzacrawler)
        * [NetezzaToSnowflakeMapper](#netezzatosnowflakemapper)
        * [SnowflakeDDLWriter](#ddlwriter)
   * [Orchestrator](#orchestrator)
        * [DeclaredOrchestrator](#declaredorchestrator)
        * [BatchOrchestrator](#batchorchestrator)
        * [AutoOrchestrator](#autoorchestrator)
    * [Utils](#utils)
        * [Project Configuration](#project-configuration)
* [Repository Cloning](#repository-cloning)
* [Miscellaneous](#miscellaneous)

## About

Hashmap Data Migrator, or hdm, is a collection of composable data transport modules designed to incrementally move data from sources systems to cloud data systems.

Data is moved through pairs of sources and sinks. Ideally, this movement is meant to use a dataset as an intermediary. This allows one to create pipelines that follow modern practices while at the same time solving many issues that may arise if a pipeline is kept in memory in its entirety. While there is an additional IO overhead in many cases, this also allows workloads to be distributed arbitrarily and for portions of the pipelines to be ran on disparate systems - sharing only a state management data store (when full lineage is desired). In fact, in many situations, especially those of interest, data necessarily must be stored in files, broken into smaller chunks and transported across the wire to a cloud provider from an on-premises system.

## Using Hashmap Data Migrator

To use the Hashmap Data Migrator (hdm) you must first

1. Identify all locations where you would like for your solution to be deployed. It is a distributed application and can be partially deployed in many locations.
1. If it does not already exist in the deployment environment, create a hidden directory in the 'user' root called .hashmap_data_migrator.
1. Within the directory created in step 2 above, you must create a [connection profile YAML](Connection Profile YAML). This will hold the necessary connection information to connect Netezza, BigQuery and other data sources. Out of the box, at this time, there is no key management solution integrated. This is on the feature roadmap.
1. Install hashmap_data_migrator and all of its dependencies. This is a pypi package and can be installed as
```bash
pip install hashmap-data-migrator
```
1. Have a database available to use for state management.

Pipelines are defined declaratively in YAML files. These YAML files identify

* The orchestrator (internal hdm concept) used to orchestrate the execution of your pipeline. The options are:

  * declared_orchestrator - for manual or fully specified execution
  * batch_orchestrator - for when orchestration is defined in a fully specified batch
  * auto_batch_orchestrator - for when the execution is across all tables in specified combinations of databases and schemas

It is formatted in the YAML as such:
```yaml
orchestrator:
  name: Manual Orchestration
  type: declared_orchestrator
  conf: null
```

Next, and this should be consistent across all of the pipelines, the State Manager is specified. This is the glue the couples the otherwise independent portions of the pipeline together.

It is formatted in the YAML as such:
```yaml
state_manager:
  name: state_manager
  type: SQLiteStateManager
  conf:
    connection: state_manager
```

Next is the portion of the YAML that specifies the different steps in the data movement. These will be specified in two separate sections:

* declared_data_links - these are fully specified portions of a pipeline. In this each pair of source & sink is called a stage. See the example below that is targeted at offloading data from Netezza and storing it on a filesystem.

```yaml
declared_data_links:
  stages:
    - source:
        name: Netezza Source Connection
        type: NetezzaSource
        conf:
          ...
      sink:
        name: File System Sink Connection
        type: FSSink
        conf:
          ...
```
* template_data_links - these are partially defined source/sink pairs. Instead of being called stages they are called templates. A template has an additional field called a batch_definition. A batch definition will define how the template source is used - source is ALWAYS the template. See an example below for creating a pipeline that is pulling multiple tables at once. A similar example would be found for auto batch orchestration.
```yaml
template_data_links:
  templates:
    - batch_definition:
        - source_name: netezza_source
          field: table_name
          values:
            - database.schema.table_1
            - database.schema.table_1
          threads: 5
      source:
        name: netezza_source
        type: NetezzaSource
        conf:
          ...
          table_name: <<template>>
      sink:
        name: fs_chunk_stg
        type: FSSink
        conf:
          ...
```
*__NOTE:__* *Any asset (source or sink) specified must either exist or be creatable through the connector. Any and all credentials must exist in the hdm_profiles.yml as well.*

*__NOTE:__* *The pipeline can be split into separate files and executed distributively.*

Now, before we move on to more detailed documentation there remains one last function - cataloging operations. Before you run your code, when you are migrating data from one database to another you must
1. Catalog the existing assets
2. Map the assets in the source system to the target system

Now that the environment is specified, pipeline defined, and so on, all that remains is to run the code. The code is executed from bash (or at the terminal) through

```
python -m hashmap_data_migrator {manifest} -l {log settings} -e {env}

or

hashmap_data_migrator {manifest} -l {log settings} -e {env}
```

The parameters are:

* manifest - path of manifest to run
* log_settings - log settings path , default value ="log_settings.yml"
* env - environment to take connection information , default value ="prod"


## State Management

The management of the state of data movement. Useful for handling failures, storing history (audit trails) and much more. State of all data transport is stored in a database - which database is up to you - there is an extensible API with many out of the box implementations. This state management data is used to manage the control flow of a pipeline from end-to-end across a distributed deployments.

As of now MySQL, SQLLite, SQL Server and Azure SQL Server database tables can be used for state management.

The state management table will track the following values:

* state_id: unique identifier
* run_id: unique identifier for a run
* job_id: unique identifier for a stage (source and sink pair).
* correlation_id_in: Correlation id linking to a preceding pipeline
* correlation_id_out: Correlation id on persistence
* action: Action performed - sourcing pre-pull | sourcing post-pull| sinking pre-pull | sinking post-pull
* status: Status of the transformation - success | failure | in_progress
* source_name: Name of source
* source_type: Type of source
* sink_name: Name of sink
* sink_type: Type of sink
* source_entity: Asset being transported
* source_filter: Any filtering applied
* sink_entity: Asset being dumped 
* sink_filter: Any filtering applied 
* first_record_pulled: First record pulled in the run.Relevant to database only.
* last_record_pulled: Last record pulled in the run.Relevant to database only. 
* git_sha: Correlates code execution to the late
* sourcing_start_time: When sourcing started
* sourcing_end_time: When sourcing ended 
* sinking_start_time: When sinking started 
* sinking_end_time: When sourcing ended 
* updated_on: When this entry was last updated
* row_count: Number of distinct rows extracted
* created_on: When this entry was created
* manifest_name: Name of the pipeline YAML file

## Pipeline YAML

What the user should be focused on until a front-end gets built. This is merely a configuration file,
given parameters by the user it will flow these data points to the relevant classes and move the data from source to sink.
Example YAML can be found under manifests folder.

```yaml
version: 1
state_manager_type:
data_links:
  type: builder
  mode: manual
  stages:
    - source:
        name: source_name_1
        type: NetezzaSource
        conf:
      sink:
        name: sink_name_1
        type: FSSink
        conf:
    - source:
        name: source_name_2
        type: FSSource
        conf:
      sink:
        name: sink_name_2
        type: s3Sink
        conf:
    - source:
        name: source_name_3
        type: s3Source
        conf:
      sink:
        name: sink_name_3
        type: SnowflakeCopySink
        conf:
```
## Connection Profile YAML

This files stores the connection information to the source, stage , sink, database for state management.
Its stored in local FS and its path is set in environment variable "HOME".
The below yml file format is based on netezza to snowflake data transport using azure blob staging:
```yaml
dev:
  netezza_jdbc:  * Note:Add this section if using JDBC driver
    host: <host>
    port: <port>
    database: <database_name>
    user: <user_name>
    password: <password>
    driver:
      name: <driver_name>
      path: <driver_path>
  netezza_odbc:  * Note:Add this section if using ODBC driver
    host: <host>
    port: <port>
    database: <database_name>
    user: <user_name>
    password: <password>
    driver: <driver_name>
  snowflake_admin_schema:
    authenticator: snowflake
    account: <account>
    role: <role>
    warehouse: <warehouse_name>
    database: <database_name>
    schema: <schema_name>
    user: <user_name>
    password: <password>
  azure:
    url: <blob_url>
    azure_account_url: <blob_url starting with azure://...>
    sas: <sas_key>
    container_name: <blob_container_name>
  state_manager:
    host: <host>
    port: <port>
    database: <database_name>
    user: <user_name>
    password: <password>
    driver: ODBC Driver 17 for SQL Server <*Note:only for azure sql server>
```

## Logging
The application logging is configurable in log_settings.yml. The log files are created at the root.
```yaml
version: 1
formatters:
  simple:
    format: '%(asctime)s - %(name)s - %(levelname)s  - %(message)s'
  json:
    format: '%(asctime)s %(name)s %(levelname)s %(message)s'
    class: pythonjsonlogger.jsonlogger.JsonFormatter
handlers:
  console:
    class : logging.StreamHandler
    formatter: simple
    level   : INFO
    stream  : ext://sys.stdout
  file:
    class : logging.handlers.RotatingFileHandler
    formatter: json
    level: <logging level>
    filename: <log file name>
    maxBytes: <maximum bytes per log file>
    backupCount: <backup log file count>
loggers:
  hdm:
    level: <logging level>
    handlers: [file, console]
    propagate: True
  hdm.core.source.netezza_source:
    level: <logging level>
    handlers: [file, console]
    propagate: True
  hdm.core.source.fs_source:
    level: <logging level>
    handlers: [file, console]
    propagate: <logging level>
  hdm.core.source.snowflake_external_stage_source:
    level: <logging level>
    handlers: [file, console]
    propagate: True
```

### User Documentation

#### Sources

##### NetezzaSource
Netezza storage source.

*base class*
```
RDBMSSource
```
*configuration*
* env - section name in hdm profile yml file for connection information
* table_name - table name
* watermark 
    - column: watermark column name
    - offset: offset value to compare to
* checksum 
    - function: checksum function. supported checksum methods are: hash, hash4, hash8
    - column: checksum column name
```
    - source:
        name: netezza_source
        type: NetezzaSource
        conf:
          env: netezza_jdbc
          table_name: ADMIN.TEST1
          watermark:
              column: T1
              offset: 2
          checksum:
              function:
              column:
```

*consume API*

input:
```
env: section name in hdm profile yml file for connection information | required
table_name: table name | required
watermark: watermark information
checksum: checksum information | default random
```

output:
```
data_frame: pandas.DataFrame
source_type: 'database'
record_count: pandas.DataFrame shape
table_name: table name
```
##### NetezzaExternalTableSource

Netezza External Table storage source.

*base class*
```
Source
```

*configuration*
* env - section name in hdm profile yml file for connection information
* table_name - table name
* directory - staging directory
* watermark 
    - column: watermark column name
    - offset: offset value to compare to
* checksum 
    - function: checksum function. supported checksum methods are: hash, hash4, hash8
    - column: checksum column name
```
    - source:
        name: netezza_source
        type: NetezzaExternalTableSource
        conf:
          env: netezza_jdbc
          table_name: ADMIN.TEST1
          watermark:
              column: T1
              offset: 2
          checksum:
              function:
              column:
          directory: $HDM_DATA_STAGING
```

*consume API*

input:
```
env: section name in hdm profile yml file for connection information | required
table_name: table name | required
directory: staging directory | required
watermark: watermark information
checksum: checksum information | default random
```

output:
```
data_frame: pandas.DataFrame | required
source_type: 'database'
record_count: pandas.DataFrame shape | required
```
##### FSSource

File system storage source.

*base class*
```
Source
```

*configuration*
* directory - staging directory
```
    - source:
        name: fs_stg
        type: FSSource
        conf:
          directory: $HDM_DATA_STAGING
```
*consume API*

input:
```
directory: staging directory | required
```

output:
```
data_frame: pandas.DataFrame | required
file_name: file name
record_count: pandas.DataFrame shape | required
table_name: extracted table name from blob file path
```
##### FSChunkSource

File system chunking storage source.

*base class*
```
Source
```

*configuration*
* directory - staging directory
* chunk - file chunk size
```
    - source:
        name: fs_chunk_stg
        type: FSChunkSource
        conf:
          directory: $HDM_DATA_STAGING
          chunk: 200
```
*consume API*

input:
```
directory: staging directory | required
chunk: file chunk size
```

output:
```
data_frame: pandas.DataFrame | required
file_name: file name
parent_file_name: file name of the large parent file | required
record_count: pandas.DataFrame shape | required
table_name: extracted table name from file path
source_filter: source filter to query state management record for updates when the source_entity is same (the parent file name)| required
```
##### AzureBlobSource

BLOB data storage source.

*base class*
```
Source
```

*configuration*
* env - section name in hdm profile yml file for connection information
* container - blob container name
```
    - source:
        name: azure_source
        type: AzureBlobSource
        conf:
          env: azure
          container: data
```

*consume API*

input:
```
env: section name in hdm profile yml file for connection information | required
container: container name | required
file_format: file format | default csv
```
output:
```
data_frame: pandas.DataFrame | required
file_name: file name
record_count: pandas.DataFrame shape | required
table_name: extracted table name from blob file path
```

##### DummySource

Dummy storage source. Use when a source is not needed.

*base class*
```
Source
```
*configuration*
* dummy: None
```
    - source:
        name: source_name
        type: DummySource
        conf:
          dummy: None
```

*consume API*

input:
```
dummy: placeholder | required
```

output:
```
None
```
#### Sinks
##### FSSink

File system storage sink

base class:
```
Sink
```

*configuration*
* directory - staging directory
```
      sink:
        name: sink_name
        type: FSSink
        conf:
          directory: $HDM_DATA_STAGING
```
*consume API*

input:
```
directory: staging directory | required
```

output:
```
record_count: pandas.DataFrame shape | required
```
##### AzureBlobSink

Azure blob storage sink

*base class*
```
Sink
```
*configuration*
* env - section name in hdm profile yml file for connection information
* container - staging directory

```
      sink:
        name: azure_sink
        type: AzureBlobSink
        conf:
          env: azure
          container: data
```
*consume API*

input:
```
env: section name in hdm profile yml file for connection information | required
container: container name | required
```

output:
```
record_count: pandas.DataFrame shape | required
```
##### SnowflakeAzureCopySink

Snowflake Azure storage stage sink

*base class*
```
SnowflakeCopySink
```
*configuration*
* env - section name in hdm profile yml file for connection information
* stage_name - staging directory
* file_format - file format
* stage_directory - azure blob container name

```
      sink:
        name: sflk_copy_into_sink
        type: SnowflakeAzureCopySink
        conf:
          stage_name: TMP_KNERRIR
          file_format: csv
```
*consume API*

input:
```
env: section name in hdm profile yml file for connection information | required
stage_name: snowflake storage stage name| required
file_format: file format | required
stage_directory: container name | required
```

output:
```
record_count: pandas.DataFrame shape | required
```
##### DummySink

Dummy sink. Use when a sink is not needed.

*base class*
```
Sink
```
*configuration*
* dummy: None
```
      sink:
        name: sink_name
        type: DummySink
        conf:
          dummy: None
```
*consume API*

input:
```
dummy: placeholder | required
```
output:
```
None
```
#### State Management

##### AzureSQLServerStateManager

Azure SQL Server State Management

*base class*
```
StateManager
```

*configuration*
* connection - state_manager

```
state_manager:
  name: state_manager
  type: AzureSQLServerStateManager
  conf:
    connection: state_manager
```
*consume API*

input:
```
connection: state_manager  | required
dao: 'azuresqlserver'  | preset value in code | required
format_date: False   | preset value in code | required
```

##### SQLServerStateManager

SQL Server State Management

*base class*
```
StateManager
```

*configuration*
```
state_manager:
  name: state_manager
  type: SQLServerStateManager
  conf:
    connection: state_manager
```
*consume API*

input:
```
connection: state_manager  | required
dao: 'azuresqlserver'  | preset value in code | required
format_date: False   | preset value in code | required
```

##### MySQLStateManager

MYSQL State Management

*base class*
```
StateManager
```

*configuration*
* connection - state_manager

```
state_manager:
  name: state_manager
  type: MySQLStateManager
  conf:
    connection: state_manager
```
*consume API*

input:
```
connection: state_manager  | required
dao: 'mysql'  | preset value in code | required
format_date: True   | preset value in code | required
```

##### SqLiteStateManager

SQLLite State Management

*base class*
```
StateManager
```
*configuration*
* connection - state_manager

```
state_manager:
  name: state_manager
  type: SqLiteStateManager
  conf:
    connection: state_manager
```
*consume API*

input:
```
connection: state_manager  | required
dao: 'sqlite'  | preset value in code | required
format_date: True   | preset value in code | required
```

##### StateManager
 State Management base class

Methods:

```
name: insert_state
      insert new state
params: source_entity, source_filter,action,state_id,
       status, correlation_id_in, correlation_id_out,
       sink_entity, sink_filter,sourcing_start_time, 
       sourcing_end_time, sinking_start_time, sinking_end_time,
       record_count, first_record_pulled, last_record_pulled

return value: dictionary of state_id,job_id,correlation_id_in,
               correlation_id_out,source_entity,source_filter,
               sourcing_start_time,sourcing_end_time,sinking_start_time,
               sinking_end_time, first_record_pulled,last_record_pulled,
               record_count,run_id,manifest_name
```
```
name: update_state
      update a state
params:source_entity, source_filter,action,state_id,
           status, correlation_id_in, correlation_id_out,
           sink_entity, sink_filter,sourcing_start_time, 
           sourcing_end_time, sinking_start_time, sinking_end_time,
           record_count, first_record_pulled, last_record_pulled
return value :dictionary of state_id,job_id,correlation_id_in,
                   correlation_id_out,source_entity,source_filter,
                   sourcing_start_time,sourcing_end_time,sinking_start_time,
                   sinking_end_time, first_record_pulled,last_record_pulled,
                   record_count,run_id,manifest_name
```
```
name: get_current_state
      get current state
params: job_id | required, entity, entity_filter
return value : dictionary of state_id,job_id,correlation_id_in,
                   correlation_id_out,source_entity,source_filter,
                   sourcing_start_time,sourcing_end_time,sinking_start_time,
                   sinking_end_time, first_record_pulled,last_record_pulled,
                   record_count,run_id,manifest_name
```
```
name: get_last_record
      gets last_record_pulled value
params: entity
return value : last_record_pulled
```
```
name: get_processing_history
      get processing history
params: none
return value : list of sink_entity for a source_name
```
#### Data Access Object

##### NetezzaJDBC

connect to netezza using JDBC driver

*base class*
```
netezza
```

input:
```
connection:  section name in hdm profile yml file for connection information | required
```
output:
```
connection - connection to netezza
```
##### NetezzaODBC

connect to netezza using ODBC driver

base class:
```
netezza
```
input:
```
connection:  section name in hdm profile yml file for connection information | required
```
output:
```
connection - connection to netezza
```
##### AzureBlob

connect to azure storage account

*base class*
```
ObjectStoreDAO
```
input:
```
connection:  section name in hdm profile yml file for connection information | required
```
output:
```
connection - connection to netezza
```
##### SnowflakeAzureCopy

connect to snowflake
create or replace snowflake azure storage stage

*base class*
```
SnowflakeCopy
```
input:
```
connection:  section name in hdm profile yml file for connection information | required
stage_directory: name of azure blob container used for staging files.
stage_name: name of snowflake azure storage stage
```
output:
```
connection - connection to snowflake
```
##### MySQL

connect to MySQL db

*base class*
```
DBDAO
```

input:
```
connection:  section name in hdm profile yml file for connection information | required
```
output:
```
connection - connection to MySQL
engine - connection engine
```
##### SQLlite

connect to SQLlite db

*base class*
```
DBDAO
```

input:
```
connection:  section name in hdm profile yml file for connection information | required
```
output:
```
connection - connection to SQLlite
engine - connection engine
```
##### SQLServer

connect to SQL Server db

*base class*
```
 DBDAO
```

input:
```
connection:  section name in hdm profile yml file for connection information | required
engine - connection engine
```
output:
```
connection - connection to SQL Server
engine - connection engine
```
##### AzureSQLServer

connect to Azure SQL Server db

*base class*
```
 DBDAO
```
input:
```
connection:  section name in hdm profile yml file for connection information | required
```
output:
```
connection - connection to Azure SQL Server
engine - connection engine
```
#### catalog
Get databases, schemas and tables from Netezza DW and create the same in Snowflake DW.
Run the below:
```
python -m CloneNetezzaDDL
```
##### NetezzaCrawler
Get database, schema and table information in Netezza DW.

input:
```
connection_name:  section name in hdm profile yml file for connection information | required
```
output:
```
databases: list of databases | required
schemas: list of schemas| required
tables: list of tables | required
```
##### NetezzaToSnowflakeMapper
Execute database, schema and table ddls

input:
```
databases: list of databases | required
schemas: list of schemas| required
tables: list of tables | required
```
output:
```
database_sql: database ddl | required
schema_sql: schema ddl | required
table_sql: table ddl
```
##### SnowflakeDDLWriter
Execute database, schema and table ddls

input:
```
env: section name in hdm profile yml file for connection information | required
database_sql: database ddl | required
schema_sql: schema ddl | required
table_sql: table ddl
```
output:
```
none
```
#### Orchestration

##### DeclaredOrchestrator

This is an orchestrator which build DataLinks and will run them as defined - they must be fully defined.

*base class*
```
orchestrator
```
*configuration*

```
orchestrator:
  name: Manual Orchestration
  type: DeclaredOrchestrator
  conf: null
```
*consume API*

input:
```
none
```
output:
```
data_links: list of data links  | required
```
##### BatchOrchestrator

This is an orchestrator which build DataLinks and will run them as defined or templated.

*base class*
```
orchestrator
```
*configuration*

```
orchestrator:
  name: Batch Orchestration
  type: BatchOrchestrator
  conf: null
```
*consume API*

input:
```
none
```
output:
```
data_links: list of data links  | required
```
##### AutoOrchestrator

This is an orchestrator which build DataLinks and will run them as defined or templated.
The template will have information about schema and database of the source system.

Note: This is work in progress ...

*base class*
```
orchestrator
```
*configuration*

```
orchestrator:
  name: Auto Batch Orchestration
  type: AutoOrchestrator
  conf: null
```
*consume API*

input:
```
none
```
output:
```
data_links: list of data links  | required
```

#### Utils

##### Project Configuration
The following can be configured in utils/project_config.py
* profile_path - folder and name of profile YML file
* file_prefix - prefix to be used for staged files
* state_manager_table_name - state management table name
* archive_folder - archive folder name (used in FSChunkSource)
* connection_max_attempts - maximum number of try  (used in DAOs)
* connection_timeout - connection timeout value  (used in DAOs)
* query_limit - numbers rows returned by a query

## Repository Cloning
Please refer to [clone readme](../../Desktop/hashmap_data_migrator/clone/clone-readme.md)

## Miscellaneous
* The database, schema, table name are part of the folder structure where files are dumped - in any file staging location.
* The sink_name of a stage is part of the folder structure where files are dumped - in any file staging location.
* Any file created locally is stored in {HDM_DATA_STAGING}\{sink_name}\{table_name}\
* Any file created in cloud staging is stored in folder structure {sink_name}\{table_name}\
* The source_name and sink_name <b>MUST MATCH between stages (combination of source and sink)</b> for the migrator to be able to pick up files for processing.
E.g. Below you can see that sink_name for stage "FS to AzureBlob" is <b>azure_sink</b>. so, the source_name for stage "cloud storage create staging and run copy"
is also <b>azure_sink</b>
```
# FS to AzureBlob
    - source:
        name: fs_chunk_stg
        type: FSSource
        conf:
          directory: $HDM_DATA_STAGING
      sink:
        name: *azure_sink*
        type: AzureBlobSink
        conf:
          env: azure
          container: data

#cloud storage create staging and run copy
    - source:
        name: *azure_sink*
        type: AzureBlobSource
        conf:
          env: azure
          container: data
      sink:
        name: sflk_copy_into_sink
        type: SnowflakeAzureCopySink
        conf:
          env: snowflake_knerrir_schema
          stage_name: TMP_KNERRIR
          file_format: csv
          stage_directory: data
```



