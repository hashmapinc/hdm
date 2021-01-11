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
# hashmap_data_migrator packaging

## About

The purpose of this document is to help setup sub package for a hashmap_data_migrator source.

## Assumption
- a gitlab sub repository already available for the source. The repository can be empty.<br>
    e.g. hashmap_data_migrator-Netezza

## Steps to create a new sub repository
Note: {source_name} is the name of the source for which we want to create sub package.
- create a new gitlab sub repository manually. Repo naming convention: hashmap_data_migrator-{source_name}<br>
  e.g. hashmap_data_migrator-netezza
  
- copy and paste netezza_config.py under hashmap_data_migrator -> clone -> config folder. 
Rename new file to  {source_name}_config.py <br>
  e.g. netezza_config.py
  
- open the {source_name}_config.py 
    - rename class name
    - add/update files wherever needed.
    
- open file clone_main.py in hashmap_data_migrator root folder and add the following lines: 
```
      {source_name} = CloneProcess({source_name}Config())
      {source_name}.clone_repo() 

   e.g. 
        netezza = CloneProcess(NetezzaConfig())
        netezza.clone_repo()
```  
## Important!!!!
- for any new files added/renamed/moved/removed related to a source, the corresponding clone config file needs 
to be updated.

## Issues
If the build fails on packaging, check the below:
- gitlab access token is set and is not expired.
- gitlab sub repository is available for the source.





