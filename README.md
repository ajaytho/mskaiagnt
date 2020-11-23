# Masking AI Agent

Masking AI Agent is built using python 3.x. This agent will help to  

  1. Intellegently load balance masking job across multiple engines
  2. Sync job/environment/global objects/entire engine using source engine
  3. Backup / Restore masking engine metadata to / from file system

There are multiple modules. List of available modules can be listed as below
##### help
```shell
./mskaiagnt --help
```
Output
```shell
Usage: mskaiagnt [OPTIONS] COMMAND [ARGS]...

Options:
  -v, --verbose
  -d, --debug
  --help         Show this message and exit.

Commands:
  version              Script Version
  add-engine           This module will add engine to pool
  list-engine          This module will remove engine from pool
  del-engine           This module will remove engine from pool
  pull-joblist         This module will pull joblist from engine
  gen-dxtools-conf     This module will generate dxtools conf file for...
  sync-job             This module will sync particular job between 2...
  sync-env             This module will sync particular env between 2...
  sync-eng             This module will sync particular env between 2...
  sync-globalobj       This module will sync global objects between 2...
  cleanup-eng          This module will cleanup engine
  run-job              This module will execute masking job on best...
  test-connectors      This module will cleanup engine
  list-eng-usage       This module will find green engines
  offline-backup-eng   This module will offline backup engine
  offline-restore-eng  This module will offline restore engine from backups
```

##### List engines
This command show engines available in pool
```shell
./mskaiagnt list-engine
```

##### Add engines to pool
This command helps to add engines to the pool
```shell
./mskaiagnt add-engine -m atmskengine01 -t 64 -s 10
./mskaiagnt add-engine -m atmskengine02 -t 64 -s 10
./mskaiagnt add-engine -m atmskengine03 -t 64 -s 10
```

##### List engines
```shell
./mskaiagnt list-engine

e.g.
 EngineName                             Total Memory(GB)   System Memory(GB)
 atmskengine01                                        64                  10
 atmskengine02                                        64                  10
 atmskengine03                                        64                  10
```

##### Generate dxtools.conf file
```shell
./mskaiagnt gen-dxtools-conf --protocol http

e.g.
./mskaiagnt gen-dxtools-conf --protocol http
                            AI Agent for Delphix Masking Server - 1.1.3
****************************************************************************************************
./dxtools.conf file generated successfully
```

##### Validate VE
This functionality/settings is needed only for load balancing feature. Not applicable for sync and backup/restore.
```shell
<FULL_PATH>/dxtoolkit2/dx_get_appliance -all -configfile ./dxtools.conf
```

##### Pull job list
```shell

./mskaiagnt pull-joblist --help
Usage: mskaiagnt pull-joblist [OPTIONS]

  This module will pull joblist from engine

Options:
  -m, --mskengname TEXT  Masking Engine name
  -u, --username TEXT    Masking mskaiagnt username to connect masking engines
  -p, --password TEXT    Masking mskaiagnt password to connect masking engines
  --protocol TEXT        http protocol
  --help                 Show this message and exit.

e.g.  
./mskaiagnt pull-joblist -m all --username admin --password xxxxxx --protocol http
```

##### Run job - simulation
```shell
./mskaiagnt -v run-job -j maskjob6 -e mskdevenv --username admin --password xxxxxx --protocol http --dxtoolkit_path /home/ubuntu/WSL/dxtoolkit2
```

##### Real run
```shell
./mskaiagnt -v run-job -j maskjob6 -e mskdevenv --username admin --password xxxxxx --protocol http --dxtoolkit_path /home/ubuntu/WSL/dxtoolkit2 -r
```

##### Sync Eng
```shell
./mskaiagnt sync-eng --srcmskengname atmskengine01 --tgtmskengname atmskengine02 -g --username admin --password xxxxxx --protocol https --delextra
```

##### Sync Env
```shell
./mskaiagnt sync-env --srcmskengname atmskengine01 --tgtmskengname atmskengine02 --srcenvname mskuatenv --tgtenvname mskuatenv -g --username admin --password xxxxxx --protocol https
```

##### Sync Job
```shell
./mskaiagnt sync-job --srcmskengname atmskengine01 --tgtmskengname atmskengine02 --srcenvname mskdevenv --tgtenvname mskdevenv --srcjobname maskjob6 -g --username admin --password xxxxxx --protocol https
```

##### Cleanup Engine
```shell
./mskaiagnt cleanup-eng --mskengname atmskengine02 --username admin --password xxxxxx --protocol https
```

##### Backup Engine
```shell
./mskaiagnt offline-backup-eng --mskengname atmskengine02 --backup_dir /home/ubuntu/WSL/test --username admin --password xxxxxx --protocol http
```

##### Restore Engine
```shell
./mskaiagnt offline-restore-eng --mskengname atmskengine02 --backup_dir /home/ubuntu/WSL/test/MMDDYYYY_HH24MISS --username admin --password xxxxxx --protocol http
```

##### List Engine Usage
```shell
./mskaiagnt.py -v list-eng-usage --username admin --password xxxxxx --protocol http --dxtoolkit_path /home/ubuntu/WSL/dxtoolkit2
```


### <a id="contribute"></a>How to Contribute

Please read [CONTRIBUTING.md](./CONTRIBUTING.md) to understand the pull requests process.

### <a id="statement-of-support"></a>Statement of Support

This software is provided as-is, without warranty of any kind or commercial support through Delphix. See the associated license for additional details. Questions, issues, feature requests, and contributions should be directed to the community as outlined in the [Delphix Community Guidelines](https://delphix.github.io/community-guidelines.html).

### <a id="license"></a>License

This is code is licensed under the Apache License 2.0. Full license is available [here](./LICENSE).

