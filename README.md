# Masking AI Agent

Masking AI Agent is built using python 3.x. It helps to 

  - Intellegently load balance masking job across multiple engines
  - Sync job/environment/global objects/entire engine
  - Backup / Restore masking engine metadata to / from file system


##### help
```shell
./mskaiagnt --help
```

##### List engines
```shell
./mskaiagnt list-engine
```

##### Add real dcenter engines
```shell
./mskaiagnt add-engine -m atmskengine01 -t 64 -s 10
./mskaiagnt add-engine -m atmskengine02 -t 64 -s 10
./mskaiagnt add-engine -m atmskengine03 -t 64 -s 10
```

##### List engines
```shell
./mskaiagnt list-engine
```

##### Generate dxtools.conf file
```shell
./mskaiagnt gen-dxtools-conf --protocol http
```

##### Validate VE
```shell
<FULL_PATH>/dxtoolkit2/dx_get_appliance -all -configfile ./dxtools.conf
```

##### Pull job list
```shell
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
./mskaiagnt sync-eng --srcmskengname atmskengine01 --tgtmskengname atmskengine02 -g --username admin --password xxxxxx --protocol https
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

### <a id="contribute"></a>How to Contribute

Please read [CONTRIBUTING.md](./CONTRIBUTING.md) to understand the pull requests process.

### <a id="statement-of-support"></a>Statement of Support

This software is provided as-is, without warranty of any kind or commercial support through Delphix. See the associated license for additional details. Questions, issues, feature requests, and contributions should be directed to the community as outlined in the [Delphix Community Guidelines](https://delphix.github.io/community-guidelines.html).

### <a id="license"></a>License

This is code is licensed under the Apache License 2.0. Full license is available [here](./LICENSE).

