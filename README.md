# Masking AI Agent

Masking AI Agent is built using python 3.x. It helps to 

  - Intellegently load balance masking job across multiple engines
  - Sync job/environment/global objects/entire engine


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
./mskaiagnt add-engine -m ajaydlpx6pri -t 64 -s 10
./mskaiagnt add-engine -m rp6021m -t 64 -s 10
./mskaiagnt add-engine -m rp6030m -t 64 -s 10
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
../dxtoolkit2/dx_get_appliance -all -configfile ./dxtools.conf
```

##### Pull job list
```shell
./mskaiagnt pull-joblist -m all --username admin --password xxxxxx --protocol http
```

##### Run job - simulataion
```shell
./mskaiagnt run-job -j maskjob6 -e mskdevenv --username admin --password xxxxxx
./mskaiagnt -v run-job -j maskjob6 -e mskdevenv --username admin --password xxxxxx
```

##### Real run
```shell
./mskaiagnt -v run-job -j maskjob6 -e mskdevenv --username admin --password xxxxxx -r
```

##### Sync Eng
```shell
./mskaiagnt sync-eng --srcmskengname atmskengine01 --tgtmskengname atmskengine02 -g --username admin --password xxxxxx
```

##### Sync Env
```shell
./mskaiagnt sync-env --srcmskengname atmskengine01 --srcenvname DB2LUW --tgtmskengname atmskengine01 --tgtenvname DB2LUW --username admin --password xxxxxx
```

##### Sync Job
```shell
./mskaiagnt sync-job --srcmskengname atmskengine01 --srcenvname mskdevenv --tgtmskengname atmskengine01 --tgtenvname mskdevenv --srcjobname maskjob6 --username admin --password xxxxxx
```

##### Cleanup Engine
```shell
./mskaiagnt cleanup-eng --mskengname atmskengine02 --username admin --password xxxxxx
```

### <a id="contribute"></a>How to Contribute

Please read [CONTRIBUTING.md](./CONTRIBUTING.md) to understand the pull requests process.

### <a id="statement-of-support"></a>Statement of Support

This software is provided as-is, without warranty of any kind or commercial support through Delphix. See the associated license for additional details. Questions, issues, feature requests, and contributions should be directed to the community as outlined in the [Delphix Community Guidelines](https://delphix.github.io/community-guidelines.html).

### <a id="license"></a>License

This is code is licensed under the Apache License 2.0. Full license is available [here](./LICENSE).

