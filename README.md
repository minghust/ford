# FORD
This is an open source repository for our papers in [FAST 2022](https://www.usenix.org/conference/fast22) and [ACM Transactions on Storage](https://dl.acm.org/journal/tos).

> Ming Zhang, Yu Hua, Pengfei Zuo, and Lurong Liu. "FORD: Fast One-sided RDMA-based Distributed Transactions for Disaggregated Persistent Memory". In 20th USENIX Conference on File and Storage Technologies, FAST 2022, Santa Clara, California, USA, February 22 - 24, 2022, pages 51-68. USENIX Association, 2022.
>
> Ming Zhang, Yu Hua, Pengfei Zuo, and Lurong Liu. "Localized Validation Accelerates Distributed Transactions on Disaggregated Persistent Memory". Accepted and to appear in ACM Transactions on Storage (TOS), 2023.

# Brief Introduction
Persistent memory (PM) disaggregation improves the resource utilization and failure isolation to build a scalable and cost-effective remote memory pool. However, due to offering limited computing power and overlooking the persistence and bandwidth properties of real PMs, existing distributed transaction schemes, which are designed for legacy DRAM-based monolithic servers, fail to efficiently work on the disaggregated PM architecture.

We propose FORD, a **F**ast **O**ne-sided **R**DMA-based **D**istributed transaction system. FORD thoroughly leverages one-sided RDMA to handle transactions for bypassing the remote CPU in PM pool. To reduce the round trips, FORD batches the read and lock operations into one request to eliminate extra locking and validations. To accelerate the transaction commit, FORD updates all the remote replicas in a single round trip with parallel undo logging and data visibility control. Moreover, considering the limited PM bandwidth, FORD enables the backup replicas to be read to alleviate the load on the primary replicas, thus improving the throughput. To efficiently guarantee the remote data persistency in the PM pool, FORD selectively flushes data to the backup replicas to mitigate the network overheads. FORD further leverages a localized validation scheme to transfer the validation operations for the read-only data from remote to local as much as possible to reduce the round trips. Experimental results demonstrate that FORD improves the transaction throughput and reduces the latency. To learn more, please read our papers.

# Framework
We implement a coroutine-enabled framework that runs FORD and its counterparts in the same manner when processing distributed transactions: 1) Issue one-sided RDMA requests. 2) Yield CPU to another coroutine. 3) Check all the RDMA ACKs and replies. This is in fact an interleaved execution model that aims to saturate the CPUs in the compute pool to improve the throughput.

# Prerequisites to Build
- Hardware
  - Intel Optane DC Persistent Memory
  - Mellanox InfiniBand NIC (e.g., ConnectX-5) that supports RDMA
  - Mellanox InfiniBand Switch
- Software
  - Operating System: Ubuntu 18.04 LTS or CentOS 7
  - Programming Language: C++ 11
  - Compiler: g++ 7.5.0 (at least)
  - Libraries: ibverbs, pthread, boost_coroutine, boost_context, boost_system
- Machines
  - At least 3 machines, in which one acts as the compute pool and other two act as the memory pool to maintain a primary-backup replication


# Configure
- Configure all the options in ```compute_node_config.json``` and ```memory_node_config.json``` in ```config/``` as you need, e.g., machine_num, machine_id, ip, port, and PM path, etc.
- Configure the options in ```core/flags.h```, e.g., ```MAX_ITEM_SIZE```, etc.
- Configure the number of backup replicas in ```core/base/common.h```, i.e., BACKUP_DEGREE.

# Build
The codes are constructed by CMake (version >= 3.3). We prepare a shell script for easy building

```sh
$ git clone https://github.com/minghust/ford.git
$ cd ford
```

- For each machine in the memory pool: 

```sh 
$ ./build.sh -s
```

- For each machine in the compute pool (boost is required):

```sh 
$ ./build.sh
```

Note that the Release version is the default option for better performance. However, if you need a Debug version, just add ```-d``` option, e.g., ```./build.sh -s -d``` for the memory pool, and ```./build.sh -d``` for the compute pool.

After running the ```build.sh``` script, cmake will automatically generate a ```build/``` directory in which all the compiled libraries and executable files are stored.


# Run
- For each machine in the memory pool: Start server to load tables. Due to using PM in *devdax* mode, you may need ```sudo``` if you are not a root user.
```sh
$ cd ford
$ cd ./build/memory_pool/server
$ sudo ./zm_mem_pool
```

- For each machine in the compute pool: After loading database tables in the memory pool, we run a benchmark, e.g., TPCC.
```sh
$ cd ford
$ cd ./build/compute_pool/run
$ ./run tpcc ford 16 8 # run ford with 16 threads and each thread spawns 8 coroutines
```
Now, the memory nodes are in a disaggregated mode, i.e., the CPUs are not used for any computation tasks in transaction processing.

# Results
After running, we automatically generate a ```bench_results``` dir to record the results. The summarized attempted and committed throughputs (K txn/sec) and the average 50th and 99th percentile latencies are recorded in ```bench_results/tpcc/result.txt```. Moreover, the detailed results of each thread are recorded in ```bench_results/tpcc/detail_result.txt``` 

# Acknowledgments

We sincerely thank the following open source repos (in the ```thirdparty/``` directory) that help us shorten the developing process

- [rlib](https://github.com/wxdwfc/rlib): We use rlib to do RDMA connections. This is a convinient and easy-to-understand library to finish RDMA connections. Moreover, we have modified rlib : 1) Fix a bug in en/decoding the QP id. 2) Change the QP connections from the active mode to the passive mode in the server side. In this way, all the QP connections are completed without explict ```connect``` usages in the server-side code. This is beneficial for the case in which the server does not know how many clients will issue the connect requests.

- [rapidjson](https://github.com/Tencent/rapidjson): We use rapidjson to read configurations from json files. This is an easy-to-use library that accelerate configurations.

# LICENSE

```text
Copyright [2022] [Ming Zhang]

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
