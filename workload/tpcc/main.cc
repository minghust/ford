// Author: Ming Zhang
// Copyright (c) 2021

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <mutex>
#include <thread>

#include "config/config_system.h"
#include "stat/result_collect.h"
#include "tpcc/tpcc_bench.h"
#include "util/json_config.h"

extern std::atomic<uint64_t> tx_id_generator;
extern std::atomic<uint64_t> connected_t_num;

// Client-side: Run various TPCC transactions

// Model multi-machine based coordinators with single-machine based multi-threading coordinators:
// They use a global transaction id generator, i.e., std::atomic<uint64_t> tx_id_generator
int main(int argc, char* argv[]) {
  ModifyComputeNodeConfig(argc, argv);
  std::string config_filepath = "../../../config/compute_node_config.json";
  auto json_config = JsonConfig::load_file(config_filepath);
  auto client_conf = json_config.get("local_compute_node");
  node_id_t machine_num = (node_id_t)client_conf.get("machine_num").get_int64();
  node_id_t machine_id = (node_id_t)client_conf.get("machine_id").get_int64();
  t_id_t thread_num_per_machine = (t_id_t)client_conf.get("thread_num_per_machine").get_int64();
  const int coro_num = (int)client_conf.get("coroutine_num").get_int64();
  assert(machine_id >= 0 && machine_id < machine_num);

  /* Start working */
  tx_id_generator = 0;  // Initial transaction id == 0
  connected_t_num = 0;  // Sync all threads' RDMA QP connections
  auto thread_arr = new std::thread[thread_num_per_machine];
  TPCC* tpcc_client = new TPCC();
  auto* global_meta_man = new MetaManager();
  RDMA_LOG(INFO) << "Alloc local memory: " << (size_t)(thread_num_per_machine * PER_THREAD_ALLOC_SIZE) / (1024 * 1024) << " MB. Waiting...";
  auto* global_rdma_region = new RDMARegionAllocator(global_meta_man, thread_num_per_machine);

  auto* param_arr = new struct thread_params[thread_num_per_machine];

  RDMA_LOG(INFO) << "spawn threads to execute...";
  for (t_id_t i = 0; i < thread_num_per_machine; i++) {
    param_arr[i].thread_local_id = i;
    param_arr[i].thread_global_id = (machine_id * thread_num_per_machine) + i;
    param_arr[i].coro_num = coro_num;
    param_arr[i].tpcc_client = tpcc_client;
    param_arr[i].global_meta_man = global_meta_man;
    param_arr[i].global_rdma_region = global_rdma_region;
    param_arr[i].thread_num_per_machine = thread_num_per_machine;
    param_arr[i].total_thread_num = thread_num_per_machine * machine_num;
    thread_arr[i] = std::thread(run_thread, &param_arr[i]);

    /* Pin thread i to hardware thread 2 * i */
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(i, &cpuset);
    int rc = pthread_setaffinity_np(thread_arr[i].native_handle(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
      RDMA_LOG(WARNING) << "Error calling pthread_setaffinity_np: " << rc;
    }
  }

  for (t_id_t i = 0; i < thread_num_per_machine; i++) {
    thread_arr[i].join();
  }
  RDMA_LOG(INFO) << "DONE";

  delete[] param_arr;
  delete global_rdma_region;
  delete global_meta_man;
  delete tpcc_client;

  CollectResult("TPCC", std::string(argv[1]));
}
