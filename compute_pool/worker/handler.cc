// Author: Ming Zhang
// Copyright (c) 2022

#include "worker/handler.h"

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <mutex>
#include <thread>

#include "util/json_config.h"
#include "worker/worker.h"

std::atomic<uint64_t> tx_id_generator;
std::atomic<uint64_t> connected_t_num;

std::vector<t_id_t> tid_vec;
std::vector<double> attemp_tp_vec;
std::vector<double> tp_vec;
std::vector<double> medianlat_vec;
std::vector<double> taillat_vec;
std::vector<double> lock_durations;
std::vector<uint64_t> total_try_times;
std::vector<uint64_t> total_commit_times;

void Handler::ConfigureComputeNode(int argc, char* argv[]) {
  std::string config_file = "../../../config/compute_node_config.json";
  std::string system_name = std::string(argv[2]);
  if (argc == 5) {
    std::string s1 = "sed -i '5c \"thread_num_per_machine\": " + std::string(argv[3]) + ",' " + config_file;
    std::string s2 = "sed -i '6c \"coroutine_num\": " + std::string(argv[4]) + ",' " + config_file;
    system(s1.c_str());
    system(s2.c_str());
  }
  // Customized test without modifying configs
  int txn_system_value = 0;
  if (system_name.find("farm") != std::string::npos) {
    txn_system_value = 0;
  } else if (system_name.find("drtm") != std::string::npos) {
    txn_system_value = 1;
  } else if (system_name.find("ford") != std::string::npos) {
    txn_system_value = 2;
  } else if (system_name.find("local") != std::string::npos) {
    txn_system_value = 3;
  }
  std::string s = "sed -i '8c \"txn_system\": " + std::to_string(txn_system_value) + ",' " + config_file;
  system(s.c_str());
  return;
}

void Handler::GenThreads(std::string bench_name) {
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

  auto* global_meta_man = new MetaManager();
  auto* global_vcache = new VersionCache();
  auto* global_lcache = new LockCache();
  RDMA_LOG(INFO) << "Alloc local memory: " << (size_t)(thread_num_per_machine * PER_THREAD_ALLOC_SIZE) / (1024 * 1024) << " MB. Waiting...";
  auto* global_rdma_region = new RDMARegionAllocator(global_meta_man, thread_num_per_machine);

  auto* param_arr = new struct thread_params[thread_num_per_machine];

  TATP* tatp_client = nullptr;
  SmallBank* smallbank_client = nullptr;
  TPCC* tpcc_client = nullptr;

  if (bench_name == "tatp") {
    tatp_client = new TATP();
    total_try_times.resize(TATP_TX_TYPES, 0);
    total_commit_times.resize(TATP_TX_TYPES, 0);
  } else if (bench_name == "smallbank") {
    smallbank_client = new SmallBank();
    total_try_times.resize(SmallBank_TX_TYPES, 0);
    total_commit_times.resize(SmallBank_TX_TYPES, 0);
  } else if (bench_name == "tpcc") {
    tpcc_client = new TPCC();
    total_try_times.resize(TPCC_TX_TYPES, 0);
    total_commit_times.resize(TPCC_TX_TYPES, 0);
  }

  RDMA_LOG(INFO) << "Spawn threads to execute...";

  for (t_id_t i = 0; i < thread_num_per_machine; i++) {
    param_arr[i].thread_local_id = i;
    param_arr[i].thread_global_id = (machine_id * thread_num_per_machine) + i;
    param_arr[i].coro_num = coro_num;
    param_arr[i].bench_name = bench_name;
    param_arr[i].global_meta_man = global_meta_man;
    param_arr[i].global_status = global_vcache;
    param_arr[i].global_lcache = global_lcache;
    param_arr[i].global_rdma_region = global_rdma_region;
    param_arr[i].thread_num_per_machine = thread_num_per_machine;
    param_arr[i].total_thread_num = thread_num_per_machine * machine_num;
    thread_arr[i] = std::thread(run_thread,
                                &param_arr[i],
                                tatp_client,
                                smallbank_client,
                                tpcc_client);

    /* Pin thread i to hardware thread i */
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(i, &cpuset);
    int rc = pthread_setaffinity_np(thread_arr[i].native_handle(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
      RDMA_LOG(WARNING) << "Error calling pthread_setaffinity_np: " << rc;
    }
  }

  for (t_id_t i = 0; i < thread_num_per_machine; i++) {
    if (thread_arr[i].joinable()) {
      thread_arr[i].join();
    }
  }

  RDMA_LOG(INFO) << "DONE";

  delete[] param_arr;
  delete global_rdma_region;
  delete global_meta_man;
  delete global_vcache;
  delete global_lcache;
  if (tatp_client) delete tatp_client;
  if (smallbank_client) delete smallbank_client;
  if (tpcc_client) delete tpcc_client;
}

void Handler::OutputResult(std::string bench_name, std::string system_name) {
  std::string results_cmd = "mkdir -p ../../../bench_results/" + bench_name;
  system(results_cmd.c_str());
  std::ofstream of, of_detail, of_abort_rate;
  std::string res_file = "../../../bench_results/" + bench_name + "/result.txt";
  std::string detail_res_file = "../../../bench_results/" + bench_name + "/detail_result.txt";
  std::string abort_rate_file = "../../../bench_results/" + bench_name + "/abort_rate.txt";

  of.open(res_file.c_str(), std::ios::app);
  of_detail.open(detail_res_file.c_str(), std::ios::app);
  of_abort_rate.open(abort_rate_file.c_str(), std::ios::app);

  of_detail << system_name << std::endl;
  of_detail << "tid attemp_tp tp 50lat 99lat" << std::endl;

  of_abort_rate << system_name << " tx_type try_num commit_num abort_rate" << std::endl;

  double total_attemp_tp = 0;
  double total_tp = 0;
  double total_median = 0;
  double total_tail = 0;

  for (int i = 0; i < tid_vec.size(); i++) {
    of_detail << tid_vec[i] << " " << attemp_tp_vec[i] << " " << tp_vec[i] << " " << medianlat_vec[i] << " " << taillat_vec[i] << std::endl;
    total_attemp_tp += attemp_tp_vec[i];
    total_tp += tp_vec[i];
    total_median += medianlat_vec[i];
    total_tail += taillat_vec[i];
  }

  size_t thread_num = tid_vec.size();

  double avg_median = total_median / thread_num;
  double avg_tail = total_tail / thread_num;

  std::sort(medianlat_vec.begin(), medianlat_vec.end());
  std::sort(taillat_vec.begin(), taillat_vec.end());

  of_detail << total_attemp_tp << " " << total_tp << " " << medianlat_vec[0] << " " << medianlat_vec[thread_num - 1]
            << " " << avg_median << " " << taillat_vec[0] << " " << taillat_vec[thread_num - 1] << " " << avg_tail << std::endl;

  of << system_name << " " << total_attemp_tp / 1000 << " " << total_tp / 1000 << " " << avg_median << " " << avg_tail << std::endl;

  if (bench_name == "tatp") {
    for (int i = 0; i < TATP_TX_TYPES; i++) {
      of_abort_rate << TATP_TX_NAME[i] << " " << total_try_times[i] << " " << total_commit_times[i] << " " << (double)(total_try_times[i] - total_commit_times[i]) / (double)total_try_times[i] << std::endl;
    }
  } else if (bench_name == "smallbank") {
    for (int i = 0; i < SmallBank_TX_TYPES; i++) {
      of_abort_rate << SmallBank_TX_NAME[i] << " " << total_try_times[i] << " " << total_commit_times[i] << " " << (double)(total_try_times[i] - total_commit_times[i]) / (double)total_try_times[i] << std::endl;
    }
  } else if (bench_name == "tpcc") {
    for (int i = 0; i < TPCC_TX_TYPES; i++) {
      of_abort_rate << TPCC_TX_NAME[i] << " " << total_try_times[i] << " " << total_commit_times[i] << " " << (double)(total_try_times[i] - total_commit_times[i]) / (double)total_try_times[i] << std::endl;
    }
  }

  of_detail << std::endl;
  of_abort_rate << std::endl;

  of.close();
  of_detail.close();
  of_abort_rate.close();

  std::cerr << system_name << " " << total_attemp_tp / 1000 << " " << total_tp / 1000 << " " << avg_median << " " << avg_tail << std::endl;

  // Open it when testing the duration
#if LOCK_WAIT
  if (bench_name == "MICRO") {
    // print avg lock duration
    std::string file = "../../../bench_results/" + bench_name + "/avg_lock_duration.txt";
    of.open(file.c_str(), std::ios::app);

    double total_lock_dur = 0;
    for (int i = 0; i < lock_durations.size(); i++) {
      total_lock_dur += lock_durations[i];
    }

    of << system_name << " " << total_lock_dur / lock_durations.size() << std::endl;
    std::cerr << system_name << " avg_lock_dur: " << total_lock_dur / lock_durations.size() << std::endl;
  }
#endif
}

void Handler::ConfigureComputeNodeForMICRO(int argc, char* argv[]) {
  std::string workload_filepath = "../../../config/micro_config.json";
  std::string arg = std::string(argv[1]);
  char access_type = arg[0];
  std::string s;
  if (access_type == 's') {
    // skewed
    s = "sed -i '4c \"is_skewed\": true,' " + workload_filepath;
  } else if (access_type == 'u') {
    // uniform
    s = "sed -i '4c \"is_skewed\": false,' " + workload_filepath;
  }
  system(s.c_str());
  // write ratio
  std::string write_ratio = arg.substr(2);  // e.g: 75
  s = "sed -i '7c \"write_ratio\": " + write_ratio + ",' " + workload_filepath;
  system(s.c_str());
}

void Handler::GenThreadsForMICRO() {
  std::string config_filepath = "../../../config/compute_node_config.json";
  std::string s = "sed -i '8c \"txn_system\": 2,' " + config_filepath;
  system(s.c_str());
  auto json_config = JsonConfig::load_file(config_filepath);
  auto client_conf = json_config.get("local_compute_node");
  node_id_t machine_num = (node_id_t)client_conf.get("machine_num").get_int64();
  node_id_t machine_id = (node_id_t)client_conf.get("machine_id").get_int64();
  t_id_t thread_num_per_machine = (t_id_t)client_conf.get("thread_num_per_machine").get_int64();
  lock_durations.resize(thread_num_per_machine);
  const int coro_num = (int)client_conf.get("coroutine_num").get_int64();
  assert(machine_id >= 0 && machine_id < machine_num);

  std::string thread_num_coro_num;
  if (coro_num < 10) {
    thread_num_coro_num = std::to_string(thread_num_per_machine) + "_0" + std::to_string(coro_num);
  } else {
    thread_num_coro_num = std::to_string(thread_num_per_machine) + "_" + std::to_string(coro_num);
  }

  system(std::string("mkdir -p ../../../bench_results/MICRO/" + thread_num_coro_num).c_str());
  system(std::string("rm ../../../bench_results/MICRO/" + thread_num_coro_num + "/total_lock_duration.txt").c_str());

  /* Start working */
  tx_id_generator = 0;  // Initial transaction id == 0
  connected_t_num = 0;  // Sync all threads' RDMA QP connections
  auto thread_arr = new std::thread[thread_num_per_machine];
  auto* global_meta_man = new MetaManager();
  auto* global_vcache = new VersionCache();
  auto* global_lcache = new LockCache();
  RDMA_LOG(INFO) << "Alloc local memory: " << (size_t)(thread_num_per_machine * PER_THREAD_ALLOC_SIZE) / (1024 * 1024) << " MB. Waiting...";
  auto* global_rdma_region = new RDMARegionAllocator(global_meta_man, thread_num_per_machine);

  auto* param_arr = new struct thread_params[thread_num_per_machine];

  RDMA_LOG(INFO) << "spawn threads...";
  for (t_id_t i = 0; i < thread_num_per_machine; i++) {
    param_arr[i].thread_local_id = i;
    param_arr[i].thread_global_id = (machine_id * thread_num_per_machine) + i;
    param_arr[i].coro_num = coro_num;
    param_arr[i].global_meta_man = global_meta_man;
    param_arr[i].global_status = global_vcache;
    param_arr[i].global_lcache = global_lcache;
    param_arr[i].global_rdma_region = global_rdma_region;
    param_arr[i].thread_num_per_machine = thread_num_per_machine;
    param_arr[i].total_thread_num = thread_num_per_machine * machine_num;
    param_arr[i].bench_name = "micro";
    thread_arr[i] = std::thread(run_thread,
                                &param_arr[i],
                                nullptr,
                                nullptr,
                                nullptr);

    /* Pin thread i to hardware thread */
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(i, &cpuset);
    pthread_setaffinity_np(thread_arr[i].native_handle(), sizeof(cpu_set_t), &cpuset);
  }

  for (t_id_t i = 0; i < thread_num_per_machine; i++) {
    if (thread_arr[i].joinable()) {
      thread_arr[i].join();
    }
  }
  RDMA_LOG(INFO) << "Done";

  delete[] param_arr;
  delete global_rdma_region;
  delete global_meta_man;
  delete global_vcache;
  delete global_lcache;
}
