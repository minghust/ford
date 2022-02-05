// Author: Ming Zhang
// Copyright (c) 2021

#include "micro/micro_bench.h"

#include <atomic>
#include <cstdio>
#include <fstream>
#include <functional>
#include <memory>
#include <set>

#include "allocator/buffer_allocator.h"
#include "allocator/log_allocator.h"
#include "connection/qp_manager.h"
#include "dtx/dtx.h"
#include "util/latency.h"
#include "util/zipf.h"

using namespace std::placeholders;

#define ATTEMPED_NUM 100000

// All the functions are executed in each thread

extern std::atomic<uint64_t> tx_id_generator;
extern std::atomic<uint64_t> connected_t_num;
extern std::vector<double> lock_durations;
extern std::mutex mux;

extern std::vector<t_id_t> tid_vec;
extern std::vector<double> attemp_tp_vec;
extern std::vector<double> tp_vec;
extern std::vector<double> medianlat_vec;
extern std::vector<double> taillat_vec;

__thread uint64_t seed; /* Thread-global random seed */
__thread t_id_t thread_gid;
__thread t_id_t thread_local_id;
__thread t_id_t thread_num;
__thread MICRO* micro_client;
__thread MetaManager* meta_man;
__thread QPManager* qp_man;
__thread RDMABufferAllocator* rdma_buffer_allocator;
__thread LogOffsetAllocator* log_offset_allocator;
__thread AddrCache* addr_cache;
__thread MicroTxType* workgen_arr;

__thread coro_id_t coro_num;
__thread CoroutineScheduler* coro_sched;  // Each transaction thread has a coroutine scheduler
__thread bool stop_run;

// Performance measurement (thread granularity)
__thread struct timespec msr_start, msr_end;
// __thread Latency* latency;
__thread double* timer;
__thread ZipfGen* zipf_gen;
__thread bool is_skewed;
__thread uint64_t data_set_size;
__thread uint64_t num_keys_global;
__thread uint64_t write_ratio;
// const int lat_multiplier = 10; // For sub-microsecond latency measurement
__thread uint64_t stat_attempted_tx_total = 0;  // Issued transaction number
__thread uint64_t stat_committed_tx_total = 0;  // Committed transaction number
const coro_id_t POLL_ROUTINE_ID = 0;            // The poll coroutine ID

/******************** The business logic (Transaction) start ********************/

struct DataItemDuplicate {
  DataItemPtr data_item_ptr;
  bool is_dup;
};

bool TxTestCachedAddr(coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  dtx->TxBegin(tx_id);
  bool is_write[data_set_size];
  DataItemDuplicate micro_objs[data_set_size];

  for (uint64_t i = 0; i < data_set_size; i++) {
    micro_key_t micro_key;
    micro_key.micro_id = 688;  // First read is non-cacheable, set ATTEMPED_NUM to 5

    assert(micro_key.item_key >= 0 && micro_key.item_key < num_keys_global);

    micro_objs[i].data_item_ptr = std::make_shared<DataItem>((table_id_t)MicroTableType::kMicroTable, micro_key.item_key);
    micro_objs[i].is_dup = false;

    dtx->AddToReadWriteSet(micro_objs[i].data_item_ptr);
    is_write[i] = true;
  }

  if (!dtx->TxExe(yield)) {
    // TLOG(DBG, thread_gid) << "tx " << tx_id << " aborts after exe";
    return false;
  }

  for (uint64_t i = 0; i < data_set_size; i++) {
    micro_val_t* micro_val = (micro_val_t*)micro_objs[i].data_item_ptr->value;
    if (micro_val->magic[0] != micro_magic) {
      RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << thread_gid << "-" << dtx->coro_id << "-" << tx_id;
    }
    if (is_write[i]) {
      micro_val->magic[1] = micro_magic * 2;
    }
  }

  bool commit_status = dtx->TxCommit(yield);
  // TLOG(DBG, thread_gid) << "tx " << tx_id << " commit? " << commit_status;
  return commit_status;
}

bool TxLockContention(coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  dtx->TxBegin(tx_id);
  bool is_write[data_set_size];
  DataItemPtr micro_objs[data_set_size];

  for (uint64_t i = 0; i < data_set_size; i++) {
    micro_key_t micro_key;
    if (is_skewed) {
      // Skewed distribution
      micro_key.micro_id = (itemkey_t)(zipf_gen->next());
    } else {
      // Uniformed distribution
      micro_key.micro_id = (itemkey_t)FastRand(&seed) & (num_keys_global - 1);
    }

    assert(micro_key.item_key >= 0 && micro_key.item_key < num_keys_global);

    micro_objs[i] = std::make_shared<DataItem>((table_id_t)MicroTableType::kMicroTable, micro_key.item_key);

    if (FastRand(&seed) % 100 < write_ratio) {
      dtx->AddToReadWriteSet(micro_objs[i]);
      is_write[i] = true;
    } else {
      dtx->AddToReadOnlySet(micro_objs[i]);
      is_write[i] = false;
    }
  }

  if (!dtx->TxExe(yield)) {
    // TLOG(DBG, thread_gid) << "tx " << tx_id << " aborts after exe";
    return false;
  }

  for (uint64_t i = 0; i < data_set_size; i++) {
    micro_val_t* micro_val = (micro_val_t*)micro_objs[i]->value;
    if (micro_val->magic[0] != micro_magic) {
      RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << thread_gid << "-" << dtx->coro_id << "-" << tx_id;
    }
    if (is_write[i]) {
      micro_val->magic[1] = micro_magic * 2;
    }
  }

  bool commit_status = dtx->TxCommit(yield);
  // TLOG(DBG, thread_gid) << "tx " << tx_id << " commit? " << commit_status;
  return commit_status;
}

bool TxReadBackup(coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  // This is used to evaluate the performance of reading backup vs. not reading backup when the write ratio is not 0
  // Remember to set remote_node_id = 0; in dtx_issue.cc to read backup for RO data
  // Use 32 threads and 8 corotines
  dtx->TxBegin(tx_id);
  bool is_write[data_set_size];
  DataItemDuplicate micro_objs[data_set_size];

  std::set<uint64_t> ids;
  std::vector<uint64_t> duplicate_item;

  for (uint64_t i = 0; i < data_set_size; i++) {
    micro_key_t micro_key;
    if (is_skewed) {
      // Skewed distribution
      micro_key.micro_id = (itemkey_t)(zipf_gen->next());
    } else {
      // Uniformed distribution
      micro_key.micro_id = (itemkey_t)FastRand(&seed) & (num_keys_global - 1);
    }

    auto ret = ids.insert(micro_key.item_key);
    if (!ret.second) {
      micro_objs[i].is_dup = true;
      continue;
    }

    assert(micro_key.item_key >= 0 && micro_key.item_key < num_keys_global);

    micro_objs[i].data_item_ptr = std::make_shared<DataItem>((table_id_t)MicroTableType::kMicroTable, micro_key.item_key);
    micro_objs[i].is_dup = false;

    if (FastRand(&seed) % 100 < write_ratio) {
      dtx->AddToReadWriteSet(micro_objs[i].data_item_ptr);
      is_write[i] = true;
    } else {
      dtx->AddToReadOnlySet(micro_objs[i].data_item_ptr);
      is_write[i] = false;
    }
  }

  if (!dtx->TxExe(yield)) {
    // TLOG(DBG, thread_gid) << "tx " << tx_id << " aborts after exe";
    return false;
  }

  for (uint64_t i = 0; i < data_set_size; i++) {
    if (micro_objs[i].is_dup) continue;
    micro_val_t* micro_val = (micro_val_t*)micro_objs[i].data_item_ptr->value;
    if (micro_val->magic[0] != micro_magic) {
      RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << thread_gid << "-" << dtx->coro_id << "-" << tx_id;
    }
    if (is_write[i]) {
      micro_val->magic[1] = micro_magic * 2;
    }
  }

  bool commit_status = dtx->TxCommit(yield);
  // TLOG(DBG, thread_gid) << "tx " << tx_id << " commit? " << commit_status;
  return commit_status;
}

bool TxReadOnly(coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  // This is used to evaluate the performance of reading backup vs. not reading backup when the write ratio is 0
  // Remember to set remote_node_id = t_id % (BACKUP_DEGREE + 1); in dtx_issue.cc to enable read from primary and backup
  dtx->TxBegin(tx_id);

  micro_key_t micro_key;
  if (is_skewed) {
    // Skewed distribution
    micro_key.micro_id = (itemkey_t)(zipf_gen->next());
  } else {
    // Uniformed distribution
    micro_key.micro_id = (itemkey_t)FastRand(&seed) & (num_keys_global - 1);
  }
  assert(micro_key.item_key >= 0 && micro_key.item_key < num_keys_global);

  DataItemPtr micro_obj = std::make_shared<DataItem>((table_id_t)MicroTableType::kMicroTable, micro_key.item_key);

  dtx->AddToReadOnlySet(micro_obj);

  if (!dtx->TxExe(yield)) {
    // TLOG(DBG, thread_gid) << "tx " << tx_id << " aborts after exe";
    return false;
  }

  bool commit_status = dtx->TxCommit(yield);
  // TLOG(DBG, thread_gid) << "tx " << tx_id << " commit? " << commit_status;
  return commit_status;
}

bool TxRFlush1(coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  dtx->TxBegin(tx_id);
  DataItemPtr micro_objs[data_set_size];

  for (uint64_t i = 0; i < data_set_size; i++) {
    micro_key_t micro_key;
    micro_key.micro_id = i;
    assert(micro_key.item_key >= 0 && micro_key.item_key < num_keys_global);
    micro_objs[i] = std::make_shared<DataItem>((table_id_t)MicroTableType::kMicroTable, micro_key.item_key);
    dtx->AddToReadWriteSet(micro_objs[i]);
  }

  if (!dtx->TxExe(yield)) {
    // TLOG(DBG, thread_gid) << "tx " << tx_id << " aborts after exe";
    return false;
  }

  for (uint64_t i = 0; i < data_set_size; i++) {
    micro_val_t* micro_val = (micro_val_t*)micro_objs[i]->value;
    if (micro_val->magic[0] != micro_magic) {
      RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << thread_gid << "-" << dtx->coro_id << "-" << tx_id;
    }
    micro_val->magic[1] = micro_magic * 2;
  }

  bool commit_status = dtx->TxCommit(yield);
  // TLOG(DBG, thread_gid) << "tx " << tx_id << " commit? " << commit_status;
  return commit_status;
}

bool TxRFlush2(coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  // Test remote flush steps:
  // 1. Turn off undo log to accurately test the perf. difference
  // 2. In Coalescent Commit. Use RDMABatchSync and RDMAReadSync for both full/batch flush
  // 3. Turn off Yield in Coalescent Commit because RDMABatchSync and RDMAReadSync already poll acks
  std::set<itemkey_t> key_set;

  dtx->TxBegin(tx_id);
  DataItemPtr micro_objs[data_set_size];

  // gen keys
  itemkey_t key = 0;
  while (key_set.size() != data_set_size) {
    if (is_skewed) {
      key = (itemkey_t)(zipf_gen->next());
    } else {
      key = (itemkey_t)FastRand(&seed) & (num_keys_global - 1);
    }
    key_set.insert(key);
  }

  int i = 0;
  for (auto it = key_set.begin(); it != key_set.end(); ++it) {
    micro_key_t micro_key;
    micro_key.micro_id = *it;
    assert(micro_key.item_key >= 0 && micro_key.item_key < num_keys_global);
    micro_objs[i] = std::make_shared<DataItem>((table_id_t)MicroTableType::kMicroTable, micro_key.item_key);
    dtx->AddToReadWriteSet(micro_objs[i]);
    i++;
  }

  if (!dtx->TxExe(yield)) {
    // TLOG(DBG, thread_gid) << "tx " << tx_id << " aborts after exe";
    return false;
  }

  for (uint64_t i = 0; i < data_set_size; i++) {
    micro_val_t* micro_val = (micro_val_t*)micro_objs[i]->value;
    if (micro_val->magic[0] != micro_magic) {
      RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << thread_gid << "-" << dtx->coro_id << "-" << tx_id;
    }
    micro_val->magic[1] = micro_magic * 2;
  }

  bool commit_status = dtx->TxCommit(yield);
  // TLOG(DBG, thread_gid) << "tx " << tx_id << " commit? " << commit_status;
  return commit_status;
}

/******************** The business logic (Transaction) end ********************/

void PollCompletion(coro_yield_t& yield) {
  while (true) {
    coro_sched->PollCompletion();
    Coroutine* next = coro_sched->coro_head->next_coro;
    if (next->coro_id != POLL_ROUTINE_ID) {
      // RDMA_LOG(DBG) << "Coro 0 yields to coro " << next->coro_id;
      coro_sched->RunCoroutine(yield, next);
    }
    if (stop_run) break;
  }
}

// Run actual transactions
void RunTx(coro_yield_t& yield, coro_id_t coro_id) {
  double total_msr_us = 0;
  // Each coroutine has a dtx: Each coroutine is a coordinator
  DTX* dtx = new DTX(meta_man, qp_man, thread_gid, coro_id, coro_sched, rdma_buffer_allocator,
                     log_offset_allocator, addr_cache);
  struct timespec tx_start_time, tx_end_time;
  bool tx_committed = false;

  // // warm up
  // RDMA_LOG(INFO) << "WARM UP...";
  // for (int i=0;i<ATTEMPED_NUM;i++) {
  //   uint64_t iter = ++tx_id_generator;
  //   TxLockContention(yield, iter, dtx);
  // }
  // RDMA_LOG(INFO) << "WARM UP finish";

  // Running transactions
  clock_gettime(CLOCK_REALTIME, &msr_start);
  while (true) {
    uint64_t iter = ++tx_id_generator;  // Global atomic transaction id
    stat_attempted_tx_total++;
    clock_gettime(CLOCK_REALTIME, &tx_start_time);
    tx_committed = TxLockContention(yield, iter, dtx);
    /********************************** Stat begin *****************************************/
    // Stat after one transaction finishes
    if (tx_committed) {
      clock_gettime(CLOCK_REALTIME, &tx_end_time);
      double tx_usec = (tx_end_time.tv_sec - tx_start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - tx_start_time.tv_nsec) / 1000;
      timer[stat_committed_tx_total++] = tx_usec;
      // latency->update(tx_usec * lat_multiplier);
      // stat_committed_tx_total++;
    }
    // Stat after a million of transactions finish
    if (stat_attempted_tx_total == ATTEMPED_NUM) {
      // A coroutine calculate the total execution time and exits
      clock_gettime(CLOCK_REALTIME, &msr_end);
      // double msr_usec = (msr_end.tv_sec - msr_start.tv_sec) * 1000000 + (double) (msr_end.tv_nsec - msr_start.tv_nsec) / 1000;
      double msr_sec = (msr_end.tv_sec - msr_start.tv_sec) + (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;

      total_msr_us = msr_sec * 1000000;

      double attemp_tput = (double)stat_attempted_tx_total / msr_sec;
      double tx_tput = (double)stat_committed_tx_total / msr_sec;

      std::string thread_num_coro_num;
      if (coro_num < 10) {
        thread_num_coro_num = std::to_string(thread_num) + "_0" + std::to_string(coro_num);
      } else {
        thread_num_coro_num = std::to_string(thread_num) + "_" + std::to_string(coro_num);
      }
      std::string log_file_path = "../../../bench_results/MICRO/" + thread_num_coro_num + "/output.txt";

      std::ofstream output_of;
      output_of.open(log_file_path, std::ios::app);

      std::sort(timer, timer + stat_committed_tx_total);
      double percentile_50 = timer[stat_committed_tx_total / 2];
      double percentile_99 = timer[stat_committed_tx_total * 99 / 100];
      mux.lock();
      tid_vec.push_back(thread_gid);
      attemp_tp_vec.push_back(attemp_tput);
      tp_vec.push_back(tx_tput);
      medianlat_vec.push_back(percentile_50);
      taillat_vec.push_back(percentile_99);
      mux.unlock();
      output_of << tx_tput << " " << percentile_50 << " " << percentile_99 << std::endl;
      output_of.close();
      // std::cout << tx_tput << " " << percentile_50 << " " << percentile_99 << std::endl;

      // Output the local addr cache miss rate
      log_file_path = "../../../bench_results/MICRO/" + thread_num_coro_num + "/miss_rate.txt";
      output_of.open(log_file_path, std::ios::app);
      output_of << double(dtx->miss_local_cache_times) / (dtx->hit_local_cache_times + dtx->miss_local_cache_times) << std::endl;
      output_of.close();

      log_file_path = "../../../bench_results/MICRO/" + thread_num_coro_num + "/cache_size.txt";
      output_of.open(log_file_path, std::ios::app);
      output_of << dtx->GetAddrCacheSize() << std::endl;
      output_of.close();

      break;
    }
  }

  std::string thread_num_coro_num;
  if (coro_num < 10) {
    thread_num_coro_num = std::to_string(thread_num) + "_0" + std::to_string(coro_num);
  } else {
    thread_num_coro_num = std::to_string(thread_num) + "_" + std::to_string(coro_num);
  }
  uint64_t total_duration = 0;
  double average_lock_duration = 0;

  // only for test
#if LOCK_WAIT

  for (auto duration : dtx->lock_durations) {
    total_duration += duration;
  }

  std::string total_lock_duration_file = "../../../bench_results/MICRO/" + thread_num_coro_num + "/total_lock_duration.txt";
  std::ofstream of;
  of.open(total_lock_duration_file, std::ios::app);
  std::sort(dtx->lock_durations.begin(), dtx->lock_durations.end());
  auto min_lock_duration = dtx->lock_durations.empty() ? 0 : dtx->lock_durations[0];
  auto max_lock_duration = dtx->lock_durations.empty() ? 0 : dtx->lock_durations[dtx->lock_durations.size() - 1];
  average_lock_duration = dtx->lock_durations.empty() ? 0 : (double)total_duration / dtx->lock_durations.size();
  lock_durations[thread_local_id] = average_lock_duration;
  of << thread_gid << " " << average_lock_duration << " " << max_lock_duration << std::endl;
  of.close();
#endif

  // only for test
#if INV_BUSY_WAIT
  total_duration = 0;
  for (auto duration : dtx->invisible_durations) {
    total_duration += duration;
  }
  std::string total_inv_duration_file = "../../../bench_results/MICRO/" + thread_num_coro_num + "/total_inv_duration.txt";
  std::ofstream ofs;
  ofs.open(total_inv_duration_file, std::ios::app);
  std::sort(dtx->invisible_durations.begin(), dtx->invisible_durations.end());
  auto min_inv_duration = dtx->invisible_durations.empty() ? 0 : dtx->invisible_durations[0];
  auto max_inv_duration = dtx->invisible_durations.empty() ? 0 : dtx->invisible_durations[dtx->invisible_durations.size() - 1];
  auto average_inv_duration = dtx->invisible_durations.empty() ? 0 : (double)total_duration / dtx->invisible_durations.size();

  double total_execution_time = 0;
  for (uint64_t i = 0; i < stat_committed_tx_total; i++) {
    total_execution_time += timer[i];
  }

  uint64_t re_read_times = 0;
  for (uint64_t i = 0; i < dtx->invisible_reread.size(); i++) {
    re_read_times += dtx->invisible_reread[i];
  }

  uint64_t avg_re_read_times = dtx->invisible_reread.empty() ? 0 : re_read_times / dtx->invisible_reread.size();

  auto average_execution_time = (total_execution_time / stat_committed_tx_total) * 1000000;  // us

  ofs << thread_gid << " " << average_inv_duration << " " << max_inv_duration << " " << average_execution_time << " " << avg_re_read_times << " " << double(total_duration / total_execution_time) << " " << (double)(total_duration / total_msr_us) << std::endl;

  ofs.close();
#endif

  /********************************** Stat end *****************************************/

  delete dtx;
}

void run_thread(struct thread_params* params) {
  stop_run = false;
  thread_gid = params->thread_global_id;
  thread_local_id = params->thread_local_id;
  thread_num = params->thread_num_per_machine;
  micro_client = params->micro_client;
  meta_man = params->global_meta_man;
  coro_num = (coro_id_t)params->coro_num;
  coro_sched = new CoroutineScheduler(thread_gid, coro_num);
  auto alloc_rdma_region_range = params->global_rdma_region->GetThreadLocalRegion(params->thread_local_id);
  addr_cache = new AddrCache();
  rdma_buffer_allocator = new RDMABufferAllocator(alloc_rdma_region_range.first, alloc_rdma_region_range.second);
  log_offset_allocator = new LogOffsetAllocator(thread_gid, params->total_thread_num);
  // latency = new Latency();
  timer = new double[ATTEMPED_NUM]();

  /* Initialize Zipf generator */
  uint64_t zipf_seed = 2 * thread_gid * GetCPUCycle();
  uint64_t zipf_seed_mask = (uint64_t(1) << 48) - 1;
  std::string config_filepath = "../../../config/micro_config.json";
  auto json_config = JsonConfig::load_file(config_filepath);
  auto micro_conf = json_config.get("micro");
  num_keys_global = align_pow2(micro_conf.get("num_keys").get_int64());
  auto zipf_theta = micro_conf.get("zipf_theta").get_double();
  is_skewed = micro_conf.get("is_skewed").get_bool();
  write_ratio = micro_conf.get("write_ratio").get_uint64();
  data_set_size = micro_conf.get("data_set_size").get_uint64();
  zipf_gen = new ZipfGen(num_keys_global, zipf_theta, zipf_seed & zipf_seed_mask);

  seed = 0xdeadbeef + thread_gid;  // Guarantee that each thread has a global different initial seed
  workgen_arr = micro_client->CreateWorkgenArray();

  // Init coroutines
  for (coro_id_t coro_i = 0; coro_i < coro_num; coro_i++) {
    coro_sched->coro_array[coro_i].coro_id = coro_i;
    // Bind workload to coroutine
    if (coro_i == POLL_ROUTINE_ID) {
      coro_sched->coro_array[coro_i].func = coro_call_t(bind(PollCompletion, _1));
    } else {
      coro_sched->coro_array[coro_i].func = coro_call_t(bind(RunTx, _1, coro_i));
    }
  }

  // Link all coroutines via pointers in a loop manner
  coro_sched->LoopLinkCoroutine(coro_num);

  // Build qp connection in thread granularity
  qp_man = new QPManager(thread_gid);
  qp_man->BuildQPConnection(meta_man);

  // Sync qp connections in one compute node before running transactions
  connected_t_num += 1;
  while (connected_t_num != thread_num) {
    usleep(2000);
  }

  // Start the first coroutine
  coro_sched->coro_array[0].func();

  // Stop running
  stop_run = true;

  // RDMA_LOG(DBG) << "Thread: " << thread_gid << ". Loop RDMA alloc times: " << rdma_buffer_allocator->loop_times;

  // Clean
  // delete latency;
  delete[] timer;
  delete addr_cache;
  delete[] workgen_arr;
  delete coro_sched;
  delete zipf_gen;
}
