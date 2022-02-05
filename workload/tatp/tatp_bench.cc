// Author: Ming Zhang
// Copyright (c) 2021

#include "tatp/tatp_bench.h"

#include <atomic>
#include <cstdio>
#include <fstream>
#include <functional>
#include <memory>

#include "allocator/buffer_allocator.h"
#include "allocator/log_allocator.h"
#include "connection/qp_manager.h"
#include "dtx/dtx.h"
// #include "util/latency.h"

using namespace std::placeholders;

// All the functions are executed in each thread

extern std::atomic<uint64_t> tx_id_generator;
extern std::atomic<uint64_t> connected_t_num;
extern std::mutex mux;

extern std::vector<t_id_t> tid_vec;
extern std::vector<double> attemp_tp_vec;
extern std::vector<double> tp_vec;
extern std::vector<double> medianlat_vec;
extern std::vector<double> taillat_vec;

__thread size_t ATTEMPTED_NUM;
__thread uint64_t seed; /* Thread-global random seed */
__thread t_id_t thread_gid;
__thread t_id_t thread_num;
__thread TATP* tatp_client;
__thread MetaManager* meta_man;
__thread QPManager* qp_man;
__thread RDMABufferAllocator* rdma_buffer_allocator;
__thread LogOffsetAllocator* log_offset_allocator;
__thread AddrCache* addr_cache;
__thread TATPTxType* workgen_arr;

__thread coro_id_t coro_num;
__thread CoroutineScheduler* coro_sched;  // Each transaction thread has a coroutine scheduler
__thread bool stop_run;

// Performance measurement (thread granularity)
__thread struct timespec msr_start, msr_end;
// __thread Latency* latency;
__thread double* timer;
// const int lat_multiplier = 10; // For sub-microsecond latency measurement
__thread uint64_t stat_attempted_tx_total = 0;  // Issued transaction number
__thread uint64_t stat_committed_tx_total = 0;  // Committed transaction number
const coro_id_t POLL_ROUTINE_ID = 0;            // The poll coroutine ID

/******************** The business logic (Transaction) start ********************/
// Read 1 SUBSCRIBER row
bool TxGetSubsciberData(coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  // RDMA_LOG(DBG) << "coro " << dtx->coro_id << " executes TxGetSubsciberData, tx_id=" << tx_id;
  dtx->TxBegin(tx_id);

  // Build key for the database record
  tatp_sub_key_t sub_key;
  sub_key.s_id = tatp_client->GetNonUniformRandomSubscriber(&seed);

  // This empty data sub_obj will be filled by RDMA reading from remote when running transaction
  auto sub_obj = std::make_shared<DataItem>((table_id_t)TATPTableType::kSubscriberTable, sub_key.item_key);

  // Add r/w set and execute transaction
  dtx->AddToReadOnlySet(sub_obj);
  if (!dtx->TxExe(yield)) return false;

  // Get value
  auto* value = (tatp_sub_val_t*)(sub_obj->value);

  // Use value
  if (value->msc_location != tatp_sub_msc_location_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << thread_gid << "-" << dtx->coro_id << "-" << tx_id;
  }

  // Commit transaction
  bool commit_status = dtx->TxCommit(yield);
  return commit_status;
}

// 1. Read 1 SPECIAL_FACILITY row
// 2. Read up to 3 CALL_FORWARDING rows
// 3. Validate up to 4 rows
bool TxGetNewDestination(coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  // RDMA_LOG(DBG) << "coro " << dtx->coro_id << " executes TxGetNewDestination";
  dtx->TxBegin(tx_id);

  uint32_t s_id = tatp_client->GetNonUniformRandomSubscriber(&seed);
  uint8_t sf_type = (FastRand(&seed) % 4) + 1;
  uint8_t start_time = (FastRand(&seed) % 3) * 8;
  uint8_t end_time = (FastRand(&seed) % 24) * 1;

  unsigned cf_to_fetch = (start_time / 8) + 1;
  assert(cf_to_fetch >= 1 && cf_to_fetch <= 3);

  /* Fetch a single special facility record */
  tatp_specfac_key_t specfac_key;

  specfac_key.s_id = s_id;
  specfac_key.sf_type = sf_type;

  auto specfac_obj =
      std::make_shared<DataItem>((table_id_t)TATPTableType::kSpecialFacilityTable, specfac_key.item_key);

  dtx->AddToReadOnlySet(specfac_obj);
  if (!dtx->TxExe(yield)) return false;

  if (specfac_obj->value_size == 0) {
    dtx->TxAbortReadOnly(yield);
    return false;
  }

  // Need to wait for reading specfac_obj from remote
  auto* specfac_val = (tatp_specfac_val_t*)(specfac_obj->value);
  if (specfac_val->data_b[0] != tatp_specfac_data_b0_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << thread_gid << "-" << dtx->coro_id << "-" << tx_id;
  }
  if (specfac_val->is_active == 0) {
    // is_active is randomly generated at pm node side
    dtx->TxAbortReadOnly(yield);
    return false;
  }

  /* Fetch possibly multiple call forwarding records. */
  DataItemPtr callfwd_obj[3];
  tatp_callfwd_key_t callfwd_key[3];

  for (unsigned i = 0; i < cf_to_fetch; i++) {
    callfwd_key[i].s_id = s_id;
    callfwd_key[i].sf_type = sf_type;
    callfwd_key[i].start_time = (i * 8);
    callfwd_obj[i] = std::make_shared<DataItem>(
        (table_id_t)TATPTableType::kCallForwardingTable,
        callfwd_key[i].item_key);
    dtx->AddToReadOnlySet(callfwd_obj[i]);
  }
  if (!dtx->TxExe(yield)) return false;

  bool callfwd_success = false;
  for (unsigned i = 0; i < cf_to_fetch; i++) {
    if (callfwd_obj[i]->value_size == 0) {
      continue;
    }

    auto* callfwd_val = (tatp_callfwd_val_t*)(callfwd_obj[i]->value);
    if (callfwd_val->numberx[0] != tatp_callfwd_numberx0_magic) {
      RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << thread_gid << "-" << dtx->coro_id << "-" << tx_id;
    }
    if (callfwd_key[i].start_time <= start_time && end_time < callfwd_val->end_time) {
      /* All conditions satisfied */
      callfwd_success = true;
    }
  }

  if (callfwd_success) {
    bool commit_status = dtx->TxCommit(yield);
    return commit_status;
  } else {
    dtx->TxAbortReadOnly(yield);
    return false;
  }
}

// Read 1 ACCESS_INFO row
bool TxGetAccessData(coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  // RDMA_LOG(DBG) << "coro " << dtx->coro_id << " executes TxGetAccessData, tx_id=" << tx_id;
  dtx->TxBegin(tx_id);

  tatp_accinf_key_t key;
  key.s_id = tatp_client->GetNonUniformRandomSubscriber(&seed);
  key.ai_type = (FastRand(&seed) & 3) + 1;

  auto acc_obj = std::make_shared<DataItem>((table_id_t)TATPTableType::kAccessInfoTable, key.item_key);

  dtx->AddToReadOnlySet(acc_obj);
  if (!dtx->TxExe(yield)) return false;

  if (acc_obj->value_size > 0) {
    /* The key was found */
    auto* value = (tatp_accinf_val_t*)(acc_obj->value);
    if (value->data1 != tatp_accinf_data1_magic) {
      RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << thread_gid << "-" << dtx->coro_id << "-" << tx_id;
    }

    bool commit_status = dtx->TxCommit(yield);
    return commit_status;
  } else {
    /* Key not found */
    dtx->TxAbortReadOnly(yield);
    return false;
  }
}

// Update 1 SUBSCRIBER row and 1 SPECIAL_FACILTY row
bool TxUpdateSubscriberData(coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  // RDMA_LOG(DBG) << "coro " << dtx->coro_id << " executes TxUpdateSubscriberData, tx_id=" << tx_id;
  dtx->TxBegin(tx_id);

  uint32_t s_id = tatp_client->GetNonUniformRandomSubscriber(&seed);
  uint8_t sf_type = (FastRand(&seed) % 4) + 1;

  /* Read + lock the subscriber record */
  tatp_sub_key_t sub_key;
  sub_key.s_id = s_id;

  auto sub_obj = std::make_shared<DataItem>((table_id_t)TATPTableType::kSubscriberTable, sub_key.item_key);
  dtx->AddToReadWriteSet(sub_obj);

  /* Read + lock the special facilty record */
  tatp_specfac_key_t specfac_key;
  specfac_key.s_id = s_id;
  specfac_key.sf_type = sf_type;

  auto specfac_obj = std::make_shared<DataItem>((table_id_t)TATPTableType::kSpecialFacilityTable, specfac_key.item_key);
  dtx->AddToReadWriteSet(specfac_obj);
  if (!dtx->TxExe(yield)) return false;

  /* If we are here, execution succeeded and we have locks */
  auto* sub_val = (tatp_sub_val_t*)(sub_obj->value);
  if (sub_val->msc_location != tatp_sub_msc_location_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << thread_gid << "-" << dtx->coro_id << "-" << tx_id;
  }
  sub_val->bits = FastRand(&seed); /* Update */

  auto* specfac_val = (tatp_specfac_val_t*)(specfac_obj->value);
  if (specfac_val->data_b[0] != tatp_specfac_data_b0_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << thread_gid << "-" << dtx->coro_id << "-" << tx_id;
  }
  specfac_val->data_a = FastRand(&seed); /* Update */

  bool commit_status = dtx->TxCommit(yield);
  return commit_status;
}

// 1. Read a SECONDARY_SUBSCRIBER row
// 2. Update a SUBSCRIBER row
bool TxUpdateLocation(coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  // RDMA_LOG(DBG) << "coro " << dtx->coro_id << " executes TxUpdateLocation, tx_id=" << tx_id;

  dtx->TxBegin(tx_id);

  uint32_t s_id = tatp_client->GetNonUniformRandomSubscriber(&seed);
  uint32_t vlr_location = FastRand(&seed);

  /* Read the secondary subscriber record */
  tatp_sec_sub_key_t sec_sub_key;
  sec_sub_key.sub_number = tatp_client->FastGetSubscribeNumFromSubscribeID(s_id);

  auto sec_sub_obj = std::make_shared<DataItem>((table_id_t)TATPTableType::kSecSubscriberTable, sec_sub_key.item_key);

  dtx->AddToReadOnlySet(sec_sub_obj);
  if (!dtx->TxExe(yield)) return false;

  auto* sec_sub_val = (tatp_sec_sub_val_t*)(sec_sub_obj->value);
  if (sec_sub_val->magic != tatp_sec_sub_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << thread_gid << "-" << dtx->coro_id << "-" << tx_id;
  }
  if (sec_sub_val->s_id != s_id) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << thread_gid << "-" << dtx->coro_id << "-" << tx_id;
  }

  tatp_sub_key_t sub_key;
  sub_key.s_id = sec_sub_val->s_id;

  auto sub_obj = std::make_shared<DataItem>((table_id_t)TATPTableType::kSubscriberTable, sub_key.item_key);

  dtx->AddToReadWriteSet(sub_obj);
  if (!dtx->TxExe(yield)) return false;

  auto* sub_val = (tatp_sub_val_t*)(sub_obj->value);
  if (sub_val->msc_location != tatp_sub_msc_location_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << thread_gid << "-" << dtx->coro_id << "-" << tx_id;
  }
  sub_val->vlr_location = vlr_location; /* Update */

  bool commit_status = dtx->TxCommit(yield);
  return commit_status;
}

// 1. Read a SECONDARY_SUBSCRIBER row
// 2. Read a SPECIAL_FACILTY row
// 3. Insert a CALL_FORWARDING row
bool TxInsertCallForwarding(coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  // RDMA_LOG(DBG) << "coro " << dtx->coro_id << " executes TxInsertCallForwarding, tx_id=" << tx_id;
  dtx->TxBegin(tx_id);

  uint32_t s_id = tatp_client->GetNonUniformRandomSubscriber(&seed);
  uint8_t sf_type = (FastRand(&seed) % 4) + 1;
  uint8_t start_time = (FastRand(&seed) % 3) * 8;
  uint8_t end_time = (FastRand(&seed) % 24) * 1;

  // Read the secondary subscriber record
  tatp_sec_sub_key_t sec_sub_key;
  sec_sub_key.sub_number = tatp_client->FastGetSubscribeNumFromSubscribeID(s_id);

  auto sec_sub_obj = std::make_shared<DataItem>((table_id_t)TATPTableType::kSecSubscriberTable, sec_sub_key.item_key);

  dtx->AddToReadOnlySet(sec_sub_obj);
  if (!dtx->TxExe(yield)) return false;

  auto* sec_sub_val = (tatp_sec_sub_val_t*)(sec_sub_obj->value);
  if (sec_sub_val->magic != tatp_sec_sub_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << thread_gid << "-" << dtx->coro_id << "-" << tx_id;
  }
  if (sec_sub_val->s_id != s_id) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << thread_gid << "-" << dtx->coro_id << "-" << tx_id;
  }

  // Read the Special Facility record
  tatp_specfac_key_t specfac_key;
  specfac_key.s_id = s_id;
  specfac_key.sf_type = sf_type;

  auto specfac_obj = std::make_shared<DataItem>((table_id_t)TATPTableType::kSpecialFacilityTable, specfac_key.item_key);

  dtx->AddToReadOnlySet(specfac_obj);
  if (!dtx->TxExe(yield)) return false;

  /* The Special Facility record exists only 62.5% of the time */
  if (specfac_obj->value_size == 0) {
    dtx->TxAbortReadOnly(yield);
    return false;
  }

  /* If we are here, the Special Facility record exists. */
  auto* specfac_val = (tatp_specfac_val_t*)(specfac_obj->value);
  if (specfac_val->data_b[0] != tatp_specfac_data_b0_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << thread_gid << "-" << dtx->coro_id << "-" << tx_id;
  }

  // Lock the Call Forwarding record
  tatp_callfwd_key_t callfwd_key;
  callfwd_key.s_id = s_id;
  callfwd_key.sf_type = sf_type;
  callfwd_key.start_time = start_time;

  auto callfwd_obj = std::make_shared<DataItem>(
      (table_id_t)TATPTableType::kCallForwardingTable,
      sizeof(tatp_callfwd_val_t),
      callfwd_key.item_key,
      tx_id,  // Using tx_id as key's version
      1);

  // Handle Insert. Only read the remote offset of callfwd_obj
  dtx->AddToReadWriteSet(callfwd_obj);
  if (!dtx->TxExe(yield)) return false;

  // Fill callfwd_val by user
  auto* callfwd_val = (tatp_callfwd_val_t*)(callfwd_obj->value);
  callfwd_val->numberx[0] = tatp_callfwd_numberx0_magic;
  callfwd_val->end_time = end_time;

  bool commit_status = dtx->TxCommit(yield);
  return commit_status;
}

// 1. Read a SECONDARY_SUBSCRIBER row
// 2. Delete a CALL_FORWARDING row
bool TxDeleteCallForwarding(coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  // RDMA_LOG(DBG) << "coro " << dtx->coro_id << " executes TxDeleteCallForwarding, tx_id=" << tx_id;
  dtx->TxBegin(tx_id);

  uint32_t s_id = tatp_client->GetNonUniformRandomSubscriber(&seed);
  uint8_t sf_type = (FastRand(&seed) % 4) + 1;
  uint8_t start_time = (FastRand(&seed) % 3) * 8;

  // Read the secondary subscriber record
  tatp_sec_sub_key_t sec_sub_key;
  sec_sub_key.sub_number = tatp_client->FastGetSubscribeNumFromSubscribeID(s_id);
  auto
      sec_sub_obj = std::make_shared<DataItem>((table_id_t)TATPTableType::kSecSubscriberTable, sec_sub_key.item_key);
  dtx->AddToReadOnlySet(sec_sub_obj);
  if (!dtx->TxExe(yield)) return false;

  auto* sec_sub_val = (tatp_sec_sub_val_t*)(sec_sub_obj->value);
  if (sec_sub_val->magic != tatp_sec_sub_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << thread_gid << "-" << dtx->coro_id << "-" << tx_id;
  }
  if (sec_sub_val->s_id != s_id) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << thread_gid << "-" << dtx->coro_id << "-" << tx_id;
  }

  // Delete the Call Forwarding record if it exists
  tatp_callfwd_key_t callfwd_key;
  callfwd_key.s_id = s_id;
  callfwd_key.sf_type = sf_type;
  callfwd_key.start_time = start_time;

  auto callfwd_obj = std::make_shared<DataItem>(
      (table_id_t)TATPTableType::kCallForwardingTable,
      callfwd_key.item_key);

  dtx->AddToReadWriteSet(callfwd_obj);
  if (!dtx->TxExe(yield)) return false;
  /*
     * Delete is handled using set is_deleted = 1, so
     * we have a finished Call Forwarding record here.
     */
  auto* callfwd_val = (tatp_callfwd_val_t*)(callfwd_obj->value);
  if (callfwd_val->numberx[0] != tatp_callfwd_numberx0_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << thread_gid << "-" << dtx->coro_id << "-" << tx_id;
  }
  callfwd_obj->valid = 0;  // 0 to indicate that this callfwd_obj will be deleted

  bool commit_status = dtx->TxCommit(yield);
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
  // Each coroutine has a dtx: Each coroutine is a coordinator
  DTX* dtx = new DTX(meta_man, qp_man, thread_gid, coro_id, coro_sched, rdma_buffer_allocator,
                     log_offset_allocator, addr_cache);
  struct timespec tx_start_time, tx_end_time;
  bool tx_committed = false;

#if 0
  int cnt = 1000;
  for (int i = 0; i < cnt; i++) {
    uint64_t iter = ++tx_id_generator; // Global atomic transaction id
    bool is_commit = TxInsertCallForwarding(yield, iter, dtx);
    //RDMA_LOG(DBG) << "[" << iter << "]>>>>>>>>>>>>>>>>>>>>>> coro " << coro_id << " TxGetSubsciberData commit? " << is_commit;
  }

  // for (int i = 0; i < cnt; i++) {
  //   uint64_t iter = ++tx_id_generator; // Global atomic transaction id
  //   bool is_commit = TxGetNewDestination(yield, iter, dtx);
  //   // RDMA_LOG(DBG) << "[" << iter << "]>>>>>>>>>>>>>>>>>>>>>> coro " << coro_id << " TxGetNewDestination commit? " << is_commit;
  // }

  // for (int i = 0; i < cnt; i++) {
  //   uint64_t iter = ++tx_id_generator; // Global atomic transaction id
  //   bool is_commit = TxGetAccessData(yield, iter, dtx);
  //   // RDMA_LOG(DBG) << "[" << iter << "]>>>>>>>>>>>>>>>>>>>>>> coro " << coro_id << " TxGetAccessData commit? " << is_commit;
  // }

  // for (int i = 0; i < cnt; i++) {
  //   uint64_t iter = ++tx_id_generator; // Global atomic transaction id
  //   bool is_commit = TxUpdateSubscriberData(yield, iter, dtx);
  //   // RDMA_LOG(DBG) << "[" << iter << "]>>>>>>>>>>>>>>>>>>>>>> coro " << coro_id << " TxUpdateSubscriberData commit? " << is_commit;
  // }
  // for (int i = 0; i < cnt; i++) {
  //   uint64_t iter = ++tx_id_generator; // Global atomic transaction id
  //   bool is_commit = TxUpdateLocation(yield, iter, dtx);
  //   // RDMA_LOG(DBG) << "[" << iter << "]>>>>>>>>>>>>>>>>>>>>>> coro " << coro_id << " TxUpdateLocation commit? " << is_commit;
  // }
  // for (int i = 0; i < cnt; i++) {
  //   uint64_t iter = ++tx_id_generator; // Global atomic transaction id
  //   bool is_commit = TxInsertCallForwarding(yield, iter, dtx);
  //   // RDMA_LOG(DBG) << "[" << iter << "]>>>>>>>>>>>>>>>>>>>>>> coro " << coro_id << " TxInsertCallForwarding commit? " << is_commit;
  // }
  // for (int i = 0; i < cnt; i++) {
  //   uint64_t iter = ++tx_id_generator; // Global atomic transaction id
  //   bool is_commit = TxDeleteCallForwarding(yield, iter, dtx);
  //   // RDMA_LOG(DBG) << "[" << iter << "]>>>>>>>>>>>>>>>>>>>>>> coro " << coro_id << " TxDeleteCallForwarding commit? " << is_commit;
  // }

  // for (int i = 0; i < 10000; i++) {
  //     uint64_t iter = ++tx_id_generator; // Global atomic transaction id
  //     bool is_commit = TxGetSubsciberData(yield, iter, dtx);
  //     // RDMA_LOG(DBG) << "["<<iter<<"]>>>>>>>>>>>>>>>>>>>>>> coro " << coro_id << "commit? " << is_commit;
  //     // RDMA_LOG(DBG) << "rmalloc_times: " << rmalloc_times << ", rfree_times: " << rfree_times << std::endl;
  // }
#else

#if ABORT_DISCARD
  // Running transactions
  clock_gettime(CLOCK_REALTIME, &msr_start);
  while (true) {
    // Guarantee that each coroutine has a different seed
    TATPTxType tx_type = workgen_arr[FastRand(&seed) % 100];
    uint64_t iter = ++tx_id_generator;  // Global atomic transaction id
    stat_attempted_tx_total++;
    clock_gettime(CLOCK_REALTIME, &tx_start_time);
    switch (tx_type) {
      case TATPTxType::kGetSubsciberData:
        tx_committed = TxGetSubsciberData(yield, iter, dtx);
        break;
      case TATPTxType::kGetNewDestination:
        tx_committed = TxGetNewDestination(yield, iter, dtx);
        break;
      case TATPTxType::kGetAccessData:
        tx_committed = TxGetAccessData(yield, iter, dtx);
        break;
      case TATPTxType::kUpdateSubscriberData:
        tx_committed = TxUpdateSubscriberData(yield, iter, dtx);
        break;
      case TATPTxType::kUpdateLocation:
        tx_committed = TxUpdateLocation(yield, iter, dtx);
        break;
      case TATPTxType::kInsertCallForwarding:
        tx_committed = TxInsertCallForwarding(yield, iter, dtx);
        break;
      case TATPTxType::kDeleteCallForwarding:
        tx_committed = TxDeleteCallForwarding(yield, iter, dtx);
        break;
      default:
        printf("Unexpected transaction type %d\n", static_cast<int>(tx_type));
        abort();
    }
#else
  switch (tx_type) {
    case TATPTxType::kGetSubsciberData: {
      do {
        clock_gettime(CLOCK_REALTIME, &tx_start_time);
        tx_committed = TxGetSubsciberData(yield, iter, dtx);
      } while (tx_committed != true);
      break;
    }
    case TATPTxType::kGetNewDestination: {
      do {
        clock_gettime(CLOCK_REALTIME, &tx_start_time);
        tx_committed = TxGetNewDestination(yield, iter, dtx);
      } while (tx_committed != true);
      break;
    }
    case TATPTxType::kGetAccessData: {
      do {
        clock_gettime(CLOCK_REALTIME, &tx_start_time);
        tx_committed = TxGetAccessData(yield, iter, dtx);
      } while (tx_committed != true);
      break;
    }
    case TATPTxType::kUpdateSubscriberData: {
      do {
        clock_gettime(CLOCK_REALTIME, &tx_start_time);
        tx_committed = TxUpdateSubscriberData(yield, iter, dtx);
      } while (tx_committed != true);
      break;
    }
    case TATPTxType::kUpdateLocation: {
      do {
        clock_gettime(CLOCK_REALTIME, &tx_start_time);
        tx_committed = TxUpdateLocation(yield, iter, dtx);
      } while (tx_committed != true);
      break;
    }
    case TATPTxType::kInsertCallForwarding: {
      do {
        clock_gettime(CLOCK_REALTIME, &tx_start_time);
        tx_committed = TxInsertCallForwarding(yield, iter, dtx);
      } while (tx_committed != true);
      break;
    }
    case TATPTxType::kDeleteCallForwarding: {
      do {
        clock_gettime(CLOCK_REALTIME, &tx_start_time);
        tx_committed = TxDeleteCallForwarding(yield, iter, dtx);
      } while (tx_committed != true);
      break;
    }
    default:
      printf("Unexpected transaction type %d\n", static_cast<int>(tx_type));
      abort();
  }
#endif
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
    if (stat_attempted_tx_total == ATTEMPTED_NUM) {
      // A coroutine calculate the total execution time and exits
      clock_gettime(CLOCK_REALTIME, &msr_end);
      // double msr_usec = (msr_end.tv_sec - msr_start.tv_sec) * 1000000 + (double) (msr_end.tv_nsec - msr_start.tv_nsec) / 1000;
      double msr_sec = (msr_end.tv_sec - msr_start.tv_sec) + (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;

      double attemp_tput = (double)stat_attempted_tx_total / msr_sec;
      double tx_tput = (double)stat_committed_tx_total / msr_sec;

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

      break;
    }
    /********************************** Stat end *****************************************/
  }

#endif

  delete dtx;
}

void run_thread(struct thread_params* params) {
  std::string config_filepath = "../../../config/tatp_config.json";
  auto json_config = JsonConfig::load_file(config_filepath);
  auto conf = json_config.get("tatp");
  ATTEMPTED_NUM = conf.get("attempted_num").get_uint64();

  stop_run = false;
  thread_gid = params->thread_global_id;
  thread_num = params->thread_num_per_machine;
  tatp_client = params->tatp_client;
  meta_man = params->global_meta_man;
  coro_num = (coro_id_t)params->coro_num;
  coro_sched = new CoroutineScheduler(thread_gid, coro_num);
  auto alloc_rdma_region_range = params->global_rdma_region->GetThreadLocalRegion(params->thread_local_id);
  addr_cache = new AddrCache();
  rdma_buffer_allocator = new RDMABufferAllocator(alloc_rdma_region_range.first, alloc_rdma_region_range.second);
  log_offset_allocator = new LogOffsetAllocator(thread_gid, params->total_thread_num);
  // latency = new Latency();
  timer = new double[ATTEMPTED_NUM]();

  seed = 0xdeadbeef + thread_gid;  // Guarantee that each thread has a global different initial seed
  workgen_arr = tatp_client->CreateWorkgenArray();

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
}
