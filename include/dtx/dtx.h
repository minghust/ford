// Author: Ming Zhang
// Copyright (c) 2021

#pragma once

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <list>
#include <queue>
#include <random>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "allocator/buffer_allocator.h"
#include "allocator/log_allocator.h"
#include "cache/addr_cache.h"
#include "common/common.h"
#include "connection/meta_manager.h"
#include "connection/qp_manager.h"
#include "dtx/doorbell.h"
#include "dtx/structs.h"
#include "memstore/hash_store.h"
#include "util/debug.h"
#include "util/hash.h"
#include "util/json_config.h"

// 0: Does not wait lock, just abort (For end-to-end tests)
// 1: wait lock until resuming execution (For lock duration tests, remember set coroutine num as 2)
#define LOCK_WAIT 0

// 0: Does not busily wait the data to be visible, e.g., yield to another coroutine to execute the next tx (For end-to-end tests)
// 1: Busily wait the data to be visible (For visibility tests, remember set coroutine num as 2)
#define INV_BUSY_WAIT 0

// 0: abort-then-retry
// 1: abort-then-next. This is the default mode in existing systems
#define ABORT_DISCARD 1

using HashNode = HashStore::HashNode;

/* One-sided RDMA-enabled distributed transaction processing */
class DTX {
 public:
  /************ Interfaces for applications ************/
  void TxBegin(tx_id_t txid);

  void AddToReadOnlySet(DataItemPtr item);

  void AddToReadWriteSet(DataItemPtr item);

  bool TxExe(coro_yield_t& yield, bool fail_abort = true);

  bool TxCommit(coro_yield_t& yield);

  /*****************************************************/

 public:
  void TxAbortReadOnly(coro_yield_t& yield);

  void TxAbortReadWrite(coro_yield_t& yield);

  void RemoveLastROItem();

 public:
  DTX(MetaManager* meta_man,
      QPManager* qp_man,
      t_id_t tid,
      coro_id_t coroid,
      CoroutineScheduler* sched,
      RDMABufferAllocator* rdma_buffer_allocator,
      LogOffsetAllocator* log_offset_allocator,
      AddrCache* addr_buf);
  ~DTX() {
    Clean();
  }

 public:
  size_t GetAddrCacheSize() {
    return addr_cache->TotalAddrSize();
  }

 private:
  // Internal transaction functions
  bool ExeRO(coro_yield_t& yield);  // Execute read-only transaction

  bool ExeRW(coro_yield_t& yield);  // Execute read-write transaction, use doorbell read+cas(lock) and background undo log

  bool Validate(coro_yield_t& yield);  // RDMA read value versions

  bool CoalescentCommit(coro_yield_t& yield);

  void Abort(coro_yield_t& yield);

  bool RDMAWriteRoundTrip(RCQP* qp, char* wt_data, uint64_t remote_offset, size_t size);  // RDMA write wrapper

  bool RDMAReadRoundTrip(RCQP* qp, char* rd_data, uint64_t remote_offset, size_t size);  // RDMA read wrapper

  node_id_t ReadWhichNode(table_id_t table_id, node_id_t& backup_idx);

  void ParallelUndoLog();

  void Clean();  // Clean data sets after commit/abort

  void DebugFetchDataItem(RCQP* qp, uint64_t remote_offset) {
    char* buf = thread_rdma_buffer_alloc->Alloc(DataItemSize);
    RDMAReadRoundTrip(qp, buf, remote_offset, DataItemSize);
    DataItem* item = (DataItem*)buf;
    item->Debug();
  }

  void DebugFetchHashBucket(RCQP* qp, uint64_t remote_offset) {
    auto* tmp_hash_node = thread_rdma_buffer_alloc->Alloc(sizeof(HashNode));
    RDMAReadRoundTrip(qp, tmp_hash_node, remote_offset, sizeof(HashNode));
    HashNode* bucket = (HashNode*)tmp_hash_node;
    for (int i = 0; i < ITEM_NUM_PER_NODE; i++) bucket->data_items[i].Debug();
  }

 private:
  // For coroutine issues RDMA requests before yield
  bool IssueReadOnly(std::vector<DirectRead>& pending_direct_ro,
                     std::vector<HashRead>& pending_hash_ro);

  bool IssueReadLock(std::vector<CasRead>& pending_cas_rw,
                     std::vector<HashRead>& pending_hash_rw,
                     std::vector<InsertOffRead>& pending_insert_off_rw);

  bool IssueValidate(std::vector<ValidateRead>& pending_validate);

  bool IssueCommitAll(std::vector<CommitWrite>& pending_commit_write, char* cas_buf);

  bool IssueCommitAllFullFlush(std::vector<CommitWrite>& pending_commit_write, char* cas_buf);

  bool IssueCommitAllSelectFlush(std::vector<CommitWrite>& pending_commit_write, char* cas_buf);

  bool IssueCommitAllBatchSelectFlush(std::vector<CommitWrite>& pending_commit_write, char* cas_buf);

 private:
  // For coroutine check RDMA requests after yield
  bool CheckReadRO(std::vector<DirectRead>& pending_direct_ro,
                   std::vector<HashRead>& pending_hash_ro,
                   std::list<InvisibleRead>& pending_invisible_ro,
                   std::list<HashRead>& pending_next_hash_ro,
                   coro_yield_t& yield);

  bool CheckReadRORW(std::vector<DirectRead>& pending_direct_ro,
                     std::vector<HashRead>& pending_hash_ro,
                     std::vector<HashRead>& pending_hash_rw,
                     std::vector<InsertOffRead>& pending_insert_off_rw,
                     std::vector<CasRead>& pending_cas_rw,
                     std::list<InvisibleRead>& pending_invisible_ro,
                     std::list<HashRead>& pending_next_hash_ro,
                     std::list<HashRead>& pending_next_hash_rw,
                     std::list<InsertOffRead>& pending_next_off_rw,
                     coro_yield_t& yield);

  bool CheckDirectRO(std::vector<DirectRead>& pending_direct_ro,
                     std::list<InvisibleRead>& pending_invisible_ro,
                     std::list<HashRead>& pending_next_hash_ro);

  bool CheckInvisibleRO(std::list<InvisibleRead>& pending_invisible_ro);

  bool CheckHashRO(std::vector<HashRead>& pending_hash_ro,
                   std::list<InvisibleRead>& pending_invisible_ro,
                   std::list<HashRead>& pending_next_hash_ro);

  bool CheckNextHashRO(std::list<InvisibleRead>& pending_invisible_ro,
                       std::list<HashRead>& pending_next_hash_ro);

  bool CheckCasRW(std::vector<CasRead>& pending_cas_rw,
                  std::list<HashRead>& pending_next_hash_rw,
                  std::list<InsertOffRead>& pending_next_off_rw);

  int FindMatchSlot(HashRead& res, std::list<InvisibleRead>& pending_invisible_ro);

  bool CheckHashRW(std::vector<HashRead>& pending_hash_rw,
                   std::list<InvisibleRead>& pending_invisible_ro,
                   std::list<HashRead>& pending_next_hash_rw);

  bool CheckNextHashRW(std::list<InvisibleRead>& pending_invisible_ro,
                       std::list<HashRead>& pending_next_hash_rw);

  int FindInsertOff(InsertOffRead& res, std::list<InvisibleRead>& pending_invisible_ro);

  bool CheckInsertOffRW(std::vector<InsertOffRead>& pending_insert_off_rw,
                        std::list<InvisibleRead>& pending_invisible_ro,
                        std::list<InsertOffRead>& pending_next_off_rw);

  bool CheckNextOffRW(std::list<InvisibleRead>& pending_invisible_ro,
                      std::list<InsertOffRead>& pending_next_off_rw);

  bool CheckValidate(std::vector<ValidateRead>& pending_validate);

  bool CheckCommitAll(std::vector<CommitWrite>& pending_commit_write, char* cas_buf);

 private:
  // For comparisons
  bool CompareExeRO(coro_yield_t& yield);

  bool CompareExeRW(coro_yield_t& yield);

  bool CompareLocking(coro_yield_t& yield);

  bool CompareValidation(coro_yield_t& yield);

  bool CompareLockingValidation(coro_yield_t& yield);

  bool CompareCommitBackup(coro_yield_t& yield);

  bool CompareCommitPrimary(coro_yield_t& yield);

 private:
  // For comparisons. Coroutine issue before yield
  bool CompareIssueReadRO(std::vector<DirectRead>& pending_direct_read,
                          std::vector<HashRead>& pending_hash_read);

  bool CompareIssueReadRW(std::vector<DirectRead>& pending_direct_read,
                          std::vector<HashRead>& pending_hash_read,
                          std::vector<InsertOffRead>& pending_insert_off_read);

  bool CompareIssueLocking(std::vector<Lock>& pending_lock);

  bool CompareIssueValidation(std::vector<Version>& pending_version_read);

  bool CompareIssueLockValidation(std::vector<ValidateRead>& pending_validate);

  bool CompareIssueCommitBackup();

  bool CompareIssueCommitBackupFullFlush();

  bool CompareIssueCommitBackupSelectiveFlush();

  bool CompareIssueCommitBackupBatchSelectFlush();

  bool CompareIssueCommitPrimary(std::vector<Unlock>& pending_unlock);

  bool CompareIssueTruncate();

 private:
  // For comparisons. Coroutine check after yield
  bool CompareCheckDirectRead(std::vector<DirectRead>& pending_direct_read, std::list<HashRead>& pending_next_hash_read, std::list<InsertOffRead>& pending_next_off_read);

  int CompareFindMatchSlot(HashRead& res);

  bool CompareCheckHashRead(std::vector<HashRead>& pending_hash_read, std::list<HashRead>& pending_next_hash_read);

  bool CompareCheckNextHashRead(std::list<HashRead>& pending_next_hash_read);

  int CompareFindInsertOff(InsertOffRead& res);

  bool CompareCheckInsertOffRead(std::vector<InsertOffRead>& pending_insert_off_read, std::list<InsertOffRead>& pending_next_off_read);

  bool CompareCheckNextOffRead(std::list<InsertOffRead>& pending_next_off_read);

  bool CompareCheckReadRO(std::vector<DirectRead>& pending_direct_read,
                          std::vector<HashRead>& pending_hash_read,
                          std::list<HashRead>& pending_next_hash_read,
                          coro_yield_t& yield);

  bool CompareCheckReadRORW(std::vector<DirectRead>& pending_direct_read,
                            std::vector<HashRead>& pending_hash_read,
                            std::vector<InsertOffRead>& pending_insert_off_read,
                            std::list<HashRead>& pending_next_hash_read,
                            std::list<InsertOffRead>& pending_next_off_read,
                            coro_yield_t& yield);

  bool CompareCheckLocking(std::vector<Lock>& pending_lock);

  bool CompareCheckValidation(std::vector<Version>& pending_version_read);

  bool CompareCheckCommitPrimary(std::vector<Unlock>& pending_unlock);

  bool CompareTruncateAsync(coro_yield_t& yield);

 public:
  tx_id_t tx_id;  // Transaction ID

  t_id_t t_id;  // Thread ID

  coro_id_t coro_id;  // Coroutine ID

 public:
  // For statistics
  std::vector<uint64_t> lock_durations;  // us

  std::vector<uint64_t> invisible_durations;  // us

  std::vector<uint64_t> invisible_reread;  // times

  size_t hit_local_cache_times;

  size_t miss_local_cache_times;

  MetaManager* global_meta_man;  // Global metadata manager

 private:
  CoroutineScheduler* coro_sched;  // Thread local coroutine scheduler

  QPManager* thread_qp_man;  // Thread local qp connection manager. Each transaction thread has one

  RDMABufferAllocator* thread_rdma_buffer_alloc;  // Thread local RDMA buffer allocator

  LogOffsetAllocator* thread_remote_log_offset_alloc;  // Thread local remote log offset generator

  bool is_ro_tx;  // Is read-only transaction

  TXStatus tx_status;

  std::vector<DataSetItem> read_only_set;

  std::vector<DataSetItem> read_write_set;

  std::vector<size_t> not_eager_locked_rw_set;  // For eager logging

  std::vector<size_t> locked_rw_set;  // For release lock during abort

  AddrCache* addr_cache;

  // For backup-enabled read. Which backup is selected (the backup index, not the backup's machine id)
  size_t select_backup;

  // For validate the version for insertion
  std::vector<OldVersionForInsert> old_version_for_insert;

  struct pair_hash {
    inline std::size_t operator()(const std::pair<node_id_t, offset_t>& v) const {
      return v.first * 31 + v.second;
    }
  };

  // Avoid inserting to the same slot in one transaction
  std::unordered_set<std::pair<node_id_t, offset_t>, pair_hash> inserted_pos;
};

/*************************************************************
  ************************************************************
  *********** Implementations of interfaces in DTX ***********
  ************************************************************
**************************************************************/

ALWAYS_INLINE
void DTX::TxBegin(tx_id_t txid) {
  Clean();  // Clean the last transaction states
  is_ro_tx = true;
  tx_id = txid;
}

ALWAYS_INLINE
void DTX::AddToReadOnlySet(DataItemPtr item) {
  DataSetItem data_set_item{.item_ptr = std::move(item), .is_fetched = false, .is_logged = false, .read_which_node = -1};
  read_only_set.emplace_back(data_set_item);
}

ALWAYS_INLINE
void DTX::AddToReadWriteSet(DataItemPtr item) {
  DataSetItem data_set_item{.item_ptr = std::move(item), .is_fetched = false, .is_logged = false, .read_which_node = -1};
  read_write_set.emplace_back(data_set_item);
}

ALWAYS_INLINE
bool DTX::TxExe(coro_yield_t& yield, bool fail_abort) {
  // Start executing transaction
  tx_status = TXStatus::TX_EXE;
  if (read_write_set.empty() && read_only_set.empty()) {
    return true;
  }

  if (read_write_set.empty()) {
    if (ExeRO(yield))
      return true;
    else {
      // TLOG(DBG, t_id) << "ExeRO false";
      goto ABORT;
    }
  } else {
    if (ExeRW(yield))
      return true;
    else {
      // TLOG(DBG, t_id) << "ExeRW false";
      goto ABORT;
    }
  }
  return true;

ABORT:
  if (fail_abort) Abort(yield);
  return false;
}

ALWAYS_INLINE
bool DTX::TxCommit(coro_yield_t& yield) {
  // Only read one item
  if (is_ro_tx && read_only_set.size() == 1) {
    return true;
  }
  if (!Validate(yield)) {
    // TLOG(DBG, t_id) << "Validate false";
    goto ABORT;
  }

  // Next step. If read-write txns, we need to commit the updates to remote replicas
  if (!is_ro_tx) {
    // Write back for read-write tx
    bool commit_stat;
    commit_stat = CoalescentCommit(yield);
    if (commit_stat) {
      return true;
    } else {
      // RDMA_LOG(FATAL) << "Thread " << t_id << " , Coroutine " << coro_id << " abort txn in CoalescentCommit";
      goto ABORT;
    }
  }

  return true;

ABORT:
  Abort(yield);
  return false;
}

ALWAYS_INLINE
void DTX::TxAbortReadOnly(coro_yield_t& yield) {
  // Application actively aborts the tx
  // User abort tx in the middle time of tx exe
  assert(read_write_set.empty());
  read_only_set.clear();
}

ALWAYS_INLINE
void DTX::TxAbortReadWrite(coro_yield_t& yield) { Abort(yield); }

ALWAYS_INLINE
void DTX::RemoveLastROItem() { read_only_set.pop_back(); }

// RDMA write `wt_data' with size `size' to remote
ALWAYS_INLINE
bool DTX::RDMAWriteRoundTrip(RCQP* qp, char* wt_data,
                             uint64_t remote_offset,
                             size_t size) {
  // ***ONLY FOR DEBUG***
  auto rc = qp->post_send(IBV_WR_RDMA_WRITE, wt_data, size, remote_offset, 0);
  if (rc != SUCC) {
    TLOG(ERROR, t_id) << "client: post write fail. rc=" << rc;
    return false;
  }
  // wait finish
  sleep(1);
  // ibv_wc wc{};
  // rc = qp->poll_till_completion(wc, no_timeout);
  // if (rc != SUCC) {
  //   TLOG(ERROR, t_id) << "client: poll write fail. rc=" << rc;
  // }
  return true;
}

// RDMA read value with size `size` from remote mr at offset `remote_offset` to
// rd_data
ALWAYS_INLINE
bool DTX::RDMAReadRoundTrip(RCQP* qp, char* rd_data,
                            uint64_t remote_offset, size_t size) {
  // ***ONLY FOR DEBUG***
  auto rc = qp->post_send(IBV_WR_RDMA_READ, rd_data, size, remote_offset, 0);
  if (rc != SUCC) {
    TLOG(ERROR, t_id) << "client: post read fail. rc=" << rc;
    return false;
  }
  // wait finish
  usleep(20);
  // ibv_wc wc{};
  // rc = qp->poll_till_completion(wc, no_timeout);
  // // then get the results, stored in the local_buffer
  // if (rc != SUCC) {
  //   TLOG(ERROR, t_id) << "client: poll read fail. rc=" << rc;
  //   return false;
  // }
  return true;
}

ALWAYS_INLINE
node_id_t DTX::ReadWhichNode(table_id_t table_id, node_id_t& backup_idx) {
  node_id_t remote_node_id;
  if (global_meta_man->enable_read_backup) {
    // Ideally, we want all backup machines can share the loads. However, in fact,
    // accessing a new backup will lose the remote address, which may decrease the performance
    // So, it may be a more efficient way to fix some backups to read.
    auto* remote_backup_nodes = global_meta_man->GetBackupNodeID(table_id);
    // backup_idx = select_backup;
    // select_backup = (select_backup + 1) % remote_backup_nodes->size();
    remote_node_id = remote_backup_nodes->at(0);
  } else {
    remote_node_id = global_meta_man->GetPrimaryNodeID(table_id);
  }
  return remote_node_id;
}

ALWAYS_INLINE
void DTX::Clean() {
  read_only_set.clear();
  read_write_set.clear();
  not_eager_locked_rw_set.clear();
  locked_rw_set.clear();
  old_version_for_insert.clear();
  inserted_pos.clear();
}
