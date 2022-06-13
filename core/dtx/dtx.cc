// Author: Ming Zhang
// Copyright (c) 2022

#include "dtx/dtx.h"

DTX::DTX(MetaManager* meta_man,
         QPManager* qp_man,
         VersionCache* status,
         LockCache* lock_table,
         t_id_t tid,
         coro_id_t coroid,
         CoroutineScheduler* sched,
         RDMABufferAllocator* rdma_buffer_allocator,
         LogOffsetAllocator* remote_log_offset_allocator,
         AddrCache* addr_buf) {
  // Transaction setup
  tx_id = 0;
  t_id = tid;
  coro_id = coroid;
  coro_sched = sched;
  global_meta_man = meta_man;
  thread_qp_man = qp_man;
  global_vcache = status;
  global_lcache = lock_table;
  thread_rdma_buffer_alloc = rdma_buffer_allocator;
  tx_status = TXStatus::TX_INIT;

  select_backup = 0;
  thread_remote_log_offset_alloc = remote_log_offset_allocator;
  addr_cache = addr_buf;

  hit_local_cache_times = 0;
  miss_local_cache_times = 0;
}

bool DTX::ExeRO(coro_yield_t& yield) {
  // You can read from primary or backup
  std::vector<DirectRead> pending_direct_ro;
  std::vector<HashRead> pending_hash_ro;

  // Issue reads
  // RDMA_LOG(DBG) << "coro: " << coro_id << " tx_id: " << tx_id << " issue read ro";
  if (!IssueReadRO(pending_direct_ro, pending_hash_ro)) return false;

  // Yield to other coroutines when waiting for network replies
  coro_sched->Yield(yield, coro_id);

  // Receive data
  std::list<InvisibleRead> pending_invisible_ro;
  std::list<HashRead> pending_next_hash_ro;
  // RDMA_LOG(DBG) << "coro: " << coro_id << " tx_id: " << tx_id << " check read ro";
  auto res = CheckReadRO(pending_direct_ro, pending_hash_ro, pending_invisible_ro, pending_next_hash_ro, yield);
  return res;
}

bool DTX::ExeRW(coro_yield_t& yield) {
  // For read-only data from primary or backup
  std::vector<DirectRead> pending_direct_ro;
  std::vector<HashRead> pending_hash_ro;

  // For read-write data from primary
  std::vector<CasRead> pending_cas_rw;
  std::vector<DirectRead> pending_direct_rw;
  std::vector<HashRead> pending_hash_rw;
  std::vector<InsertOffRead> pending_insert_off_rw;

  std::list<InvisibleRead> pending_invisible_ro;

  std::list<HashRead> pending_next_hash_ro;
  std::list<HashRead> pending_next_hash_rw;
  std::list<InsertOffRead> pending_next_off_rw;

  if (!IssueReadRO(pending_direct_ro, pending_hash_ro)) return false;  // RW transactions may also have RO data
// RDMA_LOG(DBG) << "coro: " << coro_id << " tx_id: " << tx_id << " issue read rorw";
#if READ_LOCK
  if (!IssueReadLock(pending_cas_rw, pending_hash_rw, pending_insert_off_rw)) return false;
#else
  if (!IssueReadRW(pending_direct_rw, pending_hash_rw, pending_insert_off_rw)) return false;
#endif

  // Yield to other coroutines when waiting for network replies
  coro_sched->Yield(yield, coro_id);

  // RDMA_LOG(DBG) << "coro: " << coro_id << " tx_id: " << tx_id << " check read rorw";
  bool res = false;
#if READ_LOCK
  res = CheckReadRORW(pending_direct_ro,
                      pending_hash_ro,
                      pending_hash_rw,
                      pending_insert_off_rw,
                      pending_cas_rw,
                      pending_invisible_ro,
                      pending_next_hash_ro,
                      pending_next_hash_rw,
                      pending_next_off_rw,
                      yield);
#else
  res = CompareCheckReadRORW(pending_direct_ro,
                             pending_direct_rw,
                             pending_hash_ro,
                             pending_hash_rw,
                             pending_next_hash_ro,
                             pending_next_hash_rw,
                             pending_insert_off_rw,
                             pending_next_off_rw,
                             pending_invisible_ro,
                             yield);
#endif

#if COMMIT_TOGETHER
  ParallelUndoLog();
#endif

  return res;
}

bool DTX::Validate(coro_yield_t& yield) {
  // The transaction is read-write, and all the written data have been locked before
  if (not_eager_locked_rw_set.empty() && read_only_set.empty()) {
    // TLOG(DBG, t_id) << "save validation";
    return true;
  }

  std::vector<ValidateRead> pending_validate;

#if LOCAL_VALIDATION
  ValStatus ret = IssueLocalValidate(pending_validate);

  if (ret == ValStatus::NO_NEED_VAL) {
    return true;
  } else if (ret == ValStatus::RDMA_ERROR || ret == ValStatus::MUST_ABORT) {
    return false;
  }
#else
  if (!IssueRemoteValidate(pending_validate)) return false;
#endif

  // Yield to other coroutines when waiting for network replies
  coro_sched->Yield(yield, coro_id);

  auto res = CheckValidate(pending_validate);
  return res;
}

// Invisible + write primary and backups
bool DTX::CoalescentCommit(coro_yield_t& yield) {
  tx_status = TXStatus::TX_COMMIT;
  char* cas_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
#if LOCAL_LOCK
  *(lock_t*)cas_buf = STATE_INVISIBLE;
#else
  *(lock_t*)cas_buf = STATE_LOCKED | STATE_INVISIBLE;
#endif

  std::vector<CommitWrite> pending_commit_write;

  // Check whether all the log ACKs have returned
  while (!coro_sched->CheckLogAck(coro_id)) {
    ;  // wait
  }

#if RFLUSH == 0
  if (!IssueCommitAll(pending_commit_write, cas_buf)) return false;
#elif RFLUSH == 1
  if (!IssueCommitAllFullFlush(pending_commit_write, cas_buf)) return false;
#elif RFLUSH == 2
  if (!IssueCommitAllSelectFlush(pending_commit_write, cas_buf)) return false;
#endif

  coro_sched->Yield(yield, coro_id);

  *((lock_t*)cas_buf) = 0;

  auto res = CheckCommitAll(pending_commit_write, cas_buf);

  return res;
}

void DTX::ParallelUndoLog() {
  // Write the old data from read write set
  size_t log_size = sizeof(tx_id) + sizeof(t_id);
  for (auto& set_it : read_write_set) {
    if (!set_it.is_logged && !set_it.item_ptr->user_insert) {
      // For the newly inserted data, the old data are not needed to be recorded
      log_size += DataItemSize;
    }
  }
  char* written_log_buf = thread_rdma_buffer_alloc->Alloc(log_size);

  offset_t cur = 0;
  *((tx_id_t*)(written_log_buf + cur)) = tx_id;
  cur += sizeof(tx_id);
  *((t_id_t*)(written_log_buf + cur)) = t_id;
  cur += sizeof(t_id);

  for (auto& set_it : read_write_set) {
    if (!set_it.is_logged && !set_it.item_ptr->user_insert) {
      memcpy(written_log_buf + cur, (char*)(set_it.item_ptr.get()), DataItemSize);
      cur += DataItemSize;
      set_it.is_logged = true;
    }
  }

  // Write undo logs to all memory nodes
  for (int i = 0; i < global_meta_man->remote_nodes.size(); i++) {
    offset_t log_offset = thread_remote_log_offset_alloc->GetNextLogOffset(i, log_size);
    RCQP* qp = thread_qp_man->GetRemoteLogQPWithNodeID(i);
    coro_sched->RDMALog(coro_id, tx_id, qp, written_log_buf, log_offset, log_size);
  }
}

void DTX::Abort() {
  // When failures occur, transactions need to be aborted.
  // In general, the transaction will not abort during committing replicas if no hardware failure occurs
  char* unlock_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
  *((lock_t*)unlock_buf) = 0;
  for (auto& index : locked_rw_set) {
    auto& it = read_write_set[index].item_ptr;
    node_id_t primary_node_id = global_meta_man->GetPrimaryNodeID(it->table_id);
    RCQP* primary_qp = thread_qp_man->GetRemoteDataQPWithNodeID(primary_node_id);
    auto rc = primary_qp->post_send(IBV_WR_RDMA_WRITE, unlock_buf, sizeof(lock_t), it->GetRemoteLockAddr(), 0);
    if (rc != SUCC) {
      RDMA_LOG(FATAL) << "Thread " << t_id << " , Coroutine " << coro_id << " unlock fails during abortion";
    }
  }
  tx_status = TXStatus::TX_ABORT;
}