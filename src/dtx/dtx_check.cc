// Author: Ming Zhang
// Copyright (c) 2021

#include "dtx/dtx.h"
#include "util/timer.h"

bool DTX::CheckReadRO(std::vector<DirectRead>& pending_direct_ro,
                      std::vector<HashRead>& pending_hash_ro,
                      std::list<InvisibleRead>& pending_invisible_ro,
                      std::list<HashRead>& pending_next_hash_ro,
                      coro_yield_t& yield) {
  if (!CheckDirectRO(pending_direct_ro, pending_invisible_ro, pending_next_hash_ro)) return false;
  if (!CheckHashRO(pending_hash_ro, pending_invisible_ro, pending_next_hash_ro)) return false;
  // During results checking, we may re-read data due to invisibility and hash collisions
  // if (!pending_invisible_ro.empty()) {
  //   coro_sched->Yield(yield, coro_id);
  // }

  while (!pending_invisible_ro.empty() || !pending_next_hash_ro.empty()) {
    coro_sched->Yield(yield, coro_id);
    if (!CheckInvisibleRO(pending_invisible_ro)) return false;
    if (!CheckNextHashRO(pending_invisible_ro, pending_next_hash_ro)) return false;
  }
  return true;
}

bool DTX::CheckReadRORW(std::vector<DirectRead>& pending_direct_ro,
                        std::vector<HashRead>& pending_hash_ro,
                        std::vector<HashRead>& pending_hash_rw,
                        std::vector<InsertOffRead>& pending_insert_off_rw,
                        std::vector<CasRead>& pending_cas_rw,
                        std::list<InvisibleRead>& pending_invisible_ro,
                        std::list<HashRead>& pending_next_hash_ro,
                        std::list<HashRead>& pending_next_hash_rw,
                        std::list<InsertOffRead>& pending_next_off_rw,
                        coro_yield_t& yield) {
  // check read-only results
  if (!CheckDirectRO(pending_direct_ro, pending_invisible_ro, pending_next_hash_ro)) return false;
  if (!CheckHashRO(pending_hash_ro, pending_invisible_ro, pending_next_hash_ro)) return false;
  // The reason to use separate CheckHashRO and CheckHashRW: UndoLog is needed in CheckHashRW
  // check read-write results
  if (!CheckHashRW(pending_hash_rw, pending_invisible_ro, pending_next_hash_rw)) return false;
  if (!CheckInsertOffRW(pending_insert_off_rw, pending_invisible_ro, pending_next_off_rw)) return false;
  if (!CheckCasRW(pending_cas_rw, pending_next_hash_rw, pending_next_off_rw)) return false;
  // During results checking, we may re-read data due to invisibility and hash collisions
  // if (!pending_invisible_ro.empty()) {
  //   coro_sched->Yield(yield, coro_id);
  // }

  while (!pending_invisible_ro.empty() || !pending_next_hash_ro.empty() || !pending_next_hash_rw.empty() || !pending_next_off_rw.empty()) {
    coro_sched->Yield(yield, coro_id);

    // Recheck read-only replies
    if (!CheckInvisibleRO(pending_invisible_ro)) return false;
    if (!CheckNextHashRO(pending_invisible_ro, pending_next_hash_ro)) return false;

    // Recheck read-write replies
    if (!CheckNextHashRW(pending_invisible_ro, pending_next_hash_rw)) return false;
    if (!CheckNextOffRW(pending_invisible_ro, pending_next_off_rw)) return false;
  }
  return true;
}

bool DTX::CheckValidate(std::vector<ValidateRead>& pending_validate) {
  // Check version
  for (auto& re : pending_validate) {
    auto it = re.item->item_ptr;
    if (re.has_lock_in_validate) {
#if LOCK_WAIT
      if (*((lock_t*)re.cas_buf) != STATE_CLEAN) {
        // Re-read the slot until it becomes unlocked
        // FOR TEST ONLY

        auto remote_data_addr = re.item->item_ptr->remote_offset;
        auto remote_lock_addr = re.item->item_ptr->GetRemoteLockAddr(remote_data_addr);
        auto remote_version_addr = re.item->item_ptr->GetRemoteVersionAddr(remote_data_addr);

        while (*((lock_t*)re.cas_buf) != STATE_CLEAN) {
          // timing
          Timer timer;
          timer.Start();

          auto rc = re.qp->post_cas(re.cas_buf, remote_lock_addr, STATE_CLEAN, STATE_LOCKED, IBV_SEND_SIGNALED);
          if (rc != SUCC) {
            TLOG(ERROR, t_id) << "client: post cas fail. rc=" << rc;
            exit(-1);
          }

          ibv_wc wc{};
          rc = re.qp->poll_till_completion(wc, no_timeout);
          if (rc != SUCC) {
            TLOG(ERROR, t_id) << "client: poll cas fail. rc=" << rc;
            exit(-1);
          }

          timer.Stop();
          lock_durations.emplace_back(timer.Duration_us());
        }

        auto rc = re.qp->post_send(IBV_WR_RDMA_READ, re.version_buf, sizeof(version_t), remote_version_addr, IBV_SEND_SIGNALED);

        if (rc != SUCC) {
          TLOG(ERROR, t_id) << "client: post read fail. rc=" << rc;
          exit(-1);
        }
        // Note: Now the coordinator gets the lock. It can read the data

        ibv_wc wc{};
        rc = re.qp->poll_till_completion(wc, no_timeout);
        if (rc != SUCC) {
          TLOG(ERROR, t_id) << "client: poll read fail. rc=" << rc;
          exit(-1);
        }
      }
#else
      if (*((lock_t*)re.cas_buf) != STATE_CLEAN) {
        // it->Debug();
        // RDMA_LOG(DBG) << "remote lock not clean " << std::hex << *((lock_t*)re.cas_buf);
        return false;
      }
#endif
      version_t my_version = it->version;
      if (it->user_insert) {
        // If it is an insertion, we need to compare the the fetched version with
        // the old version, instead of the new version stored in item
        for (auto& old_version : old_version_for_insert) {
          if (old_version.table_id == it->table_id && old_version.key == it->key) {
            my_version = old_version.version;
            break;
          }
        }
      }
      // Compare version
      if (my_version != *((version_t*)re.version_buf)) {
        // it->Debug();
        // RDMA_LOG(DBG) << "MY VERSION " << it->version;
        // RDMA_LOG(DBG) << "version_buf " << *((version_t*)re.version_buf);
        return false;
      }
    } else {
      // Compare version
      if (it->version != *((version_t*)re.version_buf)) {
        // it->Debug();
        // RDMA_LOG(DBG) << "MY VERSION " << it->version;
        // RDMA_LOG(DBG) << "version_buf " << *((version_t*)re.version_buf);
        return false;
      }
    }
  }
  return true;
}

bool DTX::CheckCommitAll(std::vector<CommitWrite>& pending_commit_write, char* cas_buf) {
  // Release: set visible and unlock remote data
  for (auto& re : pending_commit_write) {
    auto* qp = thread_qp_man->GetRemoteDataQPWithNodeID(re.node_id);
    qp->post_send(IBV_WR_RDMA_WRITE, cas_buf, sizeof(lock_t), re.lock_off, 0);  // Release
  }
  return true;
}