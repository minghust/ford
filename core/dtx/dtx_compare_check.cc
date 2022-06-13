// Author: Ming Zhang
// Copyright (c) 2022

#include "dtx/dtx.h"
#include "util/timer.h"

bool DTX::CompareCheckDirectRW(std::vector<DirectRead>& pending_direct_rw,
                               std::list<HashRead>& pending_next_hash_rw,
                               std::list<InsertOffRead>& pending_next_off_rw,
                               std::list<InvisibleRead>& pending_invisible_ro) {
  // Check results from direct read via local cache
  for (auto& res : pending_direct_rw) {
    auto* it = res.item->item_ptr.get();
    auto* fetched_item = (DataItem*)res.buf;
    if (likely(fetched_item->key == it->key && fetched_item->table_id == it->table_id)) {
#if LOCK_REFUSE_READ_RW
      if (it->lock == STATE_LOCKED) return false;
#endif
      if (it->user_insert) {
        // Inserting to an existing location is equal to an update
        // Note that it->remote_offet has already been set when reading the local addr cache before
        if (it->version < fetched_item->version) return false;
        old_version_for_insert.push_back(OldVersionForInsert{.table_id = it->table_id, .key = it->key, .version = fetched_item->version});
      } else {
        // update or delete
        if (likely(fetched_item->valid)) {
          if (tx_id < fetched_item->version) return false;
          *it = *fetched_item;
        } else {
          // The item is deleted before, then update the local cache
          addr_cache->Insert(res.remote_node, it->table_id, it->key, NOT_FOUND);
          return false;
        }
      }
      res.item->is_fetched = true;
    } else {
      // The cached address is stale. E.g., insert a new item after being deleted
      // Local cache does not have. We have to re-read via hash
      node_id_t remote_node_id = global_meta_man->GetPrimaryNodeID(it->table_id);
      const HashMeta& meta = global_meta_man->GetPrimaryHashMetaWithTableID(it->table_id);
      uint64_t idx = MurmurHash64A(it->key, 0xdeadbeef) % meta.bucket_num;
      offset_t node_off = idx * meta.node_size + meta.base_off;
      auto* local_hash_node = (HashNode*)thread_rdma_buffer_alloc->Alloc(sizeof(HashNode));
      if (it->user_insert) {
        pending_next_off_rw.emplace_back(InsertOffRead{.qp = res.qp, .item = res.item, .buf = (char*)local_hash_node, .remote_node = remote_node_id, .meta = meta, .node_off = node_off});
      } else {
        pending_next_hash_rw.emplace_back(HashRead{.qp = res.qp, .item = res.item, .buf = (char*)local_hash_node, .remote_node = remote_node_id, .meta = meta});
      }
      if (!coro_sched->RDMARead(coro_id, res.qp, (char*)local_hash_node, node_off, sizeof(HashNode))) return false;
    }
  }
  return true;
}

bool DTX::CompareCheckReadRORW(std::vector<DirectRead>& pending_direct_ro,
                               std::vector<DirectRead>& pending_direct_rw,
                               std::vector<HashRead>& pending_hash_ro,
                               std::vector<HashRead>& pending_hash_rw,
                               std::list<HashRead>& pending_next_hash_ro,
                               std::list<HashRead>& pending_next_hash_rw,
                               std::vector<InsertOffRead>& pending_insert_off_rw,
                               std::list<InsertOffRead>& pending_next_off_rw,
                               std::list<InvisibleRead>& pending_invisible_ro,
                               coro_yield_t& yield) {
  if (!CheckDirectRO(pending_direct_ro, pending_invisible_ro, pending_next_hash_ro)) return false;
  if (!CheckHashRO(pending_hash_ro, pending_invisible_ro, pending_next_hash_ro)) return false;

  if (!CompareCheckDirectRW(pending_direct_rw, pending_next_hash_rw, pending_next_off_rw, pending_invisible_ro)) return false;
  if (!CheckHashRW(pending_hash_rw, pending_invisible_ro, pending_next_hash_rw)) return false;
  if (!CheckInsertOffRW(pending_insert_off_rw, pending_invisible_ro, pending_next_off_rw)) return false;

  // During results checking, we may re-read data due to invisibility and hash collisions
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

bool DTX::CompareCheckLocking(std::vector<Lock>& pending_lock) {
  for (auto& re : pending_lock) {
#if LOCK_WAIT
    // Re-read the slot until it becomes unlocked
    // FOR TEST ONLY

    while (*((lock_t*)re.cas_buf) != STATE_CLEAN) {
      // timing
      Timer timer;
      timer.Start();

      auto rc = re.qp->post_cas(re.cas_buf, re.lock_off, STATE_CLEAN, STATE_LOCKED, IBV_SEND_SIGNALED);
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
    // Note: Now the coordinator gets the lock
    char* buf = thread_rdma_buffer_alloc->Alloc(DataItemSize);

    auto rc = re.qp->post_send(IBV_WR_RDMA_READ, buf, DataItemSize, re.item->item_ptr->remote_offset, IBV_SEND_SIGNALED);

    if (rc != SUCC) {
      TLOG(ERROR, t_id) << "client: post cas fail. rc=" << rc;
      exit(-1);
    }
    // Note: Now the coordinator gets the lock. It can read the data

    ibv_wc wc{};
    rc = re.qp->poll_till_completion(wc, no_timeout);
    if (rc != SUCC) {
      TLOG(ERROR, t_id) << "client: poll cas fail. rc=" << rc;
      exit(-1);
    }
    *re.item->item_ptr.get() = *((DataItem*)buf);

#else
    if (*((lock_t*)re.cas_buf) != STATE_CLEAN) return false;
#endif
  }
  return true;
}

bool DTX::CompareCheckValidation(std::vector<Version>& pending_version_read) {
  // Check version
  for (auto& re : pending_version_read) {
    auto it = re.item->item_ptr;
    version_t my_version = it->version;
    if (it->user_insert) {
      // If it is a insertion, we need to compare the the fetched version with
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
      // TLOG(DBG, t_id) << "my_version: " << my_version << ", remote version: " << *((version_t*)re.version_buf);
      return false;
    }
  }
  return true;
}

bool DTX::CompareCheckCommitPrimary(std::vector<Unlock>& pending_unlock) {
  for (auto& re : pending_unlock) {
    if (*((lock_t*)re.cas_buf) != STATE_LOCKED) {
      return false;
    }
  }
  return true;
}