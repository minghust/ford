// Author: Ming Zhang
// Copyright (c) 2021

#include "dtx/dtx.h"
#include "util/timer.h"

/*-------------------------------------------------------------------------------------------*/

bool DTX::CheckCasRW(std::vector<CasRead>& pending_cas_rw, std::list<HashRead>& pending_next_hash_rw, std::list<InsertOffRead>& pending_next_off_rw) {
  for (auto& re : pending_cas_rw) {
#if LOCK_WAIT
    if (*((lock_t*)re.cas_buf) != STATE_CLEAN) {
      // RDMA_LOG(DBG) << std::hex << *((lock_t*)re.cas_buf);
      // Re-read the slot until it becomes unlocked
      // FOR TEST ONLY

      auto remote_data_addr = re.item->item_ptr->remote_offset;
      auto remote_lock_addr = re.item->item_ptr->GetRemoteLockAddr(remote_data_addr);

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

      auto rc = re.qp->post_send(IBV_WR_RDMA_READ, re.data_buf, DataItemSize, remote_data_addr, IBV_SEND_SIGNALED);

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
    }
#else
    if (*((lock_t*)re.cas_buf) != STATE_CLEAN) {
      return false;
    }
#endif

    auto it = re.item->item_ptr;
    auto* fetched_item = (DataItem*)(re.data_buf);
    if (likely(fetched_item->key == it->key && fetched_item->table_id == it->table_id)) {
      if (it->user_insert) {
        // insert or update (insert an exsiting key)
        if (it->version < fetched_item->version) return false;
        old_version_for_insert.push_back(OldVersionForInsert{.table_id = it->table_id, .key = it->key, .version = fetched_item->version});
      } else {
        // Update or deletion
        if (likely(fetched_item->valid)) {
          *it = *fetched_item;  // Get old data
        } else {
          // The item is deleted before, then update the local cache
          addr_cache->Insert(re.primary_node_id, it->table_id, it->key, NOT_FOUND);
          return false;
        }
      }
      re.item->is_fetched = true;
    } else {
      // The cached address is stale

      // 1. Release lock
      *((lock_t*)re.cas_buf) = STATE_CLEAN;
      if (!coro_sched->RDMAWrite(coro_id, re.qp, re.cas_buf, it->GetRemoteLockAddr(), sizeof(lock_t))) return false;

      // 2. Read via hash
      const HashMeta& meta = global_meta_man->GetPrimaryHashMetaWithTableID(it->table_id);
      uint64_t idx = MurmurHash64A(it->key, 0xdeadbeef) % meta.bucket_num;
      offset_t node_off = idx * meta.node_size + meta.base_off;
      auto* local_hash_node = (HashNode*)thread_rdma_buffer_alloc->Alloc(sizeof(HashNode));
      if (it->user_insert) {
        pending_next_off_rw.emplace_back(InsertOffRead{.qp = re.qp, .item = re.item, .buf = (char*)local_hash_node, .remote_node = re.primary_node_id, .meta = meta, .node_off = node_off});
      } else {
        pending_next_hash_rw.emplace_back(HashRead{.qp = re.qp, .item = re.item, .buf = (char*)local_hash_node, .remote_node = re.primary_node_id, .meta = meta});
      }
      if (!coro_sched->RDMARead(coro_id, re.qp, (char*)local_hash_node, node_off, sizeof(HashNode))) return false;
    }
  }
  return true;
}

/*----------------------------------------------------------------------------------*/

int DTX::FindMatchSlot(HashRead& res, std::list<InvisibleRead>& pending_invisible_ro) {
  auto* local_hash_node = (HashNode*)res.buf;
  auto* it = res.item->item_ptr.get();
  bool find = false;

  for (auto& item : local_hash_node->data_items) {
    if (item.valid && item.key == it->key && item.table_id == it->table_id) {
      *it = item;
      addr_cache->Insert(res.remote_node, it->table_id, it->key, it->remote_offset);
      res.item->is_fetched = true;
      find = true;
      break;
    }
  }
  if (likely(find)) {
    if (unlikely((it->lock & STATE_INVISIBLE))) {
      // This item is invisible, we need re-read
      char* cas_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
      uint64_t lock_offset = it->GetRemoteLockAddr(it->remote_offset);
      pending_invisible_ro.emplace_back(InvisibleRead{.qp = res.qp, .buf = cas_buf, .off = lock_offset});
      if (!coro_sched->RDMARead(coro_id, res.qp, cas_buf, lock_offset, sizeof(lock_t))) return false;
    }
    return SLOT_FOUND;
  }
  return SLOT_NOT_FOUND;
}

bool DTX::CheckHashRW(std::vector<HashRead>& pending_hash_rw,
                      std::list<InvisibleRead>& pending_invisible_ro,
                      std::list<HashRead>& pending_next_hash_rw) {
  // Check results from hash read
  for (auto& res : pending_hash_rw) {
    auto rc = FindMatchSlot(res, pending_invisible_ro);
    if (rc == SLOT_NOT_FOUND) {
      auto* local_hash_node = (HashNode*)res.buf;
      if (local_hash_node->next == nullptr) return false;
      // Not found, we need to re-read the next bucket
      auto node_off = (uint64_t)local_hash_node->next - res.meta.data_ptr + res.meta.base_off;
      pending_next_hash_rw.emplace_back(HashRead{.qp = res.qp, .item = res.item, .buf = res.buf, .remote_node = res.remote_node, .meta = res.meta});
      if (!coro_sched->RDMARead(coro_id, res.qp, res.buf, node_off, sizeof(HashNode))) return false;
    }
  }
  return true;
}

bool DTX::CheckNextHashRW(std::list<InvisibleRead>& pending_invisible_ro,
                          std::list<HashRead>& pending_next_hash_rw) {
  for (auto iter = pending_next_hash_rw.begin(); iter != pending_next_hash_rw.end();) {
    auto res = *iter;
    auto rc = FindMatchSlot(res, pending_invisible_ro);
    if (rc == SLOT_FOUND)
      iter = pending_next_hash_rw.erase(iter);
    else if (rc == SLOT_NOT_FOUND) {
      auto* local_hash_node = (HashNode*)res.buf;
      // The item does not exist
      if (local_hash_node->next == nullptr) return false;
      // Read the next bucket
      auto node_off = (uint64_t)local_hash_node->next - res.meta.data_ptr + res.meta.base_off;
      if (!coro_sched->RDMARead(coro_id, res.qp, res.buf, node_off, sizeof(HashNode))) return false;
      iter++;
    }
  }
  return true;
}

/*------------------------------------------------------------------------------*/
int DTX::FindInsertOff(InsertOffRead& res, std::list<InvisibleRead>& pending_invisible_ro) {
  offset_t possible_insert_position = OFFSET_NOT_FOUND;
  version_t old_version;
  auto* local_hash_node = (HashNode*)res.buf;
  auto it = res.item->item_ptr;
  for (int i = 0; i < ITEM_NUM_PER_NODE; i++) {
    auto& data_item = local_hash_node->data_items[i];
    if (possible_insert_position == OFFSET_NOT_FOUND && !data_item.valid && data_item.lock == STATE_CLEAN) {
      // Within a txn, multiple items cannot insert into one slot
      std::pair<node_id_t, offset_t> new_pos(res.remote_node, res.node_off + i * DataItemSize);
      if (inserted_pos.find(new_pos) != inserted_pos.end()) {
        continue;
      } else {
        inserted_pos.insert(new_pos);
      }
      // We only need one possible empty and clean slot to insert.
      // This case is entered only once
      possible_insert_position = res.node_off + i * DataItemSize;
      old_version = data_item.version;
    } else if (data_item.valid && data_item.key == it->key && data_item.table_id == it->table_id) {
      // Find an item that matches. It is actually an update
      if (it->version < data_item.version) {
        return VERSION_TOO_OLD;
      }
      possible_insert_position = res.node_off + i * DataItemSize;
      old_version = data_item.version;
      it->lock = data_item.lock;
      break;
    }
  }
  // After searching the available insert offsets
  if (possible_insert_position != OFFSET_NOT_FOUND) {
    // There is no need to back up the old data for the first time insertion.
    // Therefore, during recovery, if there is no old backups for some data in remote memory pool,
    // it indicates that this is caused by an insertion.
    it->remote_offset = possible_insert_position;
    addr_cache->Insert(res.remote_node, it->table_id, it->key, possible_insert_position);
    old_version_for_insert.push_back(OldVersionForInsert{.table_id = it->table_id, .key = it->key, .version = old_version});
    if (unlikely((it->lock & STATE_INVISIBLE))) {
      // This item is invisible, we need re-read
      char* cas_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
      uint64_t lock_offset = it->GetRemoteLockAddr(it->remote_offset);
      pending_invisible_ro.emplace_back(InvisibleRead{.qp = res.qp, .buf = cas_buf, .off = lock_offset});
      if (!coro_sched->RDMARead(coro_id, res.qp, cas_buf, lock_offset, sizeof(lock_t))) return false;
    }
    res.item->is_fetched = true;
    return OFFSET_FOUND;
  }
  return OFFSET_NOT_FOUND;
}

bool DTX::CheckInsertOffRW(std::vector<InsertOffRead>& pending_insert_off_rw,
                           std::list<InvisibleRead>& pending_invisible_ro,
                           std::list<InsertOffRead>& pending_next_off_rw) {
  // Check results from offset read
  for (auto& res : pending_insert_off_rw) {
    auto rc = FindInsertOff(res, pending_invisible_ro);
    if (rc == VERSION_TOO_OLD)
      return false;
    else if (rc == OFFSET_NOT_FOUND) {
      auto* local_hash_node = (HashNode*)res.buf;
      if (local_hash_node->next == nullptr) return false;
      auto node_off = (uint64_t)local_hash_node->next - res.meta.data_ptr + res.meta.base_off;
      pending_next_off_rw.emplace_back(InsertOffRead{.qp = res.qp, .item = res.item, .buf = res.buf, .remote_node = res.remote_node, .meta = res.meta, .node_off = res.node_off});
      if (!coro_sched->RDMARead(coro_id, res.qp, res.buf, node_off, sizeof(HashNode))) return false;
    }
  }
  return true;
}

bool DTX::CheckNextOffRW(std::list<InvisibleRead>& pending_invisible_ro,
                         std::list<InsertOffRead>& pending_next_off_rw) {
  for (auto iter = pending_next_off_rw.begin(); iter != pending_next_off_rw.end();) {
    auto& res = *iter;
    auto rc = FindInsertOff(res, pending_invisible_ro);
    if (rc == VERSION_TOO_OLD)
      return false;
    else if (rc == OFFSET_FOUND)
      iter = pending_next_off_rw.erase(iter);
    else if (rc == OFFSET_NOT_FOUND) {
      auto* local_hash_node = (HashNode*)res.buf;
      if (local_hash_node->next == nullptr) return false;
      auto node_off = (uint64_t)local_hash_node->next - res.meta.data_ptr + res.meta.base_off;
      if (!coro_sched->RDMARead(coro_id, res.qp, res.buf, node_off, sizeof(HashNode))) return false;
      iter++;
    }
  }
  return true;
}