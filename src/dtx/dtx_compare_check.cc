// Author: Ming Zhang
// Copyright (c) 2021

#include "dtx/dtx.h"
#include "util/timer.h"

bool DTX::CompareCheckDirectRead(std::vector<DirectRead>& pending_direct_read, std::list<HashRead>& pending_next_hash_read, std::list<InsertOffRead>& pending_next_off_read) {
  // Check results from direct read via local cache
  for (auto& res : pending_direct_read) {
    auto* it = res.item->item_ptr.get();
    auto* fetched_item = (DataItem*)res.buf;
    if (likely(fetched_item->key == it->key && fetched_item->table_id == it->table_id)) {
      if (it->user_insert) {
        if (it->version < fetched_item->version) return false;
        old_version_for_insert.push_back(OldVersionForInsert{.table_id = it->table_id, .key = it->key, .version = fetched_item->version});
      } else {
        // read, update, delete
        if (likely(fetched_item->valid)) {
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
        pending_next_off_read.emplace_back(InsertOffRead{.qp = res.qp, .item = res.item, .buf = (char*)local_hash_node, .remote_node = remote_node_id, .meta = meta, .node_off = node_off});
      } else {
        pending_next_hash_read.emplace_back(HashRead{.qp = res.qp, .item = res.item, .buf = (char*)local_hash_node, .remote_node = remote_node_id, .meta = meta});
      }
      if (!coro_sched->RDMARead(coro_id, res.qp, (char*)local_hash_node, node_off, sizeof(HashNode))) return false;
    }
  }
  return true;
}

int DTX::CompareFindMatchSlot(HashRead& res) {
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
    return SLOT_FOUND;
  }
  return SLOT_NOT_FOUND;
}

bool DTX::CompareCheckHashRead(std::vector<HashRead>& pending_hash_read, std::list<HashRead>& pending_next_hash_read) {
  // Check results from hash read
  for (auto& res : pending_hash_read) {
    auto rc = CompareFindMatchSlot(res);
    if (rc == SLOT_NOT_FOUND) {
      auto* local_hash_node = (HashNode*)res.buf;
      if (local_hash_node->next == nullptr) return false;
      // Not found, we need to re-read the next bucket
      auto node_off = (uint64_t)local_hash_node->next - res.meta.data_ptr + res.meta.base_off;
      pending_next_hash_read.emplace_back(HashRead{.qp = res.qp, .item = res.item, .buf = res.buf, .remote_node = res.remote_node, .meta = res.meta});
      if (!coro_sched->RDMARead(coro_id, res.qp, res.buf, node_off, sizeof(HashNode))) return false;
    }
  }
  return true;
}

bool DTX::CompareCheckNextHashRead(std::list<HashRead>& pending_next_hash_read) {
  for (auto iter = pending_next_hash_read.begin(); iter != pending_next_hash_read.end();) {
    auto res = *iter;
    auto rc = CompareFindMatchSlot(res);
    if (rc == SLOT_FOUND) {
      iter = pending_next_hash_read.erase(iter);
    } else if (rc == SLOT_NOT_FOUND) {
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

int DTX::CompareFindInsertOff(InsertOffRead& res) {
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
      if (it->version < data_item.version) return VERSION_TOO_OLD;
      possible_insert_position = res.node_off + i * DataItemSize;
      old_version = data_item.version;
      it->lock = data_item.lock;
      break;
    }
  }
  // After searching the available insert offsets
  if (possible_insert_position != OFFSET_NOT_FOUND) {
    it->remote_offset = possible_insert_position;
    addr_cache->Insert(res.remote_node, it->table_id, it->key, possible_insert_position);
    old_version_for_insert.push_back(OldVersionForInsert{.table_id = it->table_id, .key = it->key, .version = old_version});
    res.item->is_fetched = true;
    return OFFSET_FOUND;
  }
  return OFFSET_NOT_FOUND;
}

bool DTX::CompareCheckInsertOffRead(std::vector<InsertOffRead>& pending_insert_off_read, std::list<InsertOffRead>& pending_next_off_read) {
  // Check results from offset read
  for (auto& res : pending_insert_off_read) {
    auto rc = CompareFindInsertOff(res);
    if (rc == VERSION_TOO_OLD) {
      return false;
    } else if (rc == OFFSET_NOT_FOUND) {
      auto* local_hash_node = (HashNode*)res.buf;
      if (local_hash_node->next == nullptr) return false;
      auto node_off = (uint64_t)local_hash_node->next - res.meta.data_ptr + res.meta.base_off;
      pending_next_off_read.emplace_back(InsertOffRead{.qp = res.qp, .item = res.item, .buf = res.buf, .remote_node = res.remote_node, .meta = res.meta, .node_off = res.node_off});
      if (!coro_sched->RDMARead(coro_id, res.qp, res.buf, node_off, sizeof(HashNode))) return false;
    }
  }
  return true;
}

bool DTX::CompareCheckNextOffRead(std::list<InsertOffRead>& pending_next_off_read) {
  for (auto iter = pending_next_off_read.begin(); iter != pending_next_off_read.end();) {
    auto& res = *iter;
    auto rc = CompareFindInsertOff(res);
    if (rc == VERSION_TOO_OLD) {
      return false;
    } else if (rc == OFFSET_FOUND) {
      iter = pending_next_off_read.erase(iter);
    } else if (rc == OFFSET_NOT_FOUND) {
      auto* local_hash_node = (HashNode*)res.buf;
      if (local_hash_node->next == nullptr) return false;
      auto node_off = (uint64_t)local_hash_node->next - res.meta.data_ptr + res.meta.base_off;
      if (!coro_sched->RDMARead(coro_id, res.qp, res.buf, node_off, sizeof(HashNode))) return false;
      iter++;
    }
  }
  return true;
}

bool DTX::CompareCheckReadRO(std::vector<DirectRead>& pending_direct_read,
                             std::vector<HashRead>& pending_hash_read,
                             std::list<HashRead>& pending_next_hash_read,
                             coro_yield_t& yield) {
  std::list<InsertOffRead> pending_next_off_read;  // No use
  if (!CompareCheckDirectRead(pending_direct_read, pending_next_hash_read, pending_next_off_read)) return false;
  if (!CompareCheckHashRead(pending_hash_read, pending_next_hash_read)) return false;
  // Remain some hash buckets need read
  while (!pending_next_hash_read.empty()) {
    coro_sched->Yield(yield, coro_id);
    if (!CompareCheckNextHashRead(pending_next_hash_read)) return false;
  }
  return true;
}

bool DTX::CompareCheckReadRORW(std::vector<DirectRead>& pending_direct_read,
                               std::vector<HashRead>& pending_hash_read,
                               std::vector<InsertOffRead>& pending_insert_off_read,
                               std::list<HashRead>& pending_next_hash_read,
                               std::list<InsertOffRead>& pending_next_off_read,
                               coro_yield_t& yield) {
  if (!CompareCheckDirectRead(pending_direct_read, pending_next_hash_read, pending_next_off_read)) return false;
  if (!CompareCheckHashRead(pending_hash_read, pending_next_hash_read)) return false;
  if (!CompareCheckInsertOffRead(pending_insert_off_read, pending_next_off_read)) return false;

  // Remain some hash buckets need read
  while (!pending_next_hash_read.empty() || !pending_next_off_read.empty()) {
    coro_sched->Yield(yield, coro_id);
    if (!CompareCheckNextHashRead(pending_next_hash_read)) return false;
    if (!CompareCheckNextOffRead(pending_next_off_read)) return false;
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