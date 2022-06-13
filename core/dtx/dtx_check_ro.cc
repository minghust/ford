// Author: Ming Zhang
// Copyright (c) 2022

#include "dtx/dtx.h"
#include "util/timer.h"

bool DTX::CheckDirectRO(std::vector<DirectRead>& pending_direct_ro,
                        std::list<InvisibleRead>& pending_invisible_ro,
                        std::list<HashRead>& pending_next_hash_ro) {
  // Check results from direct read via local cache
  for (auto& res : pending_direct_ro) {
    auto* it = res.item->item_ptr.get();
    auto* fetched_item = (DataItem*)res.buf;
    if (likely(fetched_item->key == it->key && fetched_item->table_id == it->table_id)) {
      if (likely(fetched_item->valid)) {
        *it = *fetched_item;
        res.item->is_fetched = true;
#if LOCK_REFUSE_READ_RO
        if (it->lock == STATE_LOCKED) return false;
#else
        if (unlikely((it->lock & STATE_INVISIBLE))) {
#if INV_ABORT
          return false;
#endif
          // This item is invisible, we need re-read
          char* cas_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
          uint64_t lock_offset = it->GetRemoteLockAddr(it->remote_offset);
          pending_invisible_ro.emplace_back(InvisibleRead{.qp = res.qp, .buf = cas_buf, .off = lock_offset});
          // RDMA_LOG(DBG) << "Invisible in CheckDirectRO, lock=" << std::hex << it->lock << ", table_id=" << it->table_id << ", key=" << std::dec << it->key << ", tx_id=" << tx_id;
          if (!coro_sched->RDMARead(coro_id, res.qp, cas_buf, lock_offset, sizeof(lock_t))) return false;
        }
#endif
      } else {
        // The item is deleted before, then update the local cache
        addr_cache->Insert(res.remote_node, it->table_id, it->key, NOT_FOUND);
        return false;
      }
    } else {
      // The cached address is stale. E.g., insert a new item after being deleted
      // Local cache does not have. We have to re-read via hash
      node_id_t remote_node_id = global_meta_man->GetPrimaryNodeID(it->table_id);
      const HashMeta& meta = global_meta_man->GetPrimaryHashMetaWithTableID(it->table_id);
      uint64_t idx = MurmurHash64A(it->key, 0xdeadbeef) % meta.bucket_num;
      offset_t node_off = idx * meta.node_size + meta.base_off;
      auto* local_hash_node = (HashNode*)thread_rdma_buffer_alloc->Alloc(sizeof(HashNode));
      pending_next_hash_ro.emplace_back(HashRead{.qp = res.qp, .item = res.item, .buf = (char*)local_hash_node, .remote_node = remote_node_id, .meta = meta});
      if (!coro_sched->RDMARead(coro_id, res.qp, (char*)local_hash_node, node_off, sizeof(HashNode))) return false;
    }
  }
  return true;
}

bool DTX::CheckHashRO(std::vector<HashRead>& pending_hash_ro,
                      std::list<InvisibleRead>& pending_invisible_ro,
                      std::list<HashRead>& pending_next_hash_ro) {
  // Check results from hash read
  for (auto& res : pending_hash_ro) {
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
#if LOCK_REFUSE_READ_RO
      if (it->lock == STATE_LOCKED) return false;
#else
      if (unlikely((it->lock & STATE_INVISIBLE))) {
#if INV_ABORT
        return false;
#endif
        // This item is invisible, we need re-read
        char* cas_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
        uint64_t lock_offset = it->GetRemoteLockAddr(it->remote_offset);
        pending_invisible_ro.emplace_back(InvisibleRead{.qp = res.qp, .buf = cas_buf, .off = lock_offset});
        // RDMA_LOG(DBG) << "Invisible in CheckHashRO, lock=" << std::hex << it->lock << ", table_id=" << it->table_id << ", key=" << std::dec << it->key << ", tx_id=" << tx_id;
        if (!coro_sched->RDMARead(coro_id, res.qp, cas_buf, lock_offset, sizeof(lock_t))) return false;
      }
#endif
    } else {
      if (local_hash_node->next == nullptr) return false;
      // Not found, we need to re-read the next bucket
      auto node_off = (uint64_t)local_hash_node->next - res.meta.data_ptr + res.meta.base_off;
      pending_next_hash_ro.emplace_back(HashRead{.qp = res.qp, .item = res.item, .buf = res.buf, .remote_node = res.remote_node, .meta = res.meta});
      if (!coro_sched->RDMARead(coro_id, res.qp, res.buf, node_off, sizeof(HashNode))) return false;
    }
  }
  return true;
}

bool DTX::CheckInvisibleRO(std::list<InvisibleRead>& pending_invisible_ro) {
#if INV_BUSY_WAIT
  Timer timer;
  timer.Start();
  int re_read = pending_invisible_ro.size();

  for (auto iter = pending_invisible_ro.begin(); iter != pending_invisible_ro.end();) {
    auto res = *iter;
    auto lock_value = *((lock_t*)res.buf);
    while (lock_value & STATE_INVISIBLE) {
      // TLOG(DBG, t_id) << "invisible data LOCK VALUE IS: " << std::hex << lock_value;
      // RDMA_LOG(DBG) << "LOCK VALUE IS: " << std::hex << lock_value;
      auto rc = res.qp->post_send(IBV_WR_RDMA_READ, res.buf, sizeof(lock_t), res.off, IBV_SEND_SIGNALED);
      if (rc != SUCC) {
        TLOG(ERROR, t_id) << "client: post read (read visible flag) fail. rc=" << rc;
        exit(-1);
      }
      re_read++;
      ibv_wc wc{};
      rc = res.qp->poll_till_completion(wc, no_timeout);
      if (rc != SUCC) {
        TLOG(ERROR, t_id) << "client: poll read (read visible flag) fail. rc=" << rc;
        exit(-1);
      }
      lock_value = *((lock_t*)res.buf);
      // RDMA_LOG(DBG) << "THREAD " << t_id << " re-read lock: " << std::hex << lock_value;
    }
    iter = pending_invisible_ro.erase(iter);
  }

  timer.Stop();
  invisible_durations.emplace_back(timer.Duration_us());
  invisible_reread.emplace_back(re_read);
#else
  // TLOG(DBG, t_id) << "in check invisible";
  // RDMA_LOG(DBG) << "in check invisible";
  for (auto iter = pending_invisible_ro.begin(); iter != pending_invisible_ro.end();) {
    auto res = *iter;
    auto lock_value = *((lock_t*)res.buf);
    if (lock_value & STATE_INVISIBLE) {
      // RDMA_LOG(DBG) << "LOCK VALUE IS: " << std::hex << lock_value;
      if (!coro_sched->RDMARead(coro_id, res.qp, res.buf, res.off, sizeof(lock_t))) return false;
      iter++;
    } else {
      iter = pending_invisible_ro.erase(iter);
    }
  }
#endif

  // RDMA_LOG(DBG) << "txn " << tx_id << " stucks in check invisible";
  return true;
}

bool DTX::CheckNextHashRO(std::list<InvisibleRead>& pending_invisible_ro,
                          std::list<HashRead>& pending_next_hash_ro) {
  for (auto iter = pending_next_hash_ro.begin(); iter != pending_next_hash_ro.end();) {
    auto res = *iter;
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
#if LOCK_REFUSE_READ_RO
      if (it->lock == STATE_LOCKED) return false;
#else
      if (unlikely((it->lock & STATE_INVISIBLE))) {
#if INV_ABORT
        return false;
#endif
        // This item is invisible, we need re-read
        char* cas_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
        uint64_t lock_offset = it->GetRemoteLockAddr(it->remote_offset);
        pending_invisible_ro.emplace_back(InvisibleRead{.qp = res.qp, .buf = cas_buf, .off = lock_offset});
        if (!coro_sched->RDMARead(coro_id, res.qp, cas_buf, lock_offset, sizeof(lock_t))) return false;
      }
#endif
      iter = pending_next_hash_ro.erase(iter);
    } else {
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
