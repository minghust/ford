// Author: Ming Zhang
// Copyright (c) 2022

#pragma once

#include <functional>
#include <iostream>

#include "base/flags.h"
#include "dtx/structs.h"

struct LockBkt {
  std::atomic<uint64_t> key;
  std::atomic<uint64_t> lock;
};

// Unfortunately, we find that in a high concurrency environment, substantial CPU CAS operations
// need to frequently retry to find empty slots due to the high collision rates in a small hash table. 
// This will in turn cause the latency of local locking to be an order of magnitude higher than the 
// remote locking, i.e., by using the microsecond-latency RDMA CAS. Due to this reason, we disable 
// the use of local cache in FORD.

class LockCache {
 public:
  LockCache() {
#if LOCAL_LOCK
    RDMA_LOG(INFO) << "Initializing local lock tables";
    for (int i = 0; i < MAX_TABLE_NUM; i++) {
      total_slot = (size_t)(SLOT_PER_BKT * NUM_BKT);
      auto* table = new LockBkt[total_slot];
      size_t sz = sizeof(LockBkt) * total_slot;
      memset(table, 0, sz);
      RDMA_LOG(INFO) << "Initializing table " << i << " " << sz / 1024 / 1024 << " MB";
      status_table.push_back(table);
    }
#endif
  }

  ~LockCache() {
    for (auto* table : status_table) {
      if (table) delete[] table;
    }
  }

  bool TryLock(std::vector<DataSetItem>& read_write_set) {
    for (auto& item : read_write_set) {
      itemkey_t my_key = item.item_ptr->key;
      table_id_t table_id = item.item_ptr->table_id;
      auto* table = status_table[table_id];

      for (uint64_t bkt_id = GetBktId(my_key);; bkt_id++) {
        bkt_id = bkt_id % total_slot;

        uint64_t probed_key = table[bkt_id].key.load(std::memory_order_relaxed);

        if (probed_key == my_key) {
          lock_t expect_lock = 0;
          bool exchanged = table[bkt_id].lock.compare_exchange_strong(expect_lock, STATE_LOCKED);
          if (!exchanged) return false;  // This key is locked by another coordinator
          // I successfully lock this key
          item.bkt_idx = (int64_t)bkt_id;
          break;
        } else {
          // Another key occupies
          if (probed_key != 0) continue;

          // An empty slot
          uint64_t expect_key = 0;
          bool exchanged = table[bkt_id].key.compare_exchange_strong(expect_key, my_key);
          if (exchanged) {
            // We cannot just use store here because another thread may cas succ in `if (probed_key == my_key)' above after
            // we fill the key. So we need to do cas instead of pure store. Only cas succ we can get the lock
            lock_t expect_lock = 0;
            bool exchanged = table[bkt_id].lock.compare_exchange_strong(expect_lock, STATE_LOCKED);
            if (!exchanged) return false;  // This key is locked by another coordinator
            // I successfully lock this key
            item.bkt_idx = (int64_t)bkt_id;
            break;
          } else if (!exchanged && expect_key == my_key) {
            // Another thread locks the same key, I abort
            return false;
          } else if (!exchanged && expect_key != my_key) {
            // Another thread writes a different key, I keep probe
            continue;
          }
        }
      }
    }

    return true;
  }

  void Unlock(std::vector<DataSetItem>& read_write_set) {
    for (auto& item : read_write_set) {
      if (item.bkt_idx == -1) continue;
      table_id_t table_id = item.item_ptr->table_id;
      auto* table = status_table[table_id];
      table[item.bkt_idx].lock.store(STATE_CLEAN, std::memory_order_relaxed);
    }
  }

 private:
  uint64_t GetBktId(itemkey_t k) {
    // return std_hash(k);
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccd;
    k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53;
    k ^= k >> 33;
    return k;
  }

  std::hash<itemkey_t> std_hash;
  std::vector<LockBkt*> status_table;
  size_t total_slot;
  int CONFLICT_COUNT = 0;
};