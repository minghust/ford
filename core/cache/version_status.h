// Author: Ming Zhang
// Copyright (c) 2022

#pragma once

#include <functional>
#include <iostream>

#include "base/flags.h"
#include "dtx/structs.h"

// Assumption: In the future, there is one compute pool and one memory pool. Each pool contains many units intead of standalone machines.
// Therefore, the compute units can share some local caches, which include a update_status cache here

#define FAST_PATH 1
#define ADJUST 0
#define TWO_HASH 0

enum VersionStatus : int {
  NO_VERSION_CHANGED = 0,
  VERSION_CHANGED = 1,
  VERSION_EVICTED = 2
};

// It is an optimization validation. Therefore it is ok to set a large hash table with
// ideal concurrency and one bucket has one slot, which stores a k-v pair
struct StatusSlot {
  itemkey_t key;

  // 8B value ensures atomic visibility to readers
  tx_id_t version;
};

struct StatusBucket {
  StatusSlot slots[SLOT_PER_BKT];
};

constexpr size_t BUCKET_SIZE = sizeof(StatusBucket);

class VersionCache {
 public:
  VersionCache() {
#if LOCAL_VALIDATION
    RDMA_LOG(INFO) << "Initializing status tables for validation";
    for (int i = 0; i < MAX_TABLE_NUM; i++) {
      auto* table = new StatusBucket[NUM_BKT];
      size_t sz = BUCKET_SIZE * NUM_BKT;
      std::memset(table, 0, sz);
      RDMA_LOG(INFO) << "Initializing table " << i << " " << sz / 1024 / 1024 << " MB";
      status_table.push_back(table);
    }
#endif
  }

  ~VersionCache() {
    for (auto* table : status_table) {
      if (table) delete[] table;
    }
  }

  void GetTwoBktId(itemkey_t t_k, uint64_t& id1, uint64_t id2) {
    id1 = std_hash(t_k) % NUM_BKT;
    id2 = MurmurHash64A(t_k, 0x43242432);
  }

  void SetVersion(std::vector<DataSetItem>& read_write_set, tx_id_t tx_id) {
#if FAST_PATH
    for (auto& item : read_write_set) {
      itemkey_t key = item.item_ptr->key;
      table_id_t table_id = item.item_ptr->table_id;
      uint64_t bkt_id = GetBktId(key);
      auto* bkt = &(status_table[table_id][bkt_id]);

      bool find_empty = false;
      bool find_match = false;
      int target = 0;
      for (int i = 0; i < SLOT_PER_BKT; i++) {
        if (!find_empty && bkt->slots[i].key == 0) {
          find_empty = true;
          target = i;
        } else if (bkt->slots[i].key == key) {
          bkt->slots[i].version = tx_id;
          find_match = true;
          break;
        }
      }
      if (!find_match) {
        bkt->slots[target].version = tx_id;
        bkt->slots[target].key = key;
      }
    }
#elif ADJUST
    for (auto& item : read_write_set) {
      itemkey_t key = item.item_ptr->key;
      table_id_t table_id = item.item_ptr->table_id;
      uint64_t bkt_id = GetBktId(key);
      auto* bkt = &(status_table[table_id][bkt_id]);

      bool find_empty = false;
      bool find_match = false;
      int target = 0;
      for (int i = 0; i < SLOT_PER_BKT; i++) {
        if (!find_empty && bkt->slots[i].key == 0) {
          find_empty = true;
          target = i;
        } else if (bkt->slots[i].key == key) {
          bkt->slots[i].version = tx_id;
          find_match = true;
          break;
        }
      }

      if (find_match) continue;

      if (find_empty) {
        bkt->slots[target].version = tx_id;
        bkt->slots[target].key = key;
      } else {
        CONFLICT_COUNT++;
        std::cerr << "CONFLICT_COUNT: " << CONFLICT_COUNT << std::endl;
      }
    }
#elif TWO_HASH
    for (auto& item : read_write_set) {
      itemkey_t key = item.item_ptr->key;
      table_id_t table_id = item.item_ptr->table_id;
      uint64_t bkt_id1, bkt_id2;
      GetTwoBktId(key, bkt_id1, bkt_id2);

      bool find_match = false;

      auto* bkt1 = &(status_table[table_id][bkt_id1]);
      bool find_empty1 = false;
      int target1 = 0;

      for (int i = 0; i < SLOT_PER_BKT; i++) {
        if (!find_empty1 && bkt1->slots[i].key == 0) {
          find_empty1 = true;
          target1 = i;
        } else if (bkt1->slots[i].key == key) {
          bkt1->slots[i].version = tx_id;
          find_match = true;
          break;
        }
      }

      if (find_match) continue;

      auto* bkt2 = &(status_table[table_id][bkt_id2]);
      bool find_empty2 = false;
      int target2 = 0;

      for (int i = 0; i < SLOT_PER_BKT; i++) {
        if (!find_empty2 && bkt2->slots[i].key == 0) {
          find_empty2 = true;
          target2 = i;
        } else if (bkt2->slots[i].key == key) {
          bkt2->slots[i].version = tx_id;
          find_match = true;
          break;
        }
      }

      if (find_match) continue;

      if (find_empty1) {
        bkt1->slots[target1].version = tx_id;
        bkt1->slots[target1].key = key;
      } else if (find_empty2) {
        bkt2->slots[target2].version = tx_id;
        bkt2->slots[target2].key = key;
      } else {
        CONFLICT_COUNT++;
        std::cerr << "CONFLICT_COUNT: " << CONFLICT_COUNT << std::endl;
      }
    }
#endif
  }

  // Note that for the read-write set, it must be locked. And the validation is done along
  // with that locking anyway. So we don't care about the read-write set
  // True: a data version has been modified
  // False: no data version changed
  VersionStatus CheckVersion(std::vector<DataSetItem>& read_only_set, tx_id_t my_tx_id) {
    // If no conflict in SetVersion, then no conflict in CheckVersion, because if a ro key has not occur, then
    // it must not be updated by any other writers, so its version remains unchanged. Hence, we only need to
    // ensure no conflict occurs in SetVersion
#if !TWO_HASH
    for (auto& item : read_only_set) {
      itemkey_t key = item.item_ptr->key;
      table_id_t table_id = item.item_ptr->table_id;
      uint64_t bkt_id = GetBktId(key);
      auto* bkt = &(status_table[table_id][bkt_id]);

      for (int i = 0; i < SLOT_PER_BKT; i++) {
        if (bkt->slots[i].key == key) {
          if (bkt->slots[i].version > my_tx_id) {
            return VersionStatus::VERSION_CHANGED;
          }
          break;
        }
      }
    }

    // In a sufficiently large local version cache.
    // If the key is not found, it means that no other txns update the key, 
    // so the versions are not changed, we still do not need remote validations
    return VersionStatus::NO_VERSION_CHANGED;
#else
    for (auto& item : read_only_set) {
      itemkey_t key = item.item_ptr->key;
      table_id_t table_id = item.item_ptr->table_id;
      uint64_t bkt_id1, bkt_id2;
      GetTwoBktId(key, bkt_id1, bkt_id2);

      bool find_match = false;

      auto* bkt1 = &(status_table[table_id][bkt_id1]);

      for (int i = 0; i < SLOT_PER_BKT; i++) {
        if (bkt1->slots[i].key == key) {
          find_match = true;
          if (bkt1->slots[i].version > my_tx_id) {
            return VersionStatus::VERSION_CHANGED;
          }
          break;
        }
      }

      if (find_match) continue;

      auto* bkt2 = &(status_table[table_id][bkt_id2]);

      for (int i = 0; i < SLOT_PER_BKT; i++) {
        if (bkt2->slots[i].key == key) {
          find_match = true;
          if (bkt2->slots[i].version > my_tx_id) {
            return VersionStatus::VERSION_CHANGED;
          }
          break;
        }
      }
      if (find_match) continue;
    }
    return VersionStatus::NO_VERSION_CHANGED;
#endif
  }

 private:
  uint64_t GetBktId(itemkey_t t_k) {
    return (std_hash(t_k) % NUM_BKT);
  }

  std::hash<itemkey_t> std_hash;
  std::vector<StatusBucket*> status_table;
  int CONFLICT_COUNT = 0;
};