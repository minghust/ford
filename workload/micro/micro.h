// Author: Ming Zhang
// Copyright (c) 2021

#pragma once

#include <cassert>
#include <cstdint>
#include <vector>

#include "config/table_type.h"
#include "memstore/hash_store.h"
#include "util/fast_random.h"
#include "util/json_config.h"

/* STORED PROCEDURE EXECUTION FREQUENCIES (0-100) */
#define FREQUENCY_LOCKCONTENTION 100

union micro_key_t {
  uint64_t micro_id;
  uint64_t item_key;

  micro_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(micro_key_t) == sizeof(uint64_t), "");

struct micro_val_t {
  // 40 bytes, consistent with FaSST
  uint64_t magic[5];
};
static_assert(sizeof(micro_val_t) == 40, "");

// Magic numbers for debugging. These are unused in the spec.
#define Micro_MAGIC 97 /* Some magic number <= 255 */
#define micro_magic (Micro_MAGIC)

// Helpers for generating workload
enum class MicroTxType : int {
  kLockContention,
};

// Table id
enum class MicroTableType : uint64_t {
  kMicroTable = TABLE_MICRO,
};

static ALWAYS_INLINE 
uint64_t align_pow2(uint64_t v) {
  v--;
  v |= v >> 1;
  v |= v >> 2;
  v |= v >> 4;
  v |= v >> 8;
  v |= v >> 16;
  v |= v >> 32;
  return v + 1;
}

class MICRO {
 public:
  uint64_t num_keys_global;

  /* Tables */
  HashStore* micro_table;

  std::vector<HashStore*> primary_table_ptrs;
  
  std::vector<HashStore*> backup_table_ptrs;

  // For server usage: Provide interfaces to servers for loading tables
  // Also for client usage: Provide interfaces to clients for generating ids during tests
  MICRO() {
    std::string config_filepath = "../../../config/micro_config.json";
    auto json_config = JsonConfig::load_file(config_filepath);
    auto conf = json_config.get("micro");
    auto num_keys = conf.get("num_keys").get_int64();
    num_keys_global = align_pow2(num_keys);
    micro_table = nullptr;
  }

  ~MICRO() {
    if (micro_table) delete micro_table;
  }

  MicroTxType* CreateWorkgenArray() {
    MicroTxType* workgen_arr = new MicroTxType[100];
    int i = 0, j = 0;
    j += FREQUENCY_LOCKCONTENTION;
    for (; i < j; i++) workgen_arr[i] = MicroTxType::kLockContention;
    assert(i == 100 && j == 100);
    return workgen_arr;
  }

  void LoadTable(node_id_t node_id,
                 node_id_t num_server,
                 MemStoreAllocParam* mem_store_alloc_param,
                 MemStoreReserveParam* mem_store_reserve_param);

  void PopulateMicroTable(MemStoreReserveParam* mem_store_reserve_param);

  int LoadRecord(HashStore* table,
                 itemkey_t item_key,
                 void* val_ptr,
                 size_t val_size,
                 table_id_t table_id,
                 MemStoreReserveParam* mem_store_reserve_param);

  ALWAYS_INLINE
  std::vector<HashStore*> GetPrimaryHashStore() {
    return primary_table_ptrs;
  }

  ALWAYS_INLINE
  std::vector<HashStore*> GetBackupHashStore() {
    return backup_table_ptrs;
  }
};
