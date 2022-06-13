// Author: Ming Zhang
// Copyright (c) 2022

#pragma once

#include <memory>

#include "dtx/dtx.h"
#include "micro/micro_db.h"
#include "util/zipf.h"

/******************** The business logic (Transaction) start ********************/

struct DataItemDuplicate {
  DataItemPtr data_item_ptr;
  bool is_dup;
};

bool TxTestCachedAddr(ZipfGen* zipf_gen, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_skewed, uint64_t data_set_size, uint64_t num_keys_global, uint64_t write_ratio);
bool TxLockContention(ZipfGen* zipf_gen, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_skewed, uint64_t data_set_size, uint64_t num_keys_global, uint64_t write_ratio);
bool TxReadBackup(ZipfGen* zipf_gen, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_skewed, uint64_t data_set_size, uint64_t num_keys_global, uint64_t write_ratio);
bool TxReadOnly(ZipfGen* zipf_gen, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_skewed, uint64_t data_set_size, uint64_t num_keys_global, uint64_t write_ratio);
bool TxRFlush1(ZipfGen* zipf_gen, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_skewed, uint64_t data_set_size, uint64_t num_keys_global, uint64_t write_ratio);
bool TxRFlush2(ZipfGen* zipf_gen, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_skewed, uint64_t data_set_size, uint64_t num_keys_global, uint64_t write_ratio);
/******************** The business logic (Transaction) end ********************/