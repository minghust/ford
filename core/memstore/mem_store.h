// Author: Ming Zhang
// Copyright (c) 2022

#pragma once

#include <string>

#include "base/common.h"

enum class MemStoreType {
  kHash = 0,
  kBPlusTree,
};

struct MemStoreAllocParam {
  // The start of the registered memory region for storing memory stores
  char* mem_region_start;

  // The start of the whole memory store space (e.g., Hash Store Space)
  char* mem_store_start;

  // The start offset of each memory store instance
  offset_t mem_store_alloc_offset;

  // The start address of the whole reserved space (e.g., for insert in hash conflict). Here for overflow check
  char* mem_store_reserve;

  MemStoreAllocParam(char* region_start, char* store_start, offset_t start_off, char* reserve_start)
      : mem_region_start(region_start),
        mem_store_start(store_start),
        mem_store_alloc_offset(start_off),
        mem_store_reserve(reserve_start) {}
};

struct MemStoreReserveParam {
  // The start address of the whole reserved space (e.g., for insert in hash conflict).
  char* mem_store_reserve;

  // For allocation in case of memory store (e.g., HashStore) conflict
  offset_t mem_store_reserve_offset;
  
  // The end address of the memory store space. Here for overflow check
  char* mem_store_end;

  MemStoreReserveParam(char* reserve_start, offset_t reserve_off, char* end)
      : mem_store_reserve(reserve_start), mem_store_reserve_offset(reserve_off), mem_store_end(end) {}
};