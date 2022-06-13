// Author: Ming Zhang, Lurong Liu
// Copyright (c) 2022

#pragma once

#include <cassert>

#include "memstore/data_item.h"
#include "memstore/mem_store.h"
#include "util/hash.h"

#define OFFSET_NOT_FOUND -1
#define OFFSET_FOUND 0
#define VERSION_TOO_OLD -2  // The new version < old version

#define SLOT_NOT_FOUND -1
#define SLOT_INV -2
#define SLOT_LOCKED -3
#define SLOT_FOUND 0

const int ITEM_NUM_PER_NODE = 22;

struct HashMeta {
  // To which table this hash store belongs
  table_id_t table_id;

  // Virtual address of the table, used to calculate the distance
  // between some HashNodes with the table for traversing
  // the linked list
  uint64_t data_ptr;

  // Offset of the table, relative to the RDMA local_mr
  offset_t base_off;

  // Total hash buckets
  uint64_t bucket_num;

  // Size of hash node
  size_t node_size;

  HashMeta(table_id_t table_id,
           uint64_t data_ptr,
           uint64_t bucket_num,
           size_t node_size,
           offset_t base_off) : table_id(table_id),
                                data_ptr(data_ptr),
                                base_off(base_off),
                                bucket_num(bucket_num),
                                node_size(node_size) {}
  HashMeta() {}
} Aligned8;

// A hashnode is a bucket
struct HashNode {
  // A dataitem is a slot
  DataItem data_items[ITEM_NUM_PER_NODE];
  HashNode* next;
} Aligned8;

class HashStore {
 public:
  HashStore(table_id_t table_id, uint64_t bucket_num, MemStoreAllocParam* param)
      : table_id(table_id), base_off(0), bucket_num(bucket_num), data_ptr(nullptr), node_num(0) {
    assert(bucket_num > 0);
    table_size = (bucket_num) * sizeof(HashNode);
    region_start_ptr = param->mem_region_start;
    assert((uint64_t)param->mem_store_start + param->mem_store_alloc_offset + table_size <= (uint64_t)param->mem_store_reserve);
    data_ptr = param->mem_store_start + param->mem_store_alloc_offset;
    param->mem_store_alloc_offset += table_size;

    base_off = (uint64_t)data_ptr - (uint64_t)region_start_ptr;
    assert(base_off >= 0);

    RDMA_LOG(INFO) << "Table " << table_id << " size: " << table_size / 1024 / 1024
                   << " MB. Start address: " << std::hex << "0x" << (uint64_t)data_ptr
                   << ", base_off: 0x" << base_off << ", bucket_size: " << std::dec << ITEM_NUM_PER_NODE * DataItemSize << " B";
    assert(data_ptr != nullptr);
    memset(data_ptr, 0, table_size);
  }

  table_id_t GetTableID() const {
    return table_id;
  }

  offset_t GetBaseOff() const {
    return base_off;
  }

  uint64_t GetHashNodeSize() const {
    return sizeof(HashNode);
  }

  uint64_t GetBucketNum() const {
    return bucket_num;
  }

  char* GetDataPtr() const {
    return data_ptr;
  }

  offset_t GetItemRemoteOffset(const void* item_ptr) const {
    return (uint64_t)item_ptr - (uint64_t)region_start_ptr;
  }

  uint64_t TableSize() const {
    return table_size;
  }

  uint64_t GetHash(itemkey_t key) {
    return MurmurHash64A(key, 0xdeadbeef) % bucket_num;
  }

  DataItem* LocalGet(itemkey_t key);

  DataItem* LocalInsert(itemkey_t key, const DataItem& data_item, MemStoreReserveParam* param);

  DataItem* LocalPut(itemkey_t key, const DataItem& data_item, MemStoreReserveParam* param);

  bool LocalDelete(itemkey_t key);

 private:
  // To which table this hash store belongs
  table_id_t table_id;

  // The offset in the RDMA region
  offset_t base_off;

  // Total hash buckets
  uint64_t bucket_num;

  // The point to value in the table
  char* data_ptr;

  // Total hash node nums
  uint64_t node_num;

  // The size of the entire hash table
  size_t table_size;

  // Start of the memory region address, for installing remote offset for data item
  char* region_start_ptr;
};

ALWAYS_INLINE
DataItem* HashStore::LocalGet(itemkey_t key) {
  uint64_t hash = GetHash(key);
  auto* node = (HashNode*)(hash * sizeof(HashNode) + data_ptr);
  while (node) {
    for (auto& data_item : node->data_items) {
      if (data_item.valid && data_item.key == key) {
        return &data_item;
      }
    }
    node = node->next;
  }
  return nullptr;  // failed to found one
}

ALWAYS_INLINE
DataItem* HashStore::LocalInsert(itemkey_t key, const DataItem& data_item, MemStoreReserveParam* param) {
  uint64_t hash = GetHash(key);
  auto* node = (HashNode*)(hash * sizeof(HashNode) + data_ptr);

  // Find
  while (node) {
    for (auto& item : node->data_items) {
      if (!item.valid) {
        item = data_item;
        item.valid = 1;
        return &item;
      }
    }
    if (!node->next) break;
    node = node->next;
  }

  // Allocate
  RDMA_LOG(INFO) << "Table " << table_id << " alloc a new bucket for key: " << key << ". Current slotnum/bucket: " << ITEM_NUM_PER_NODE;
  assert((uint64_t)param->mem_store_reserve + param->mem_store_reserve_offset <= (uint64_t)param->mem_store_end);
  auto* new_node = (HashNode*)(param->mem_store_reserve + param->mem_store_reserve_offset);
  param->mem_store_reserve_offset += sizeof(HashNode);
  memset(new_node, 0, sizeof(HashNode));
  new_node->data_items[0] = data_item;
  new_node->data_items[0].valid = 1;
  new_node->next = nullptr;
  node->next = new_node;
  node_num++;
  return &(new_node->data_items[0]);
}

ALWAYS_INLINE
DataItem* HashStore::LocalPut(itemkey_t key, const DataItem& data_item, MemStoreReserveParam* param) {
  DataItem* res;
  if ((res = LocalGet(key)) != nullptr) {
    // KV pair has already exist, then update
    *res = data_item;
    return res;
  }
  // Insert
  return LocalInsert(key, data_item, param);
}

ALWAYS_INLINE
bool HashStore::LocalDelete(itemkey_t key) {
  uint64_t hash = GetHash(key);
  auto* node = (HashNode*)(hash * sizeof(HashNode) + data_ptr);
  for (auto& data_item : node->data_items) {
    if (data_item.valid && data_item.key == key) {
      data_item.valid = 0;
      return true;
    }
  }
  node = node->next;
  while (node) {
    for (auto& data_item : node->data_items) {
      if (data_item.valid && data_item.key == key) {
        data_item.valid = 0;
        return true;
      }
    }
    node = node->next;
  }
  return false;  // Failed to find one to be deleted
}
