// Author: Ming Zhang
// Copyright (c) 2021

#pragma once

#include <cstring>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>

#include "common/common.h"
#include "util/debug.h"

struct DataItem {
  table_id_t table_id;
  size_t value_size;  // The length of uint8* value
  itemkey_t key;
  // remote_offset records this item's offset in the remote memory region
  // it's helpful for addressing each filed in DataItem
  offset_t remote_offset;
  version_t version;
  lock_t lock;
  uint8_t value[MAX_ITEM_SIZE];
  uint8_t valid;        // 1: Not deleted, 0: Deleted
  uint8_t user_insert;  // 1: User insert operation, 0: Not user insert operation

  DataItem() {}
  // Build an empty item for fetching data from remote
  DataItem(table_id_t t, itemkey_t k)
      : table_id(t), value_size(0), key(k), remote_offset(0), version(0), lock(0), valid(1), user_insert(0) {}

  // For user insert item
  DataItem(table_id_t t, size_t s, itemkey_t k, version_t v, uint8_t ins)
      : table_id(t), value_size(s), key(k), remote_offset(0), version(v), lock(0), valid(1), user_insert(ins) {}

  // For server load data
  DataItem(table_id_t t, size_t s, itemkey_t k, uint8_t* d) : table_id(t), value_size(s), key(k), remote_offset(0), version(0), lock(0), valid(1), user_insert(0) {
    memcpy(value, d, s);
  }

  ALWAYS_INLINE
  size_t GetSerializeSize() const {
    return sizeof(*this);
  }

  ALWAYS_INLINE
  void Serialize(char* undo_buffer) {
    memcpy(undo_buffer, (char*)this, sizeof(*this));
  }

  ALWAYS_INLINE
  uint64_t GetRemoteLockAddr() {
    return remote_offset + sizeof(table_id) + sizeof(value_size) + sizeof(key) + sizeof(remote_offset) + sizeof(version);
  }

  ALWAYS_INLINE
  uint64_t GetRemoteLockAddr(offset_t remote_item_off) {
    return remote_item_off + sizeof(table_id) + sizeof(value_size) + sizeof(key) + sizeof(remote_offset) + sizeof(version);
  }

  ALWAYS_INLINE
  uint64_t GetRemoteVersionAddr() {
    return remote_offset + sizeof(table_id) + sizeof(value_size) + sizeof(key) + sizeof(remote_offset);
  }

  ALWAYS_INLINE
  uint64_t GetRemoteVersionAddr(offset_t remote_item_off) {
    return remote_item_off + sizeof(table_id) + sizeof(value_size) + sizeof(key) + sizeof(remote_offset);
  }

  ALWAYS_INLINE
  void Debug(t_id_t tid) const {
    // For debug usage
    TLOG(INFO, tid) << "[Item debug] table id: " << this->table_id << ", value size: " << this->value_size
                    << ", key: " << this->key
                    << ", remote offset: " << this->remote_offset << ", version: " << this->version
                    << ", lock: "
                    << (int)this->lock << ", valid: " << (int)this->valid << ", user insert: "
                    << (int)this->user_insert << std::endl;
    //        TLOG(INFO, tid) << "Contents: 0x";
    //        int i = 0;
    //        for (; i < MAX_ITEM_SIZE - 1; i++) {
    //            TLOG(INFO, tid) << (int) value[i] << " ";
    //        }
    //        TLOG(INFO, tid) << (int) value[i] << " END\n\n";
  }
  ALWAYS_INLINE
  void Debug() const {
    // For debug usage
    RDMA_LOG(DBG) << "[Item debug] table id: " << this->table_id << ", value size: " << this->value_size
                  << ", key: " << this->key
                  << ", remote offset: " << this->remote_offset << ", version: " << this->version
                  << ", lock: " << std::hex << "0x"
                  << this->lock << ", valid: " << std::dec << (int)this->valid << ", user insert: "
                  << (int)this->user_insert;
  }
} Aligned8;  // Size: 560B in X86 arch.

const size_t DataItemSize = sizeof(DataItem);

const size_t RFlushReadSize = 1;  // The size of RDMA read, that is after write to emulate rdma flush

using DataItemPtr = std::shared_ptr<DataItem>;