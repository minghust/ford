// Author: Ming Zhang
// Copyright (c) 2021

#pragma once

#include "common/common.h"

const offset_t LOG_BUFFER_SIZE = 1024 * 1024 * 1024;
const node_id_t NUM_MEMORY_NODES = BACKUP_DEGREE + 1;

// Remote offset to write log
class LogOffsetAllocator {
 public:
  LogOffsetAllocator(t_id_t tid, t_id_t num_thread) {
    auto per_thread_remote_log_buffer_size = LOG_BUFFER_SIZE / num_thread;
    for (node_id_t i = 0; i < NUM_MEMORY_NODES; i++) {
      start_log_offsets[i] = tid * per_thread_remote_log_buffer_size;
      end_log_offsets[i] = (tid + 1) * per_thread_remote_log_buffer_size;
      current_log_offsets[i] = 0;
    }
  }

  offset_t GetNextLogOffset(node_id_t node_id, size_t log_entry_size) {
    if (unlikely(start_log_offsets[node_id] + current_log_offsets[node_id] + log_entry_size > end_log_offsets[node_id])) {
      current_log_offsets[node_id] = 0;
    }
    offset_t offset = start_log_offsets[node_id] + current_log_offsets[node_id];
    current_log_offsets[node_id] += log_entry_size;
    return offset;
  }

 private:
  offset_t start_log_offsets[NUM_MEMORY_NODES];
  offset_t end_log_offsets[NUM_MEMORY_NODES];
  offset_t current_log_offsets[NUM_MEMORY_NODES];
};