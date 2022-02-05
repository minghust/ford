// Author: Ming Zhang
// Copyright (c) 2021

#pragma once

#include "allocator/region_allocator.h"
#include "common/common.h"

// Alloc registered RDMA buffer for each thread
class RDMABufferAllocator {
 public:
  RDMABufferAllocator(char* s, char* e) : start(s), end(e), cur_offset(0) {}

  ALWAYS_INLINE
  char* Alloc(size_t size) {
    // When the thread local region is exhausted, the region
    // can be re-used (i.e., overwritten) at the front offset, i.e., 0. This is almost always true,
    // because the local region is typically GB-scale, and hence the front
    // allocated buffer has already finished serving for RDMA requests and replies, or has already aborted.
    // As such, our Allocator is extremely fast due to simply moving the pointer.
    // If anyone relies on a more reliable allocator, you can just re-implement this Alloc interface
    // using other standard allocators, e.g., ptmalloc/jemalloc/tcmalloc.

    if (unlikely(start + cur_offset + size > end)) {
      cur_offset = 0;
    }
    char* ret = start + cur_offset;
    cur_offset += size;
    return ret;
  }

  ALWAYS_INLINE
  void Free(void* p) {
    // As the memory region can be safely reused, we do not need to
    // explicitly deallocate the previously allocated memory region buffer.
  }

 private:
  // Each thread has a local RDMA region to temporarily alloc a small buffer.
  // This local region has an address range: [start, end)
  char* start;
  char* end;
  uint64_t cur_offset;
};
