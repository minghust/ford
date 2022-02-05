// Author: Ming Zhang
// Copyright (c) 2021

#pragma once

#include "allocator/region_allocator.h"
#include "connection/meta_manager.h"
#include "smallbank/smallbank.h"

struct thread_params {
  t_id_t thread_local_id;
  t_id_t thread_global_id;
  t_id_t thread_num_per_machine;
  node_id_t node_num;
  t_id_t total_thread_num;
  SmallBank* smallbank_client;
  MetaManager* global_meta_man;
  RDMARegionAllocator* global_rdma_region;
  int coro_num;
};

void run_thread(struct thread_params* params);