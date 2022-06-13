// Author: Ming Zhang
// Copyright (c) 2022

#include "micro/micro_db.h"
#include "unistd.h"
#include "util/json_config.h"

/* Called by main. Only initialize here. The worker threads will populate. */
void MICRO::LoadTable(node_id_t node_id,
                      node_id_t num_server,
                      MemStoreAllocParam* mem_store_alloc_param,
                      MemStoreReserveParam* mem_store_reserve_param) {
  // Initiate + Populate table for primary role
  if ((node_id_t)MicroTableType::kMicroTable % num_server == node_id) {
    printf("Primary: Initializing MICRO table\n");
    std::string config_filepath = "../../../workload/micro/micro_tables/micro.json";
    auto json_config = JsonConfig::load_file(config_filepath);
    auto table_config = json_config.get("table");
    micro_table = new HashStore((table_id_t)MicroTableType::kMicroTable,
                                table_config.get("bkt_num").get_uint64(),
                                mem_store_alloc_param);
    PopulateMicroTable(mem_store_reserve_param);
    primary_table_ptrs.push_back(micro_table);
  }

  // Initiate + Populate table for backup role
  if (BACKUP_DEGREE < num_server) {
    for (node_id_t i = 1; i <= BACKUP_DEGREE; i++) {
      if ((node_id_t)MicroTableType::kMicroTable % num_server == (node_id - i + num_server) % num_server) {
        printf("Backup: Initializing MICRO table\n");
        std::string config_filepath = "../../../workload/micro/micro_tables/micro.json";
        auto json_config = JsonConfig::load_file(config_filepath);
        auto table_config = json_config.get("table");
        micro_table = new HashStore((table_id_t)MicroTableType::kMicroTable,
                                    table_config.get("bkt_num").get_uint64(),
                                    mem_store_alloc_param);
        PopulateMicroTable(mem_store_reserve_param);
        backup_table_ptrs.push_back(micro_table);
      }
    }
  }
}

void MICRO::PopulateMicroTable(MemStoreReserveParam* mem_store_reserve_param) {
  /* All threads must execute the loop below deterministically */
  RDMA_LOG(DBG) << "NUM KEYS TOTAL: " << num_keys_global;
  /* Populate the tables */
  for (uint64_t id = 0; id < num_keys_global; id++) {
    micro_key_t micro_key;
    micro_key.micro_id = (uint64_t)id;

    micro_val_t micro_val;
    for (int i = 0; i < 5; i++) {
      micro_val.magic[i] = micro_magic + i;
    }

    LoadRecord(micro_table, micro_key.item_key,
               (void*)&micro_val, sizeof(micro_val_t),
               (table_id_t)MicroTableType::kMicroTable,
               mem_store_reserve_param);
  }
}

int MICRO::LoadRecord(HashStore* table,
                      itemkey_t item_key,
                      void* val_ptr,
                      size_t val_size,
                      table_id_t table_id,
                      MemStoreReserveParam* mem_store_reserve_param) {
  assert(val_size <= MAX_ITEM_SIZE);
  /* Insert into HashStore */
  DataItem item_to_be_inserted(table_id, val_size, item_key, (uint8_t*)val_ptr);
  DataItem* inserted_item = table->LocalInsert(item_key, item_to_be_inserted, mem_store_reserve_param);
  inserted_item->remote_offset = table->GetItemRemoteOffset(inserted_item);
  return 1;
}
