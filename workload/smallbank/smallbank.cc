// Author: Ming Zhang, Lurong Liu
// Copyright (c) 2021

#include "smallbank.h"

#include "unistd.h"
#include "util/json_config.h"

/* Called by main. Only initialize here. The worker threads will populate. */
void SmallBank::LoadTable(node_id_t node_id,
                          node_id_t num_server,
                          MemStoreAllocParam* mem_store_alloc_param,
                          MemStoreReserveParam* mem_store_reserve_param) {
  // Initiate + Populate table for primary role
  if ((node_id_t)SmallBankTableType::kSavingsTable % num_server == node_id) {
    printf("Primary: Initializing SAVINGS table\n");
    std::string config_filepath = "../../../workload/smallbank/smallbank_tables/savings.json";
    auto json_config = JsonConfig::load_file(config_filepath);
    auto table_config = json_config.get("table");
    savings_table = new HashStore((table_id_t)SmallBankTableType::kSavingsTable,
                                  table_config.get("item_count").get_uint64(),
                                  mem_store_alloc_param);
    PopulateSavingsTable(mem_store_reserve_param);
    primary_table_ptrs.push_back(savings_table);
  }
  if ((node_id_t)SmallBankTableType::kCheckingTable % num_server == node_id) {
    printf("Primary: Initializing CHECKING table\n");
    std::string config_filepath = "../../../workload/smallbank/smallbank_tables/checking.json";
    auto json_config = JsonConfig::load_file(config_filepath);
    auto table_config = json_config.get("table");
    checking_table = new HashStore((table_id_t)SmallBankTableType::kCheckingTable,
                                   table_config.get("item_count").get_uint64(),
                                   mem_store_alloc_param);
    PopulateCheckingTable(mem_store_reserve_param);
    primary_table_ptrs.push_back(checking_table);
  }

  // Initiate + Populate table for backup role
  if (BACKUP_DEGREE < num_server) {
    for (node_id_t i = 1; i <= BACKUP_DEGREE; i++) {
      if ((node_id_t)SmallBankTableType::kSavingsTable % num_server == (node_id - i + num_server) % num_server) {
        printf("Backup: Initializing SAVINGS table\n");
        std::string config_filepath = "../../../workload/smallbank/smallbank_tables/savings.json";
        auto json_config = JsonConfig::load_file(config_filepath);
        auto table_config = json_config.get("table");
        savings_table = new HashStore((table_id_t)SmallBankTableType::kSavingsTable,
                                      table_config.get("item_count").get_uint64(),
                                      mem_store_alloc_param);
        PopulateSavingsTable(mem_store_reserve_param);
        backup_table_ptrs.push_back(savings_table);
      }
      if ((node_id_t)SmallBankTableType::kCheckingTable % num_server == (node_id - i + num_server) % num_server) {
        printf("Backup: Initializing CHECKING table\n");
        std::string config_filepath = "../../../workload/smallbank/smallbank_tables/checking.json";
        auto json_config = JsonConfig::load_file(config_filepath);
        auto table_config = json_config.get("table");
        checking_table = new HashStore((table_id_t)SmallBankTableType::kCheckingTable,
                                       table_config.get("item_count").get_uint64(),
                                       mem_store_alloc_param);
        PopulateCheckingTable(mem_store_reserve_param);
        backup_table_ptrs.push_back(checking_table);
      }
    }
  }
}

int SmallBank::LoadRecord(HashStore* table,
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

void SmallBank::PopulateSavingsTable(MemStoreReserveParam* mem_store_reserve_param) {
  /* All threads must execute the loop below deterministically */

  /* Populate the tables */
  for (uint32_t acct_id = 0; acct_id < num_accounts_global; acct_id++) {
    // Savings
    smallbank_savings_key_t savings_key;
    savings_key.acct_id = (uint64_t)acct_id;

    smallbank_savings_val_t savings_val;
    savings_val.magic = smallbank_savings_magic;
    savings_val.bal = 1000000000ull;

    LoadRecord(savings_table, savings_key.item_key,
               (void*)&savings_val, sizeof(smallbank_savings_val_t),
               (table_id_t)SmallBankTableType::kSavingsTable,
               mem_store_reserve_param);
  }
}

void SmallBank::PopulateCheckingTable(MemStoreReserveParam* mem_store_reserve_param) {
  /* All threads must execute the loop below deterministically */

  /* Populate the tables */
  for (uint32_t acct_id = 0; acct_id < num_accounts_global; acct_id++) {
    // Checking
    smallbank_checking_key_t checking_key;
    checking_key.acct_id = (uint64_t)acct_id;

    smallbank_checking_val_t checking_val;
    checking_val.magic = smallbank_checking_magic;
    checking_val.bal = 1000000000ull;

    LoadRecord(checking_table, checking_key.item_key,
               (void*)&checking_val, sizeof(smallbank_checking_val_t),
               (table_id_t)SmallBankTableType::kCheckingTable,
               mem_store_reserve_param);
  }
}