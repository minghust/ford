// Author: Ming Zhang, Lurong Liu
// Copyright (c) 2021

#include "tpcc.h"

#define NUM_CUSTOMER_LAST_NAME_FROM_CID 100
#define NUM_ORDER_MINUS_NEWORDER 210

void TPCC::LoadTable(node_id_t node_id,
                     node_id_t num_server,
                     MemStoreAllocParam* mem_store_alloc_param,
                     MemStoreReserveParam* mem_store_reserve_param) {
  printf(
      "sizeof(tpcc_warehouse_val_t) = %lu, sizeof(tpcc_district_val_t) = %lu\n"
      "sizeof(tpcc_customer_val_t) = %lu, sizeof(tpcc_customer_index_val_t) = %lu\n"
      "sizeof(tpcc_history_val_t) = %lu, sizeof(tpcc_new_order_val_t) = %lu\n"
      "sizeof(tpcc_order_val_t) = %lu, sizeof(tpcc_order_index_val_t) = %lu\n"
      "sizeof(tpcc_order_line_val_t) = %lu, sizeof(tpcc_item_val_t) = %lu\n"
      "sizeof(tpcc_stock_val_t) = %lu, DataItemSize = %lu\n",
      sizeof(tpcc_warehouse_val_t),
      sizeof(tpcc_district_val_t),

      sizeof(tpcc_customer_val_t),
      sizeof(tpcc_customer_index_val_t),
      sizeof(tpcc_history_val_t),

      sizeof(tpcc_new_order_val_t),
      sizeof(tpcc_order_val_t),
      sizeof(tpcc_order_index_val_t),
      sizeof(tpcc_order_line_val_t),
      sizeof(tpcc_item_val_t),
      sizeof(tpcc_stock_val_t),
      DataItemSize);
  // Initiate + Populate table for primary role
  if ((node_id_t)TPCCTableType::kWarehouseTable % num_server == node_id) {
    printf("Primary: Initializing Warehouse table\n");
    std::string config_filepath = "../../../workload/tpcc/tpcc_tables/warehouse.json";
    auto json_config = JsonConfig::load_file(config_filepath);
    auto table_config = json_config.get("table");
    warehouse_table =
        new HashStore((table_id_t)TPCCTableType::kWarehouseTable,
                      table_config.get("item_count").get_uint64(),
                      mem_store_alloc_param);
    printf("Warehouse table setup\n");
    PopulateWarehouseTable(9324, mem_store_reserve_param);
    primary_table_ptrs.push_back(warehouse_table);
  }
  if ((node_id_t)TPCCTableType::kDistrictTable % num_server == node_id) {
    printf("Primary: Initializing District table\n");
    std::string warehouse_config_filepath = "../../../workload/tpcc/tpcc_tables/warehouse.json";
    auto warehouse_json_config = JsonConfig::load_file(warehouse_config_filepath);
    auto warehouse_table_config = warehouse_json_config.get("table");
    std::string district_config_filepath = "../../../workload/tpcc/tpcc_tables/district.json";
    auto district_json_config = JsonConfig::load_file(district_config_filepath);
    auto district_table_config = district_json_config.get("table");
    district_table =
        new HashStore((table_id_t)TPCCTableType::kDistrictTable,
                      warehouse_table_config.get("item_count").get_uint64() * district_table_config.get("item_count").get_uint64(),
                      mem_store_alloc_param);

    PopulateDistrictTable(129856349, mem_store_reserve_param);
    primary_table_ptrs.push_back(district_table);
  }
  if ((node_id_t)TPCCTableType::kCustomerTable % num_server == node_id) {
    printf("Primary: Initializing Customer+CustomerIndex+History table\n");
    std::string warehouse_config_filepath = "../../../workload/tpcc/tpcc_tables/warehouse.json";
    auto warehouse_json_config = JsonConfig::load_file(warehouse_config_filepath);
    auto warehouse_table_config = warehouse_json_config.get("table");
    std::string district_config_filepath = "../../../workload/tpcc/tpcc_tables/district.json";
    auto district_json_config = JsonConfig::load_file(district_config_filepath);
    auto district_table_config = district_json_config.get("table");
    std::string customer_config_filepath = "../../../workload/tpcc/tpcc_tables/customer.json";
    auto customer_json_config = JsonConfig::load_file(customer_config_filepath);
    auto customer_table_config = customer_json_config.get("table");

    customer_table =
        new HashStore((table_id_t)TPCCTableType::kCustomerTable,
                      warehouse_table_config.get("item_count").get_uint64() * district_table_config.get("item_count").get_uint64() * customer_table_config.get("item_count").get_uint64(),
                      mem_store_alloc_param);

    customer_index_table =
        new HashStore((table_id_t)TPCCTableType::kCustomerIndexTable,
                      warehouse_table_config.get("item_count").get_uint64() * district_table_config.get("item_count").get_uint64() * customer_table_config.get("item_count").get_uint64(),
                      mem_store_alloc_param);
    history_table =
        new HashStore((table_id_t)TPCCTableType::kHistoryTable,
                      warehouse_table_config.get("item_count").get_uint64() * district_table_config.get("item_count").get_uint64() * customer_table_config.get("item_count").get_uint64(),
                      mem_store_alloc_param);

    PopulateCustomerAndHistoryTable(923587856425, mem_store_reserve_param);
    primary_table_ptrs.push_back(customer_table);
    primary_table_ptrs.push_back(customer_index_table);
    primary_table_ptrs.push_back(history_table);
  }
  if ((node_id_t)TPCCTableType::kOrderTable % num_server == node_id) {
    printf("Primary: Initializing Order+OrderIndex+NewOrder+OrderLine table\n");
    std::string warehouse_config_filepath = "../../../workload/tpcc/tpcc_tables/warehouse.json";
    auto warehouse_json_config = JsonConfig::load_file(warehouse_config_filepath);
    auto warehouse_table_config = warehouse_json_config.get("table");
    std::string district_config_filepath = "../../../workload/tpcc/tpcc_tables/district.json";
    auto district_json_config = JsonConfig::load_file(district_config_filepath);
    auto district_table_config = district_json_config.get("table");
    std::string customer_config_filepath = "../../../workload/tpcc/tpcc_tables/customer.json";
    auto customer_json_config = JsonConfig::load_file(customer_config_filepath);
    auto customer_table_config = customer_json_config.get("table");

    order_table =
        new HashStore((table_id_t)TPCCTableType::kOrderTable,
                      warehouse_table_config.get("item_count").get_uint64() * district_table_config.get("item_count").get_uint64() * customer_table_config.get("item_count").get_uint64(),
                      mem_store_alloc_param);
    order_index_table =
        new HashStore((table_id_t)TPCCTableType::kOrderIndexTable,
                      warehouse_table_config.get("item_count").get_uint64() * district_table_config.get("item_count").get_uint64() * customer_table_config.get("item_count").get_uint64(),
                      mem_store_alloc_param);
    new_order_table =
        new HashStore((table_id_t)TPCCTableType::kNewOrderTable,
                      warehouse_table_config.get("item_count").get_uint64() * district_table_config.get("item_count").get_uint64() * customer_table_config.get("item_count").get_uint64() * 0.3,
                      mem_store_alloc_param);
    order_line_table =
        new HashStore((table_id_t)TPCCTableType::kOrderLineTable,
                      warehouse_table_config.get("item_count").get_uint64() * district_table_config.get("item_count").get_uint64() * customer_table_config.get("item_count").get_uint64() * 15,
                      mem_store_alloc_param);

    PopulateOrderNewOrderAndOrderLineTable(2343352, mem_store_reserve_param);
    primary_table_ptrs.push_back(order_table);
    primary_table_ptrs.push_back(order_index_table);
    primary_table_ptrs.push_back(new_order_table);
    primary_table_ptrs.push_back(order_line_table);
  }
  if ((node_id_t)TPCCTableType::kStockTable % num_server == node_id) {
    printf("Primary: Initializing Stock table\n");
    std::string config_filepath = "../../../workload/tpcc/tpcc_tables/stock.json";
    auto json_config = JsonConfig::load_file(config_filepath);
    auto table_config = json_config.get("table");
    stock_table =
        new HashStore((table_id_t)TPCCTableType::kStockTable,
                      table_config.get("item_count").get_uint64(),
                      mem_store_alloc_param);
    PopulateStockTable(89785943, mem_store_reserve_param);
    primary_table_ptrs.push_back(stock_table);
  }
  if ((node_id_t)TPCCTableType::kItemTable % num_server == node_id) {
    printf("Primary: Initializing Item table\n");
    std::string config_filepath = "../../../workload/tpcc/tpcc_tables/item.json";
    auto json_config = JsonConfig::load_file(config_filepath);
    auto table_config = json_config.get("table");
    item_table =
        new HashStore((table_id_t)TPCCTableType::kItemTable,
                      table_config.get("item_count").get_uint64(),
                      mem_store_alloc_param);
    PopulateItemTable(235443, mem_store_reserve_param);
    primary_table_ptrs.push_back(item_table);
  }

  // Initiate + Populate table for backup role
  if (BACKUP_DEGREE < num_server) {
    for (node_id_t i = 1; i <= BACKUP_DEGREE; i++) {
      if ((node_id_t)TPCCTableType::kWarehouseTable % num_server == (node_id - i + num_server) % num_server) {
        printf("Backup: Initializing Warehouse table\n");
        std::string config_filepath = "../../../workload/tpcc/tpcc_tables/warehouse.json";
        auto json_config = JsonConfig::load_file(config_filepath);
        auto table_config = json_config.get("table");
        warehouse_table =
            new HashStore((table_id_t)TPCCTableType::kWarehouseTable,
                          table_config.get("item_count").get_uint64(),
                          mem_store_alloc_param);
        PopulateWarehouseTable(9324, mem_store_reserve_param);
        backup_table_ptrs.push_back(warehouse_table);
      }
      if ((node_id_t)TPCCTableType::kDistrictTable % num_server == (node_id - i + num_server) % num_server) {
        printf("Backup: Initializing District table\n");
        std::string warehouse_config_filepath = "../../../workload/tpcc/tpcc_tables/warehouse.json";
        auto warehouse_json_config = JsonConfig::load_file(warehouse_config_filepath);
        auto warehouse_table_config = warehouse_json_config.get("table");
        std::string district_config_filepath = "../../../workload/tpcc/tpcc_tables/district.json";
        auto district_json_config = JsonConfig::load_file(district_config_filepath);
        auto district_table_config = district_json_config.get("table");
        district_table =
            new HashStore((table_id_t)TPCCTableType::kDistrictTable,
                          warehouse_table_config.get("item_count").get_uint64() * district_table_config.get("item_count").get_uint64(),
                          mem_store_alloc_param);

        PopulateDistrictTable(129856349, mem_store_reserve_param);
        backup_table_ptrs.push_back(district_table);
      }
      if ((node_id_t)TPCCTableType::kCustomerTable % num_server == (node_id - i + num_server) % num_server) {
        printf("Backup: Initializing Customer+CustomerIndex+History table\n");
        std::string warehouse_config_filepath = "../../../workload/tpcc/tpcc_tables/warehouse.json";
        auto warehouse_json_config = JsonConfig::load_file(warehouse_config_filepath);
        auto warehouse_table_config = warehouse_json_config.get("table");
        std::string district_config_filepath = "../../../workload/tpcc/tpcc_tables/district.json";
        auto district_json_config = JsonConfig::load_file(district_config_filepath);
        auto district_table_config = district_json_config.get("table");
        std::string customer_config_filepath = "../../../workload/tpcc/tpcc_tables/customer.json";
        auto customer_json_config = JsonConfig::load_file(customer_config_filepath);
        auto customer_table_config = customer_json_config.get("table");

        customer_table =
            new HashStore((table_id_t)TPCCTableType::kCustomerTable,
                          warehouse_table_config.get("item_count").get_uint64() * district_table_config.get("item_count").get_uint64() * customer_table_config.get("item_count").get_uint64(),
                          mem_store_alloc_param);

        customer_index_table =
            new HashStore((table_id_t)TPCCTableType::kCustomerIndexTable,
                          warehouse_table_config.get("item_count").get_uint64() * district_table_config.get("item_count").get_uint64() * customer_table_config.get("item_count").get_uint64(),
                          mem_store_alloc_param);
        history_table =
            new HashStore((table_id_t)TPCCTableType::kHistoryTable,
                          warehouse_table_config.get("item_count").get_uint64() * district_table_config.get("item_count").get_uint64() * customer_table_config.get("item_count").get_uint64(),
                          mem_store_alloc_param);

        PopulateCustomerAndHistoryTable(923587856425, mem_store_reserve_param);
        backup_table_ptrs.push_back(customer_table);
        backup_table_ptrs.push_back(customer_index_table);
        backup_table_ptrs.push_back(history_table);
      }
      if ((node_id_t)TPCCTableType::kOrderTable % num_server == (node_id - i + num_server) % num_server) {
        printf("Backup: Initializing Order+OrderIndex+NewOrder+OrderLine table\n");
        std::string warehouse_config_filepath = "../../../workload/tpcc/tpcc_tables/warehouse.json";
        auto warehouse_json_config = JsonConfig::load_file(warehouse_config_filepath);
        auto warehouse_table_config = warehouse_json_config.get("table");
        std::string district_config_filepath = "../../../workload/tpcc/tpcc_tables/district.json";
        auto district_json_config = JsonConfig::load_file(district_config_filepath);
        auto district_table_config = district_json_config.get("table");
        std::string customer_config_filepath = "../../../workload/tpcc/tpcc_tables/customer.json";
        auto customer_json_config = JsonConfig::load_file(customer_config_filepath);
        auto customer_table_config = customer_json_config.get("table");

        order_table =
            new HashStore((table_id_t)TPCCTableType::kOrderTable,
                          warehouse_table_config.get("item_count").get_uint64() * district_table_config.get("item_count").get_uint64() * customer_table_config.get("item_count").get_uint64(),
                          mem_store_alloc_param);
        order_index_table =
            new HashStore((table_id_t)TPCCTableType::kOrderIndexTable,
                          warehouse_table_config.get("item_count").get_uint64() * district_table_config.get("item_count").get_uint64() * customer_table_config.get("item_count").get_uint64(),
                          mem_store_alloc_param);
        new_order_table =
            new HashStore((table_id_t)TPCCTableType::kNewOrderTable,
                          warehouse_table_config.get("item_count").get_uint64() * district_table_config.get("item_count").get_uint64() * customer_table_config.get("item_count").get_uint64() * 0.3,
                          mem_store_alloc_param);
        order_line_table =
            new HashStore((table_id_t)TPCCTableType::kOrderLineTable,
                          warehouse_table_config.get("item_count").get_uint64() * district_table_config.get("item_count").get_uint64() * customer_table_config.get("item_count").get_uint64() * 15,
                          mem_store_alloc_param);

        PopulateOrderNewOrderAndOrderLineTable(2343352, mem_store_reserve_param);
        backup_table_ptrs.push_back(order_table);
        backup_table_ptrs.push_back(order_index_table);
        backup_table_ptrs.push_back(new_order_table);
        backup_table_ptrs.push_back(order_line_table);
      }
      if ((node_id_t)TPCCTableType::kStockTable % num_server == (node_id - i + num_server) % num_server) {
        printf("Backup: Initializing Stock table\n");
        std::string config_filepath = "../../../workload/tpcc/tpcc_tables/stock.json";
        auto json_config = JsonConfig::load_file(config_filepath);
        auto table_config = json_config.get("table");
        stock_table =
            new HashStore((table_id_t)TPCCTableType::kStockTable,
                          table_config.get("item_count").get_uint64(),
                          mem_store_alloc_param);
        PopulateStockTable(89785943, mem_store_reserve_param);
        backup_table_ptrs.push_back(stock_table);
      }
      if ((node_id_t)TPCCTableType::kItemTable % num_server == (node_id - i + num_server) % num_server) {
        printf("Backup: Initializing Item table\n");
        std::string config_filepath = "../../../workload/tpcc/tpcc_tables/item.json";
        auto json_config = JsonConfig::load_file(config_filepath);
        auto table_config = json_config.get("table");
        item_table =
            new HashStore((table_id_t)TPCCTableType::kItemTable,
                          table_config.get("item_count").get_uint64(),
                          mem_store_alloc_param);
        PopulateItemTable(235443, mem_store_reserve_param);
        backup_table_ptrs.push_back(item_table);
      }
    }
  }
}

void TPCC::PopulateWarehouseTable(unsigned long seed, MemStoreReserveParam* mem_store_reserve_param) {
  int total_warehouse_records_inserted = 0, total_warehouse_records_examined = 0;
  FastRandom random_generator(seed);
  //populate warehouse table
  for (uint32_t w_id = 1; w_id <= num_warehouse; w_id++) {
    tpcc_warehouse_key_t warehouse_key;
    warehouse_key.w_id = w_id;

    /* Initialize the warehouse payload */
    tpcc_warehouse_val_t warehouse_val;
    warehouse_val.w_ytd = 300000 * 100;
    //  NOTICE:: scale should check consistency requirements.
    //  W_YTD = sum(D_YTD) where (W_ID = D_W_ID).
    //  W_YTD = sum(H_AMOUNT) where (W_ID = H_W_ID).
    warehouse_val.w_tax = (float)RandomNumber(random_generator, 0, 2000) / 10000.0;
    strcpy(warehouse_val.w_name,
           RandomStr(random_generator, RandomNumber(random_generator, tpcc_warehouse_val_t::MIN_NAME, tpcc_warehouse_val_t::MAX_NAME)).c_str());
    strcpy(warehouse_val.w_street_1,
           RandomStr(random_generator, RandomNumber(random_generator, Address::MIN_STREET, Address::MAX_STREET)).c_str());
    strcpy(warehouse_val.w_street_2,
           RandomStr(random_generator, RandomNumber(random_generator, Address::MIN_STREET, Address::MAX_STREET)).c_str());
    strcpy(warehouse_val.w_city,
           RandomStr(random_generator, RandomNumber(random_generator, Address::MIN_CITY, Address::MAX_CITY)).c_str());
    strcpy(warehouse_val.w_state, RandomStr(random_generator, Address::STATE).c_str());
    strcpy(warehouse_val.w_zip, "123456789");

    assert(warehouse_val.w_state[2] == '\0' && strcmp(warehouse_val.w_zip, "123456789") == 0);
    total_warehouse_records_inserted += LoadRecord(warehouse_table,
                                                   warehouse_key.item_key,
                                                   (void*)&warehouse_val,
                                                   sizeof(tpcc_warehouse_val_t),
                                                   (table_id_t)TPCCTableType::kWarehouseTable,
                                                   mem_store_reserve_param);
    total_warehouse_records_examined++;
  }
  // printf("total_warehouse_records_inserted = %d, total_warehouse_records_examined = %d\n",
  //        total_warehouse_records_inserted, total_warehouse_records_examined);
}
void TPCC::PopulateDistrictTable(unsigned long seed, MemStoreReserveParam* mem_store_reserve_param) {
  int total_district_records_inserted = 0, total_district_records_examined = 0;
  FastRandom random_generator(seed);
  for (uint32_t w_id = 1; w_id <= num_warehouse; w_id++) {
    for (uint32_t d_id = 1; d_id <= num_district_per_warehouse; d_id++) {
      tpcc_district_key_t district_key;
      district_key.d_id = MakeDistrictKey(w_id, d_id);

      /* Initialize the district payload */
      tpcc_district_val_t district_val;

      district_val.d_ytd = 30000 * 100;  // different from warehouse, notice it did the scale up
      //  NOTICE:: scale should check consistency requirements.
      //  D_YTD = sum(H_AMOUNT) where (D_W_ID, D_ID) = (H_W_ID, H_D_ID).
      district_val.d_tax = (float)RandomNumber(random_generator, 0, 2000) / 10000.0;
      district_val.d_next_o_id = num_customer_per_district + 1;
      //  NOTICE:: scale should check consistency requirements.
      //  D_NEXT_O_ID - 1 = max(O_ID) = max(NO_O_ID)

      strcpy(district_val.d_name,
             RandomStr(random_generator, RandomNumber(random_generator, tpcc_district_val_t::MIN_NAME, tpcc_district_val_t::MAX_NAME)).c_str());
      strcpy(district_val.d_street_1,
             RandomStr(random_generator, RandomNumber(random_generator, Address::MIN_STREET, Address::MAX_STREET)).c_str());
      strcpy(district_val.d_street_2,
             RandomStr(random_generator, RandomNumber(random_generator, Address::MIN_STREET, Address::MAX_STREET)).c_str());
      strcpy(district_val.d_city,
             RandomStr(random_generator, RandomNumber(random_generator, Address::MIN_CITY, Address::MAX_CITY)).c_str());
      strcpy(district_val.d_state, RandomStr(random_generator, Address::STATE).c_str());
      strcpy(district_val.d_zip, "123456789");

      total_district_records_inserted += LoadRecord(district_table,
                                                    district_key.item_key,
                                                    (void*)&district_val,
                                                    sizeof(tpcc_district_val_t),
                                                    (table_id_t)TPCCTableType::kDistrictTable,
                                                    mem_store_reserve_param);
      total_district_records_examined++;
    }
  }
  // printf("total_district_records_inserted = %d, total_district_records_examined = %d\n",
  //        total_district_records_inserted, total_district_records_examined);
}

//no batch in this implementation
void TPCC::PopulateCustomerAndHistoryTable(unsigned long seed, MemStoreReserveParam* mem_store_reserve_param) {
  int total_customer_records_inserted = 0, total_customer_records_examined = 0;
  int total_customer_index_records_inserted = 0, total_customer_index_records_examined = 0;
  int total_history_records_inserted = 0, total_history_records_examined = 0;
  // printf("total_customer_records_inserted = %d, total_customer_records_examined = %d\n",
  //        total_customer_records_inserted, total_customer_records_examined);
  FastRandom random_generator(seed);
  printf("num_warehouse = %d, num_district_per_warehouse = %d, num_customer_per_district = %d\n",
         num_warehouse, num_district_per_warehouse, num_customer_per_district);
  for (uint32_t w_id = 1; w_id <= num_warehouse; w_id++) {
    for (uint32_t d_id = 1; d_id <= num_district_per_warehouse; d_id++) {
      for (uint32_t c_id = 1; c_id <= num_customer_per_district; c_id++) {
        tpcc_customer_key_t customer_key;
        customer_key.c_id = MakeCustomerKey(w_id, d_id, c_id);

        tpcc_customer_val_t customer_val;
        customer_val.c_discount = (float)(RandomNumber(random_generator, 1, 5000) / 10000.0);
        if (RandomNumber(random_generator, 1, 100) <= 10)
          strcpy(customer_val.c_credit, "BC");
        else
          strcpy(customer_val.c_credit, "GC");
        std::string c_last;
        if (c_id <= num_customer_per_district / 3) {
          c_last.assign(GetCustomerLastName(random_generator, c_id - 1));
          strcpy(customer_val.c_last, c_last.c_str());
        } else {
          c_last.assign(GetNonUniformCustomerLastNameLoad(random_generator));
          strcpy(customer_val.c_last, c_last.c_str());
        }

        std::string c_first = RandomStr(random_generator, RandomNumber(random_generator, tpcc_customer_val_t::MIN_FIRST, tpcc_customer_val_t::MAX_FIRST));
        strcpy(customer_val.c_first, c_first.c_str());

        customer_val.c_credit_lim = 50000;

        customer_val.c_balance = -10;
        customer_val.c_ytd_payment = 10;
        customer_val.c_payment_cnt = 1;
        customer_val.c_delivery_cnt = 0;
        strcpy(customer_val.c_street_1,
               RandomStr(random_generator, RandomNumber(random_generator, Address::MIN_STREET, Address::MAX_STREET)).c_str());
        strcpy(customer_val.c_street_2,
               RandomStr(random_generator, RandomNumber(random_generator, Address::MIN_STREET, Address::MAX_STREET)).c_str());
        strcpy(customer_val.c_city,
               RandomStr(random_generator, RandomNumber(random_generator, Address::MIN_CITY, Address::MAX_CITY)).c_str());
        strcpy(customer_val.c_state, RandomStr(random_generator, Address::STATE).c_str());
        strcpy(customer_val.c_zip, (RandomNStr(random_generator, 4) + "11111").c_str());

        strcpy(customer_val.c_phone, RandomNStr(random_generator, tpcc_customer_val_t::PHONE).c_str());
        customer_val.c_since = GetCurrentTimeMillis();
        strcpy(customer_val.c_middle, "OE");
        strcpy(customer_val.c_data,
               RandomStr(random_generator, RandomNumber(random_generator, tpcc_customer_val_t::MIN_DATA, tpcc_customer_val_t::MAX_DATA)).c_str());

        assert(!strcmp(customer_val.c_credit, "BC") || !strcmp(customer_val.c_credit, "GC"));
        assert(!strcmp(customer_val.c_middle, "OE"));
        // printf("before insert customer record\n");

        total_customer_records_inserted += LoadRecord(customer_table,
                                                      customer_key.item_key,
                                                      (void*)&customer_val,
                                                      sizeof(tpcc_customer_val_t),
                                                      (table_id_t)TPCCTableType::kCustomerTable,
                                                      mem_store_reserve_param);
        total_customer_records_examined++;

        // printf("total_customer_records_inserted = %d, total_customer_records_examined = %d\n", total_customer_records_inserted, total_customer_records_examined);

        tpcc_customer_index_key_t customer_index_key;
        //TODO:: MakeCustomerIndexKey may have some problem \
                even the same <w_id, d_id, c_last, c_first> will cause different customer_index_key
        customer_index_key.item_key = MakeCustomerIndexKey(w_id, d_id, c_last, c_first);

        tpcc_customer_index_val_t customer_index_val;
        customer_index_val.debug_magic = tpcc_add_magic;
        DataItem* mn = GetRecord(customer_index_table,
                                 customer_index_key.item_key,
                                 (table_id_t)TPCCTableType::kCustomerIndexTable);
        assert(mn == NULL);
        if (mn == NULL) {
          customer_index_val.c_id = customer_key.c_id;
          total_customer_index_records_inserted += LoadRecord(customer_index_table,
                                                              customer_index_key.item_key,
                                                              (void*)&customer_index_val,
                                                              sizeof(tpcc_customer_index_val_t),
                                                              (table_id_t)TPCCTableType::kCustomerIndexTable,
                                                              mem_store_reserve_param);
          total_customer_index_records_examined++;
          // printf("total_customer_index_records_inserted = %d, total_customer_index_records_examined = %d\n", total_customer_index_records_inserted, total_customer_index_records_examined);
        }

        tpcc_history_key_t history_key;
        history_key.h_id = MakeHistoryKey(w_id, d_id, w_id, d_id, c_id);
        tpcc_history_val_t history_val;
        history_val.h_date = GetCurrentTimeMillis();
        history_val.h_amount = 10;
        strcpy(history_val.h_data,
               RandomStr(random_generator, RandomNumber(random_generator, tpcc_history_val_t::MIN_DATA, tpcc_history_val_t::MAX_DATA)).c_str());

        total_history_records_inserted += LoadRecord(history_table,
                                                     history_key.item_key,
                                                     (void*)&history_val,
                                                     sizeof(tpcc_history_val_t),
                                                     (table_id_t)TPCCTableType::kHistoryTable,
                                                     mem_store_reserve_param);
        total_history_records_examined++;
        // printf("total_history_records_inserted = %d, total_history_records_examined = %d\n", total_history_records_inserted, total_history_records_examined);
      }
    }
  }

  // printf("total_customer_records_inserted = %d, total_customer_records_examined = %d\n", total_customer_records_inserted, total_customer_records_examined);
  // printf("total_customer_index_records_inserted = %d, total_customer_index_records_examined = %d\n", total_customer_index_records_inserted, total_customer_index_records_examined);
  // printf("total_history_records_inserted = %d, total_history_records_examined = %d\n", total_history_records_inserted, total_history_records_examined);
}

void TPCC::PopulateOrderNewOrderAndOrderLineTable(unsigned long seed, MemStoreReserveParam* mem_store_reserve_param) {
  int total_order_records_inserted = 0, total_order_records_examined = 0;
  int total_order_index_records_inserted = 0, total_order_index_records_examined = 0;
  int total_new_order_records_inserted = 0, total_new_order_records_examined = 0;
  int total_order_line_records_inserted = 0, total_order_line_records_examined = 0;
  FastRandom random_generator(seed);
  // printf("total_order_records_inserted = %d, total_order_records_examined = %d\n", total_order_records_inserted, total_order_records_examined);
  for (uint32_t w_id = 1; w_id <= num_warehouse; w_id++) {
    for (uint32_t d_id = 1; d_id <= num_district_per_warehouse; d_id++) {
      std::set<uint32_t> c_ids_s;
      std::vector<uint32_t> c_ids;
      while (c_ids.size() != num_customer_per_district) {
        const auto x = (random_generator.Next() % num_customer_per_district) + 1;
        if (c_ids_s.count(x))
          continue;
        c_ids_s.insert(x);
        c_ids.emplace_back(x);
      }
      for (uint32_t c = 1; c <= num_customer_per_district; c++) {
        tpcc_order_key_t order_key;
        order_key.o_id = MakeOrderKey(w_id, d_id, c);

        tpcc_order_val_t order_val;
        order_val.o_c_id = c_ids[c - 1];
        if (c <= num_customer_per_district * 0.7)
          order_val.o_carrier_id = RandomNumber(random_generator, tpcc_order_val_t::MIN_CARRIER_ID, tpcc_order_val_t::MAX_CARRIER_ID);
        else
          order_val.o_carrier_id = 0;
        order_val.o_ol_cnt = RandomNumber(random_generator, tpcc_order_line_val_t::MIN_OL_CNT, tpcc_order_line_val_t::MAX_OL_CNT);

        order_val.o_all_local = 1;
        order_val.o_entry_d = GetCurrentTimeMillis();

        total_order_records_inserted += LoadRecord(order_table,
                                                   order_key.item_key,
                                                   (void*)&order_val,
                                                   sizeof(tpcc_order_val_t),
                                                   (table_id_t)TPCCTableType::kOrderTable,
                                                   mem_store_reserve_param);
        total_order_records_examined++;
        // printf("total_order_records_inserted = %d, total_order_records_examined = %d\n", total_order_records_inserted, total_order_records_examined);

        tpcc_order_index_key_t order_index_key;
        order_index_key.item_key = MakeOrderIndexKey(w_id, d_id, order_val.o_c_id, c);

        tpcc_order_index_val_t order_index_val;
        order_index_val.o_id = order_key.o_id;

        DataItem* mn = GetRecord(order_index_table,
                                 order_index_key.item_key,
                                 (table_id_t)TPCCTableType::kOrderIndexTable);
        assert(mn == NULL);
        if (mn == NULL) {
          order_index_val.o_id = order_key.o_id;
          order_index_val.debug_magic = tpcc_add_magic;
          total_order_index_records_inserted += LoadRecord(order_index_table,
                                                           order_index_key.item_key,
                                                           (void*)&order_index_val,
                                                           sizeof(tpcc_order_index_val_t),
                                                           (table_id_t)TPCCTableType::kOrderIndexTable,
                                                           mem_store_reserve_param);
          total_order_index_records_examined++;
          // printf("total_order_index_records_inserted = %d, total_order_index_records_examined = %d\n", total_order_index_records_inserted, total_order_index_records_examined);
        }

        if (c > num_customer_per_district * tpcc_new_order_val_t::SCALE_CONSTANT_BETWEEN_NEWORDER_ORDER) {
          // MZ-Notation: must obey the relationship between the numbers of entries in Order and New-Order specified in tpcc docs
          // The number of entries in New-Order is about 30% of that in Order
          tpcc_new_order_key_t new_order_key;
          new_order_key.no_id = MakeNewOrderKey(w_id, d_id, c);

          tpcc_new_order_val_t new_order_val;
          new_order_val.debug_magic = tpcc_add_magic;
          total_new_order_records_inserted += LoadRecord(new_order_table,
                                                         new_order_key.item_key,
                                                         (void*)&new_order_val,
                                                         sizeof(tpcc_new_order_val_t),
                                                         (table_id_t)TPCCTableType::kNewOrderTable,
                                                         mem_store_reserve_param);
          total_new_order_records_examined++;
          // printf("total_new_order_records_inserted = %d, total_new_order_records_examined = %d\n", total_new_order_records_inserted, total_new_order_records_examined);
        }
        for (uint32_t l = 1; l <= uint32_t(order_val.o_ol_cnt); l++) {
          tpcc_order_line_key_t order_line_key;
          order_line_key.ol_id = MakeOrderLineKey(w_id, d_id, c, l);

          tpcc_order_line_val_t order_line_val;
          order_line_val.ol_i_id = RandomNumber(random_generator, 1, num_item);
          if (c <= num_customer_per_district * 0.7) {
            order_line_val.ol_delivery_d = order_val.o_entry_d;
            order_line_val.ol_amount = 0;
          } else {
            order_line_val.ol_delivery_d = 0;
            /* random within [0.01 .. 9,999.99] */
            order_line_val.ol_amount = (float)(RandomNumber(random_generator, 1, 999999) / 100.0);
          }

          order_line_val.ol_supply_w_id = w_id;
          order_line_val.ol_quantity = 5;
          // order_line_val.ol_dist_info comes from stock_data(ol_supply_w_id, ol_o_id)

          order_line_val.debug_magic = tpcc_add_magic;
          assert(order_line_val.ol_i_id >= 1 && static_cast<size_t>(order_line_val.ol_i_id) <= num_item);
          total_order_line_records_inserted += LoadRecord(order_line_table,
                                                          order_line_key.item_key,
                                                          (void*)&order_line_val,
                                                          sizeof(tpcc_order_line_val_t),
                                                          (table_id_t)TPCCTableType::kOrderLineTable,
                                                          mem_store_reserve_param);
          total_order_line_records_examined++;
          // printf("total_order_line_records_inserted = %d, total_order_line_records_examined = %d\n", total_order_line_records_inserted, total_order_line_records_examined);
        }
      }
    }
  }
  // printf("total_order_records_inserted = %d, total_order_records_examined = %d\n", total_order_records_inserted, total_order_records_examined);

  RDMA_LOG(INFO) << "total_order_records_inserted = " << total_order_records_inserted << ", total_order_records_examined = " << total_order_records_examined;

  // printf("total_order_index_records_inserted = %d, total_order_index_records_examined = %d\n",
  //        total_order_index_records_inserted, total_order_index_records_examined);
  // printf("total_new_order_records_inserted = %d, total_new_order_records_examined = %d\n",
  //        total_new_order_records_inserted, total_new_order_records_examined);
  // printf("total_order_line_records_inserted = %d, total_order_line_records_examined = %d\n",
  //        total_order_line_records_inserted, total_order_line_records_examined);
}

void TPCC::PopulateItemTable(unsigned long seed, MemStoreReserveParam* mem_store_reserve_param) {
  int total_item_records_inserted = 0, total_item_records_examined = 0;
  //    printf("total_item_records_inserted = %d, total_item_records_examined = %d\n",
  //           total_item_records_inserted, total_item_records_examined);
  FastRandom random_generator(seed);
  for (int64_t i_id = 1; i_id <= num_item; i_id++) {
    tpcc_item_key_t item_key;
    item_key.i_id = i_id;

    /* Initialize the item payload */
    tpcc_item_val_t item_val;

    strcpy(item_val.i_name,
           RandomStr(random_generator, RandomNumber(random_generator, tpcc_item_val_t::MIN_NAME, tpcc_item_val_t::MAX_NAME)).c_str());
    item_val.i_price = (float)(RandomNumber(random_generator, 100, 10000) / 100.0);
    const int len = RandomNumber(random_generator, tpcc_item_val_t::MIN_DATA, tpcc_item_val_t::MAX_DATA);
    if (RandomNumber(random_generator, 1, 100) > 10) {
      strcpy(item_val.i_data, RandomStr(random_generator, len).c_str());
    } else {
      const int startOriginal = RandomNumber(random_generator, 2, (len - 8));
      const std::string i_data = RandomStr(random_generator, startOriginal) +
                                 "ORIGINAL" + RandomStr(random_generator, len - startOriginal - 8);
      strcpy(item_val.i_data, i_data.c_str());
    }
    item_val.i_im_id = RandomNumber(random_generator, tpcc_item_val_t::MIN_IM, tpcc_item_val_t::MAX_IM);
    item_val.debug_magic = tpcc_add_magic;
    //check item price
    assert(item_val.i_price >= 1.0 && item_val.i_price <= 100.0);

    total_item_records_inserted += LoadRecord(item_table,
                                              item_key.item_key,
                                              (void*)&item_val,
                                              sizeof(tpcc_item_val_t),
                                              (table_id_t)TPCCTableType::kItemTable,
                                              mem_store_reserve_param);
    total_item_records_examined++;
    // printf("total_item_records_inserted = %d, total_item_records_examined = %d\n", total_item_records_inserted, total_item_records_examined);
  }
  // printf("total_item_records_inserted = %d, total_item_records_examined = %d\n", total_item_records_inserted, total_item_records_examined);
}

void TPCC::PopulateStockTable(unsigned long seed, MemStoreReserveParam* mem_store_reserve_param) {
  int total_stock_records_inserted = 0, total_stock_records_examined = 0;
  for (uint32_t w_id = 1; w_id <= num_warehouse; w_id++) {
    for (uint32_t i_id = 1; i_id <= num_item; i_id++) {
      tpcc_stock_key_t stock_key;
      stock_key.s_id = MakeStockKey(w_id, i_id);

      /* Initialize the stock payload */
      tpcc_stock_val_t stock_val;
      FastRandom random_generator(seed);
      stock_val.s_quantity = RandomNumber(random_generator, 10, 100);
      stock_val.s_ytd = 0;
      stock_val.s_order_cnt = 0;
      stock_val.s_remote_cnt = 0;

      const int len = RandomNumber(random_generator, tpcc_stock_val_t::MIN_DATA, tpcc_stock_val_t::MAX_DATA);
      if (RandomNumber(random_generator, 1, 100) > 10) {
        const std::string s_data = RandomStr(random_generator, len);
        strcpy(stock_val.s_data, s_data.c_str());
      } else {
        const int startOriginal = RandomNumber(random_generator, 2, (len - 8));
        const std::string s_data = RandomStr(random_generator, startOriginal) + "ORIGINAL" + RandomStr(random_generator, len - startOriginal - 8);
        strcpy(stock_val.s_data, s_data.c_str());
      }

      stock_val.debug_magic = tpcc_add_magic;
      total_stock_records_inserted += LoadRecord(stock_table,
                                                 stock_key.item_key,
                                                 (void*)&stock_val,
                                                 sizeof(tpcc_stock_val_t),
                                                 (table_id_t)TPCCTableType::kStockTable,
                                                 mem_store_reserve_param);
      total_stock_records_examined++;
      // printf("total_stock_records_inserted = %d, total_stock_records_examined = %d\n", total_stock_records_inserted, total_stock_records_examined);
    }
  }
  // printf("total_stock_records_inserted = %d, total_stock_records_examined = %d\n", total_stock_records_inserted, total_stock_records_examined);
}

int TPCC::LoadRecord(HashStore* table,
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

DataItem* TPCC::GetRecord(HashStore* table,
                          itemkey_t item_key,
                          table_id_t table_id) {
  /* get from HashStore */
  return table->LocalGet(item_key);
}
