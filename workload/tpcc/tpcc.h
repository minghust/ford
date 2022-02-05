// Author: Ming Zhang, Lurong Liu
// Copyright (c) 2021

#pragma once

#include <cassert>
#include <cstdint>
#include <set>
#include <vector>

#include "config/table_type.h"
#include "memstore/hash_store.h"
#include "util/fast_random.h"
#include "util/json_config.h"

// YYYY-MM-DD HH:MM:SS This is supposed to be a date/time field from Jan 1st 1900 -
// Dec 31st 2100 with a resolution of 1 second. See TPC-C 5.11.0.
// static const int DATETIME_SIZE = 14;
// Use uint32 for data and time
static int g_uniform_item_dist = 0;

static int g_new_order_remote_item_pct = 1;

static int g_mico_dist_num = 20;

static const size_t CustomerLastNameMaxSize = 16;

static std::string NameTokens[10] = {
    std::string("BAR"),
    std::string("OUGHT"),
    std::string("ABLE"),
    std::string("PRI"),
    std::string("PRES"),
    std::string("ESE"),
    std::string("ANTI"),
    std::string("CALLY"),
    std::string("ATION"),
    std::string("EING"),
};

const char GOOD_CREDIT[] = "GC";

const char BAD_CREDIT[] = "BC";

static const int DUMMY_SIZE = 12;

static const int DIST = 24;

static const int NUM_DISTRICT_PER_WAREHOUSE = 10;

// Constants
struct Address {
  static const int MIN_STREET = 10;  //W_STREET_1 random a-string [10 .. 20] W_STREET_2 random a-string [10 .. 20]
  static const int MAX_STREET = 20;
  static const int MIN_CITY = 10;  //W_CITY random a-string [10 .. 20]
  static const int MAX_CITY = 20;
  static const int STATE = 2;  // W_STATE random a-string of 2 letters
  static const int ZIP = 9;    // ZIP a-string of 9 letters
};

/******************** TPCC table definitions (Schemas of key and value) start **********************/
/*
 * Warehouse table
 * Primary key: <int32_t w_id>
*/

union tpcc_warehouse_key_t {
  struct {
    int32_t w_id;
    uint8_t unused[4];
  };
  itemkey_t item_key;

  tpcc_warehouse_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tpcc_warehouse_key_t) == sizeof(itemkey_t), "");

struct tpcc_warehouse_val_t {
  static const int MIN_NAME = 6;
  static const int MAX_NAME = 10;

  float w_tax;
  float w_ytd;
  char w_name[MAX_NAME + 1];
  char w_street_1[Address::MAX_STREET + 1];
  char w_street_2[Address::MAX_STREET + 1];
  char w_city[Address::MAX_CITY + 1];
  char w_state[Address::STATE + 1];
  char w_zip[Address::ZIP + 1];
};

static_assert(sizeof(tpcc_warehouse_val_t) == 96, "");

/*
 * District table
 * Primary key: <int32_t d_id, int32_t d_w_id>
*/

union tpcc_district_key_t {
  struct {
    int64_t d_id;
  };
  itemkey_t item_key;

  tpcc_district_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tpcc_district_key_t) == sizeof(itemkey_t), "");

struct tpcc_district_val_t {
  static const int MIN_NAME = 6;
  static const int MAX_NAME = 10;

  float d_tax;
  float d_ytd;
  int32_t d_next_o_id;
  char d_name[MAX_NAME + 1];
  char d_street_1[Address::MAX_STREET + 1];
  char d_street_2[Address::MAX_STREET + 1];
  char d_city[Address::MAX_CITY + 1];
  char d_state[Address::STATE + 1];
  char d_zip[Address::ZIP + 1];
};

static_assert(sizeof(tpcc_district_val_t) == 100, "");

/*
 * Customer table
 * Primary key: <int32_t c_id, int32_t c_d_id, int32_t c_w_id>
*/

union tpcc_customer_key_t {
  int64_t c_id;
  itemkey_t item_key;

  tpcc_customer_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tpcc_customer_key_t) == sizeof(itemkey_t), "");

struct tpcc_customer_val_t {
  static const int MIN_FIRST = 8;  //C_FIRST random a-string [8 .. 16]
  static const int MAX_FIRST = 16;
  static const int MIDDLE = 2;
  static const int MAX_LAST = 16;
  static const int PHONE = 16;  //C_PHONE random n-string of 16 numbers
  static const int CREDIT = 2;
  static const int MIN_DATA = 300;  //C_DATA random a-string [300 .. 500]
  static const int MAX_DATA = 500;

  float c_credit_lim;
  float c_discount;
  float c_balance;
  float c_ytd_payment;
  int32_t c_payment_cnt;
  int32_t c_delivery_cnt;
  char c_first[MAX_FIRST + 1];
  char c_middle[MIDDLE + 1];
  char c_last[MAX_LAST + 1];
  char c_street_1[Address::MAX_STREET + 1];
  char c_street_2[Address::MAX_STREET + 1];
  char c_city[Address::MAX_CITY + 1];
  char c_state[Address::STATE + 1];
  char c_zip[Address::ZIP + 1];
  char c_phone[PHONE + 1];
  uint32_t c_since;
  char c_credit[CREDIT + 1];
  char c_data[MAX_DATA + 1];
};

static_assert(sizeof(tpcc_customer_val_t) == 664, "");

union tpcc_customer_index_key_t {
  struct {
    uint64_t c_index_id;
  };
  itemkey_t item_key;

  tpcc_customer_index_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tpcc_customer_index_key_t) == sizeof(itemkey_t), "");

struct tpcc_customer_index_val_t {
  int64_t c_id;
  int64_t debug_magic;
};

static_assert(sizeof(tpcc_customer_index_val_t) == 16, "");  // add debug magic
//static_assert(sizeof(tpcc_customer_index_val_t) == 8, "");

/*
 * History table
 * Primary key: none
*/

union tpcc_history_key_t {
  int64_t h_id;
  itemkey_t item_key;

  tpcc_history_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tpcc_history_key_t) == sizeof(itemkey_t), "");

struct tpcc_history_val_t {
  static const int MIN_DATA = 12;  //H_DATA random a-string [12 .. 24] from TPCC documents 5.11
  static const int MAX_DATA = 24;

  float h_amount;
  uint32_t h_date;
  char h_data[MAX_DATA + 1];
};

static_assert(sizeof(tpcc_history_val_t) == 36, "");

/*
 * NewOrder table
 * Primary key: <int32_t no_w_id, int32_t no_d_id, int32_t no_o_id>
*/
union tpcc_new_order_key_t {
  int64_t no_id;
  itemkey_t item_key;

  tpcc_new_order_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tpcc_new_order_key_t) == sizeof(itemkey_t), "");

struct tpcc_new_order_val_t {
  static constexpr double SCALE_CONSTANT_BETWEEN_NEWORDER_ORDER = 0.7;

  char no_dummy[DUMMY_SIZE + 1];
  int64_t debug_magic;
};

static_assert(sizeof(tpcc_new_order_val_t) == 24, "");  // add debug magic
//static_assert(sizeof(tpcc_new_order_val_t) == 13, "");

/*
 * Order table
 * Primary key: <int32_t o_w_id, int32_t o_d_id, int32_t o_id>
*/
union tpcc_order_key_t {
  int64_t o_id;
  itemkey_t item_key;

  tpcc_order_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tpcc_order_key_t) == sizeof(itemkey_t), "");

struct tpcc_order_val_t {
  static const int MIN_CARRIER_ID = 1;
  static const int MAX_CARRIER_ID = 10;  // number of distinct per warehouse

  int32_t o_c_id;
  int32_t o_carrier_id;
  int32_t o_ol_cnt;
  int32_t o_all_local;
  uint32_t o_entry_d;
};

static_assert(sizeof(tpcc_order_val_t) == 20, "");

union tpcc_order_index_key_t {
  int64_t o_index_id;
  itemkey_t item_key;

  tpcc_order_index_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tpcc_order_index_key_t) == sizeof(itemkey_t), "");

struct tpcc_order_index_val_t {
  uint64_t o_id;
  int64_t debug_magic;
};

static_assert(sizeof(tpcc_order_index_val_t) == 16, "");  // add debug magic
//static_assert(sizeof(tpcc_order_index_val_t) == 8, "");

/*
 * OrderLine table
 * Primary key: <int32_t ol_o_id, int32_t ol_d_id, int32_t ol_w_id, int32_t ol_number>
*/

union tpcc_order_line_key_t {
  int64_t ol_id;
  itemkey_t item_key;

  tpcc_order_line_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tpcc_order_line_key_t) == sizeof(itemkey_t), "");

struct tpcc_order_line_val_t {
  static const int MIN_OL_CNT = 5;
  static const int MAX_OL_CNT = 15;

  int32_t ol_i_id;
  int32_t ol_supply_w_id;
  int32_t ol_quantity;
  float ol_amount;
  uint32_t ol_delivery_d;
  char ol_dist_info[DIST + 1];
  int64_t debug_magic;
};

static_assert(sizeof(tpcc_order_line_val_t) == 56, "");  // add debug magic
//static_assert(sizeof(tpcc_order_line_val_t) == 48, "");

/*
 * Item table
 * Primary key: <int32_t i_id>
*/

union tpcc_item_key_t {
  struct {
    int64_t i_id;
  };
  itemkey_t item_key;

  tpcc_item_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tpcc_item_key_t) == sizeof(itemkey_t), "");

struct tpcc_item_val_t {
  static const int MIN_NAME = 14;  //I_NAME random a-string [14 .. 24]
  static const int MAX_NAME = 24;
  static const int MIN_DATA = 26;
  static const int MAX_DATA = 50;  //I_DATA random a-string [26 .. 50]

  static const int MIN_IM = 1;
  static const int MAX_IM = 10000;

  int32_t i_im_id;
  float i_price;
  char i_name[MAX_NAME + 1];
  char i_data[MAX_DATA + 1];
  int64_t debug_magic;
};

static_assert(sizeof(tpcc_item_val_t) == 96, "");  // add debug magic
//static_assert(sizeof(tpcc_item_val_t) == 84, "");

/*
 * Stock table
 * Primary key: <int32_t s_i_id, int32_t s_w_id>
*/

union tpcc_stock_key_t {
  struct {
    int64_t s_id;
  };
  itemkey_t item_key;

  tpcc_stock_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tpcc_stock_key_t) == sizeof(itemkey_t), "");

struct tpcc_stock_val_t {
  static const int MIN_DATA = 26;
  static const int MAX_DATA = 50;
  static const int32_t MIN_STOCK_LEVEL_THRESHOLD = 10;
  static const int32_t MAX_STOCK_LEVEL_THRESHOLD = 20;
  static const int STOCK_LEVEL_ORDERS = 20;

  int32_t s_quantity;
  int32_t s_ytd;
  int32_t s_order_cnt;
  int32_t s_remote_cnt;
  char s_dist[NUM_DISTRICT_PER_WAREHOUSE][DIST + 1];
  char s_data[MAX_DATA + 1];
  int64_t debug_magic;
};

static_assert(sizeof(tpcc_stock_val_t) == 328, "");  // add debug magic
//static_assert(sizeof(tpcc_stock_val_t) == 320, "");

#if 0
// TPCC schema references: https://github.com/evanj/tpccbench
struct Warehouse {
    static constexpr float MIN_TAX = 0;
    static constexpr float MAX_TAX = 0.2000f;
    static constexpr float INITIAL_YTD = 300000.00f;
    static const int MIN_NAME = 6;
    static const int MAX_NAME = 10;
    // TPC-C 1.3.1 (page 11) requires 2*W. This permits testing up to 50 warehouses. This is an
    // arbitrary limit created to pack ids into integers.
    static const int MAX_WAREHOUSE_ID = 100;

    int32_t w_id;
    float w_tax;
    float w_ytd;
    char w_name[MAX_NAME + 1];
    char w_street_1[Address::MAX_STREET + 1];
    char w_street_2[Address::MAX_STREET + 1];
    char w_city[Address::MAX_CITY + 1];
    char w_state[Address::STATE + 1];
    char w_zip[Address::ZIP + 1];
};

struct District {
    static constexpr float MIN_TAX = 0;
    static constexpr float MAX_TAX = 0.2000f;
    static constexpr float INITIAL_YTD = 30000.00;  // different from Warehouse
    static const int INITIAL_NEXT_O_ID = 3001;
    static const int MIN_NAME = 6;
    static const int MAX_NAME = 10;
    static const int NUM_PER_WAREHOUSE = 10;

    int32_t d_id;
    int32_t d_w_id;
    float d_tax;
    float d_ytd;
    int32_t d_next_o_id;
    char d_name[MAX_NAME + 1];
    char d_street_1[Address::MAX_STREET + 1];
    char d_street_2[Address::MAX_STREET + 1];
    char d_city[Address::MAX_CITY + 1];
    char d_state[Address::STATE + 1];
    char d_zip[Address::ZIP + 1];
};

struct Customer {
    static constexpr float INITIAL_CREDIT_LIM = 50000.00;
    static constexpr float MIN_DISCOUNT = 0.0000;
    static constexpr float MAX_DISCOUNT = 0.5000;
    static constexpr float INITIAL_BALANCE = -10.00;
    static constexpr float INITIAL_YTD_PAYMENT = 10.00;
    static const int INITIAL_PAYMENT_CNT = 1;
    static const int INITIAL_DELIVERY_CNT = 0;
    static const int MIN_FIRST = 6;
    static const int MAX_FIRST = 10;
    static const int MIDDLE = 2;
    static const int MAX_LAST = 16;
    static const int PHONE = 16;
    static const int CREDIT = 2;
    static const int MIN_DATA = 300;
    static const int MAX_DATA = 500;
    static const int NUM_PER_DISTRICT = 3000;
    static const char GOOD_CREDIT[];
    static const char BAD_CREDIT[];

    int32_t c_id;
    int32_t c_d_id;
    int32_t c_w_id;
    float c_credit_lim;
    float c_discount;
    float c_balance;
    float c_ytd_payment;
    int32_t c_payment_cnt;
    int32_t c_delivery_cnt;
    char c_first[MAX_FIRST + 1];
    char c_middle[MIDDLE + 1];
    char c_last[MAX_LAST + 1];
    char c_street_1[Address::MAX_STREET + 1];
    char c_street_2[Address::MAX_STREET + 1];
    char c_city[Address::MAX_CITY + 1];
    char c_state[Address::STATE + 1];
    char c_zip[Address::ZIP + 1];
    char c_phone[PHONE + 1];
    char c_since[DATETIME_SIZE + 1];
    char c_credit[CREDIT + 1];
    char c_data[MAX_DATA + 1];
};

struct History {
    static const int MIN_DATA = 12;
    static const int MAX_DATA = 24;
    static constexpr float INITIAL_AMOUNT = 10.00f;

    int32_t h_c_id;
    int32_t h_c_d_id;
    int32_t h_c_w_id;
    int32_t h_d_id;
    int32_t h_w_id;
    float h_amount;
    char h_date[DATETIME_SIZE + 1];
    char h_data[MAX_DATA + 1];
};


struct NewOrder {
    static const int INITIAL_NUM_PER_DISTRICT = 900;

    int32_t no_w_id;
    int32_t no_d_id;
    int32_t no_o_id;
};

struct Order {
    static const int MIN_CARRIER_ID = 1;
    static const int MAX_CARRIER_ID = 10;
    // HACK: This is not strictly correct, but it works
    static const int NULL_CARRIER_ID = 0;
    // Less than this value, carrier != null, >= -> carrier == null
    static const int NULL_CARRIER_LOWER_BOUND = 2101;
    static const int MIN_OL_CNT = 5;
    static const int MAX_OL_CNT = 15;
    static const int INITIAL_ALL_LOCAL = 1;
    static const int INITIAL_ORDERS_PER_DISTRICT = 3000;
    // See TPC-C 1.3.1 (page 15)
    static const int MAX_ORDER_ID = 10000000;

    int32_t o_id;
    int32_t o_c_id;
    int32_t o_d_id;
    int32_t o_w_id;
    int32_t o_carrier_id;
    int32_t o_ol_cnt;
    int32_t o_all_local;
    char o_entry_d[DATETIME_SIZE+1];
};

struct OrderLine {
    static const int MIN_I_ID = 1;
    static const int MAX_I_ID = 100000;  // Item::NUM_ITEMS
    static const int INITIAL_QUANTITY = 5;
    static constexpr float MIN_AMOUNT = 0.01f;
    static constexpr float MAX_AMOUNT = 9999.99f;
    // new order has 10/1000 probability of selecting a remote warehouse for ol_supply_w_id
    static const int REMOTE_PROBABILITY_MILLIS = 10;

    int32_t ol_o_id;
    int32_t ol_d_id;
    int32_t ol_w_id;
    int32_t ol_number;
    int32_t ol_i_id;
    int32_t ol_supply_w_id;
    int32_t ol_quantity;
    float ol_amount;
    char ol_delivery_d[DATETIME_SIZE+1];
    char ol_dist_info[Stock::DIST+1];
};

struct Item {
    static const int MIN_IM = 1;
    static const int MAX_IM = 10000;
    static constexpr float MIN_PRICE = 1.00;
    static constexpr float MAX_PRICE = 100.00;
    static const int MIN_NAME = 14;
    static const int MAX_NAME = 24;
    static const int MIN_DATA = 26;
    static const int MAX_DATA = 50;
    static const int NUM_ITEMS = 100000;

    int32_t i_id;
    int32_t i_im_id;
    float i_price;
    char i_name[MAX_NAME+1];
    char i_data[MAX_DATA+1];
};

struct Stock {
    static const int MIN_QUANTITY = 10;
    static const int MAX_QUANTITY = 100;
    static const int DIST = 24;
    static const int MIN_DATA = 26;
    static const int MAX_DATA = 50;
    static const int NUM_STOCK_PER_WAREHOUSE = 100000;

    int32_t s_i_id;
    int32_t s_w_id;
    int32_t s_quantity;
    int32_t s_ytd;
    int32_t s_order_cnt;
    int32_t s_remote_cnt;
    char s_dist[District::NUM_PER_WAREHOUSE][DIST+1];
    char s_data[MAX_DATA+1];
};
#endif

/******************** TPCC table definitions (Schemas of key and value) end **********************/

// Magic numbers for debugging. These are unused in the spec.
const std::string tpcc_zip_magic("123456789");  // warehouse, district
const uint32_t tpcc_no_time_magic = 0;          //customer, history, order
const int64_t tpcc_add_magic = 818;             //customer_index, order_index, new_order, order_line, item, stock

#define TATP_MAGIC 97 /* Some magic number <= 255 */
#define tatp_sub_msc_location_magic (TATP_MAGIC)
#define tatp_sec_sub_magic (TATP_MAGIC + 1)
#define tatp_accinf_data1_magic (TATP_MAGIC + 2)
#define tatp_specfac_data_b0_magic (TATP_MAGIC + 3)
#define tatp_callfwd_numberx0_magic (TATP_MAGIC + 4)

/* STORED PROCEDURE EXECUTION FREQUENCIES (0-100) */
#define FREQUENCY_NEW_ORDER 45
#define FREQUENCY_PAYMENT 43
#define FREQUENCY_ORDER_STATUS 4
#define FREQUENCY_DELIVERY 4
#define FREQUENCY_STOCK_LEVEL 4

// Transaction workload type
#define TPCC_TX_TYPES 5
enum class TPCCTxType {
  kNewOrder = 0,
  kPayment,
  kDelivery,
  kOrderStatus,
  kStockLevel,
};

// Table id
enum class TPCCTableType : uint64_t {
  kWarehouseTable = TABLE_TPCC,  // 48076
  kDistrictTable,
  kCustomerTable,
  kHistoryTable,
  kNewOrderTable,  // 48080
  kOrderTable,
  kOrderLineTable,
  kItemTable,
  kStockTable,  // 48084
  kCustomerIndexTable,
  kOrderIndexTable,  // 48086
};

class TPCC {
 public:
  // Pre-defined constants, which will be modified for tests
  uint32_t num_warehouse = 3000;

  uint32_t num_district_per_warehouse = 10;

  uint32_t num_customer_per_district = 3000;

  uint32_t num_item = 100000;

  uint32_t num_stock_per_warehouse = 100000;

  /* Tables */
  HashStore* warehouse_table = nullptr;

  HashStore* district_table = nullptr;

  HashStore* customer_table = nullptr;

  HashStore* history_table = nullptr;

  HashStore* new_order_table = nullptr;

  HashStore* order_table = nullptr;

  HashStore* order_line_table = nullptr;

  HashStore* item_table = nullptr;

  HashStore* stock_table = nullptr;

  HashStore* customer_index_table = nullptr;

  HashStore* order_index_table = nullptr;

  std::vector<HashStore*> primary_table_ptrs;

  std::vector<HashStore*> backup_table_ptrs;

  // For server and client usage: Provide interfaces to servers for loading tables
  TPCC() {
    std::string warehouse_config_filepath = "../../../workload/tpcc/tpcc_tables/warehouse.json";
    auto warehouse_json_config = JsonConfig::load_file(warehouse_config_filepath);
    auto warehouse_table_config = warehouse_json_config.get("table");
    std::string district_config_filepath = "../../../workload/tpcc/tpcc_tables/district.json";
    auto district_json_config = JsonConfig::load_file(district_config_filepath);
    auto district_table_config = district_json_config.get("table");
    std::string customer_config_filepath = "../../../workload/tpcc/tpcc_tables/customer.json";
    auto customer_json_config = JsonConfig::load_file(customer_config_filepath);
    auto customer_table_config = customer_json_config.get("table");
    std::string item_config_filepath = "../../../workload/tpcc/tpcc_tables/item.json";
    auto item_json_config = JsonConfig::load_file(item_config_filepath);
    auto item_table_config = item_json_config.get("table");
    std::string stock_config_filepath = "../../../workload/tpcc/tpcc_tables/stock.json";
    auto stock_json_config = JsonConfig::load_file(stock_config_filepath);
    auto stock_table_config = stock_json_config.get("table");

    num_warehouse = warehouse_table_config.get("item_count").get_uint64();
    num_district_per_warehouse = district_table_config.get("item_count").get_uint64();
    num_customer_per_district = customer_table_config.get("item_count").get_uint64();
    num_item = item_table_config.get("item_count").get_uint64();
    num_stock_per_warehouse = stock_table_config.get("item_count").get_uint64();
  }

  ~TPCC() {
    delete warehouse_table;
    delete customer_table;
    delete history_table;
    delete new_order_table;
    delete order_table;
    delete order_line_table;
    delete item_table;
    delete stock_table;
  }

  /* create workload generation array for benchmarking */
  ALWAYS_INLINE
  TPCCTxType* CreateWorkgenArray() {
    TPCCTxType* workgen_arr = new TPCCTxType[100];

    int i = 0, j = 0;

    j += FREQUENCY_NEW_ORDER;
    for (; i < j; i++) workgen_arr[i] = TPCCTxType::kNewOrder;

    j += FREQUENCY_PAYMENT;
    for (; i < j; i++) workgen_arr[i] = TPCCTxType::kPayment;

    j += FREQUENCY_ORDER_STATUS;
    for (; i < j; i++) workgen_arr[i] = TPCCTxType::kOrderStatus;

    j += FREQUENCY_DELIVERY;
    for (; i < j; i++) workgen_arr[i] = TPCCTxType::kDelivery;

    j += FREQUENCY_STOCK_LEVEL;
    for (; i < j; i++) workgen_arr[i] = TPCCTxType::kStockLevel;

    assert(i == 100 && j == 100);
    return workgen_arr;
  }

  // For server-side usage
  void LoadTable(node_id_t node_id,
                 node_id_t num_server,
                 MemStoreAllocParam* mem_store_alloc_param,
                 MemStoreReserveParam* mem_store_reserve_param);

  void PopulateWarehouseTable(unsigned long seed, MemStoreReserveParam* mem_store_reserve_param);

  void PopulateDistrictTable(unsigned long seed, MemStoreReserveParam* mem_store_reserve_param);

  void PopulateCustomerAndHistoryTable(unsigned long seed, MemStoreReserveParam* mem_store_reserve_param);

  void PopulateOrderNewOrderAndOrderLineTable(unsigned long seed, MemStoreReserveParam* mem_store_reserve_param);

  void PopulateItemTable(unsigned long seed, MemStoreReserveParam* mem_store_reserve_param);

  void PopulateStockTable(unsigned long seed, MemStoreReserveParam* mem_store_reserve_param);

  int LoadRecord(HashStore* table,
                 itemkey_t item_key,
                 void* val_ptr,
                 size_t val_size,
                 table_id_t table_id,
                 MemStoreReserveParam* mem_store_reserve_param);
  DataItem* GetRecord(HashStore* table,
                      itemkey_t item_key,
                      table_id_t table_id);

  ALWAYS_INLINE
  std::vector<HashStore*>& GetPrimaryHashStore() {
    return primary_table_ptrs;
  }

  ALWAYS_INLINE
  std::vector<HashStore*>& GetBackupHashStore() {
    return backup_table_ptrs;
  }

  /* Followng pieces of codes mainly comes from Silo */
  ALWAYS_INLINE
  uint32_t GetCurrentTimeMillis() {
    // XXX(stephentu): implement a scalable GetCurrentTimeMillis()
    // for now, we just give each core an increasing number
    static __thread uint32_t tl_hack = 0;
    return ++tl_hack;
  }

  // utils for generating random #s and strings
  ALWAYS_INLINE
  int CheckBetweenInclusive(int v, int lower, int upper) {
    assert(v >= lower);
    assert(v <= upper);
    return v;
  }

  ALWAYS_INLINE
  int RandomNumber(FastRandom& r, int min, int max) {
    return CheckBetweenInclusive((int)(r.NextUniform() * (max - min + 1) + min), min, max);
  }

  ALWAYS_INLINE
  int NonUniformRandom(FastRandom& r, int A, int C, int min, int max) {
    return (((RandomNumber(r, 0, A) | RandomNumber(r, min, max)) + C) % (max - min + 1)) + min;
  }

  ALWAYS_INLINE
  int64_t GetItemId(FastRandom& r) {
    return CheckBetweenInclusive(g_uniform_item_dist ? RandomNumber(r, 1, num_item) : NonUniformRandom(r, 8191, 7911, 1, num_item), 1, num_item);
  }

  ALWAYS_INLINE
  int GetCustomerId(FastRandom& r) {
    return CheckBetweenInclusive(NonUniformRandom(r, 1023, 259, 1, num_customer_per_district), 1, num_customer_per_district);
  }

  // pick a number between [start, end)
  ALWAYS_INLINE
  unsigned PickWarehouseId(FastRandom& r, unsigned start, unsigned end) {
    assert(start < end);
    const unsigned diff = end - start;
    if (diff == 1)
      return start;
    return (r.Next() % diff) + start;
  }

  inline size_t GetCustomerLastName(uint8_t* buf, FastRandom& r, int num) {
    const std::string& s0 = NameTokens[num / 100];
    const std::string& s1 = NameTokens[(num / 10) % 10];
    const std::string& s2 = NameTokens[num % 10];
    uint8_t* const begin = buf;
    const size_t s0_sz = s0.size();
    const size_t s1_sz = s1.size();
    const size_t s2_sz = s2.size();
    memcpy(buf, s0.data(), s0_sz);
    buf += s0_sz;
    memcpy(buf, s1.data(), s1_sz);
    buf += s1_sz;
    memcpy(buf, s2.data(), s2_sz);
    buf += s2_sz;
    return buf - begin;
  }

  ALWAYS_INLINE
  size_t GetCustomerLastName(char* buf, FastRandom& r, int num) {
    return GetCustomerLastName((uint8_t*)buf, r, num);
  }

  inline std::string GetCustomerLastName(FastRandom& r, int num) {
    std::string ret;
    ret.resize(CustomerLastNameMaxSize);
    ret.resize(GetCustomerLastName((uint8_t*)&ret[0], r, num));
    return ret;
  }

  ALWAYS_INLINE
  std::string GetNonUniformCustomerLastNameLoad(FastRandom& r) {
    return GetCustomerLastName(r, NonUniformRandom(r, 255, 157, 0, 999));
  }

  ALWAYS_INLINE
  size_t GetNonUniformCustomerLastNameRun(uint8_t* buf, FastRandom& r) {
    return GetCustomerLastName(buf, r, NonUniformRandom(r, 255, 223, 0, 999));
  }

  ALWAYS_INLINE
  size_t GetNonUniformCustomerLastNameRun(char* buf, FastRandom& r) {
    return GetNonUniformCustomerLastNameRun((uint8_t*)buf, r);
  }

  ALWAYS_INLINE
  std::string GetNonUniformCustomerLastNameRun(FastRandom& r) {
    return GetCustomerLastName(r, NonUniformRandom(r, 255, 223, 0, 999));
  }

  ALWAYS_INLINE
  std::string RandomStr(FastRandom& r, uint64_t len) {
    // this is a property of the oltpbench implementation...
    if (!len)
      return "";

    uint64_t i = 0;
    std::string buf(len, 0);
    while (i < (len)) {
      const char c = (char)r.NextChar();
      // XXX(stephentu): oltpbench uses java's Character.isLetter(), which
      // is a less restrictive filter than isalnum()
      if (!isalnum(c))
        continue;
      buf[i++] = c;
    }
    return buf;
  }

  // RandomNStr() actually produces a string of length len
  ALWAYS_INLINE
  std::string RandomNStr(FastRandom& r, uint64_t len) {
    const char base = '0';
    std::string buf(len, 0);
    for (uint64_t i = 0; i < len; i++)
      buf[i] = (char)(base + (r.Next() % 10));
    return buf;
  }

  ALWAYS_INLINE
  int64_t MakeDistrictKey(int32_t w_id, int32_t d_id) {
    int32_t did = d_id + (w_id * num_district_per_warehouse);
    int64_t id = static_cast<int64_t>(did);
    // assert(districtKeyToWare(id) == w_id);
    return id;
  }

  ALWAYS_INLINE
  int64_t MakeCustomerKey(int32_t w_id, int32_t d_id, int32_t c_id) {
    int32_t upper_id = w_id * num_district_per_warehouse + d_id;
    int64_t id = static_cast<int64_t>(upper_id) << 32 | static_cast<int64_t>(c_id);
    // assert(customerKeyToWare(id) == w_id);
    return id;
  }

  // only used for customer index, maybe some problems when used.
  ALWAYS_INLINE
  void ConvertString(char* newstring, const char* oldstring, int size) {
    for (int i = 0; i < 8; i++)
      if (i < size)
        newstring[7 - i] = oldstring[i];
      else
        newstring[7 - i] = '\0';

    for (int i = 8; i < 16; i++)
      if (i < size)
        newstring[23 - i] = oldstring[i];
      else
        newstring[23 - i] = '\0';
  }

  ALWAYS_INLINE
  uint64_t MakeCustomerIndexKey(int32_t w_id, int32_t d_id, std::string s_last, std::string s_first) {
    uint64_t* seckey = new uint64_t[5];
    int32_t did = d_id + (w_id * num_district_per_warehouse);
    seckey[0] = did;
    ConvertString((char*)(&seckey[1]), s_last.data(), s_last.size());
    ConvertString((char*)(&seckey[3]), s_first.data(), s_first.size());
    return (uint64_t)seckey;
  }

  ALWAYS_INLINE
  int64_t MakeHistoryKey(int32_t h_w_id, int32_t h_d_id, int32_t h_c_w_id, int32_t h_c_d_id, int32_t h_c_id) {
    int32_t cid = (h_c_w_id * num_district_per_warehouse + h_c_d_id) * num_customer_per_district + h_c_id;
    int32_t did = h_d_id + (h_w_id * num_district_per_warehouse);
    int64_t id = static_cast<int64_t>(cid) << 20 | static_cast<int64_t>(did);
    return id;
  }

  ALWAYS_INLINE
  int64_t MakeNewOrderKey(int32_t w_id, int32_t d_id, int32_t o_id) {
    int32_t upper_id = w_id * num_district_per_warehouse + d_id;
    int64_t id = static_cast<int64_t>(upper_id) << 32 | static_cast<int64_t>(o_id);
    return id;
  }

  ALWAYS_INLINE
  int64_t MakeOrderKey(int32_t w_id, int32_t d_id, int32_t o_id) {
    int32_t upper_id = w_id * num_district_per_warehouse + d_id;
    int64_t id = static_cast<int64_t>(upper_id) << 32 | static_cast<int64_t>(o_id);
    // assert(orderKeyToWare(id) == w_id);
    return id;
  }

  ALWAYS_INLINE
  int64_t MakeOrderIndexKey(int32_t w_id, int32_t d_id, int32_t c_id, int32_t o_id) {
    int32_t upper_id = (w_id * num_district_per_warehouse + d_id) * num_customer_per_district + c_id;
    int64_t id = static_cast<int64_t>(upper_id) << 32 | static_cast<int64_t>(o_id);
    return id;
  }

  ALWAYS_INLINE
  int64_t MakeOrderLineKey(int32_t w_id, int32_t d_id, int32_t o_id, int32_t number) {
    int32_t upper_id = w_id * num_district_per_warehouse + d_id;
    // 10000000 is the MAX ORDER ID
    int64_t oid = static_cast<int64_t>(upper_id) * 10000000 + static_cast<int64_t>(o_id);
    int64_t olid = oid * 15 + number;
    int64_t id = static_cast<int64_t>(olid);
    // assert(orderLineKeyToWare(id) == w_id);
    return id;
  }

  ALWAYS_INLINE
  int64_t MakeStockKey(int32_t w_id, int32_t i_id) {
    int32_t item_id = i_id + (w_id * num_stock_per_warehouse);
    int64_t s_id = static_cast<int64_t>(item_id);
    // assert(stockKeyToWare(id) == w_id);
    return s_id;
  }
};

// consistency Requirements
//
//1. Entries in the WAREHOUSE and DISTRICT tables must satisfy the relationship:
//        W_YTD = sum(D_YTD)
//    for each warehouse defined by (W_ID = D_W_ID).

//2. Entries in the DISTRICT, ORDER, and NEW-ORDER tables must satisfy the relationship:
//        D_NEXT_O_ID - 1 = max(O_ID) = max(NO_O_ID)
//    for each district defined by (D_W_ID = O_W_ID = NO_W_ID) and (D_ID = O_D_ID = NO_D_ID).
//    This condition does not apply to the NEW-ORDER table for any districts which have no outstanding new orders
//    (i.e., the numbe r of rows is zero).

//3. Entries in the NEW-ORDER table must satisfy the relationship:
//        max(NO_O_ID) - min(NO_O_ID) + 1 = [number of rows in the NEW-ORDER table for this district]
//    for each district defined by NO_W_ID and NO_D_ID.
//    This condition does not apply to any districts which have no outstanding new orders (i.e., the number of rows is zero).

//4. Entries in the ORDER and ORDER-LINE tables must satisfy the relationship:
//        sum(O_OL_CNT) = [number of rows in the ORDER-LINE table for this district]
//    for each district defined by (O_W_ID = OL_W_ID) and (O_D_ID = OL_D_ID).

//5. For any row in the ORDER table, O_CARRIER_ID is set to a null value if and only if
//    there is a corresponding row in the NEW-ORDER table defined by (O_W_ID, O_D_ID, O_ID) = (NO_W_ID, NO_D_ID, NO_O_ID).

//6. For any row in the ORDER table, O_OL_CNT must equal the number of rows in the ORDER-LINE table
//    for the corresponding order defined by (O_W_ID, O_D_ID, O_ID) = (OL_W_ID, OL_D_ID, OL_O_ID).

//7. For any row in the ORDER-LINE table, OL_DELIVERY_D is set to a null date/ time if and only if
//    the corresponding row in the ORDER table defined by
//    (O_W_ID, O_D_ID, O_ID) = (OL_W_ID, OL_D_ID, OL_O_ID) has O_CARRIER_ID set to a null value.

//8. Entries in the WAREHOUSE and HISTORY tables must satisfy the relationship: W_YTD = sum(H_AMOUNT)
//    for each warehouse defined by (W_ID = H_W_ID). 3.3.2.9

//9. Entries in the DISTRICT and HISTORY tables must satisfy the relationship: D_YTD = sum(H_AMOUNT)
//    for each district defined by (D_W_ID, D_ID) = (H_W_ID, H_D_ID). 3.3.2.10

//10. Entries in the CUSTOMER, HISTORY, ORDER, and ORDER-LINE tables must satisfy the relationship:
//    C_BALANCE = sum(OL_AMOUNT) - sum(H_AMOUNT)
//    where: H_AMOUNT is selected by (C_W_ID, C_D_ID, C_ID) = (H_C_W_ID, H_C_D_ID, H_C_ID) and
//    OL_AMOUNT is selected by: (OL_W_ID, OL_D_ID, OL_O_ID) = (O_W_ID, O_D_ID, O_ID) and
//    (O_W_ID, O_D_ID, O_C_ID) = (C_W_ID, C_D_ID, C_ID) and (OL_DELIVERY_D is not a null value)

//11. Entries in the CUSTOMER, ORDER and NEW-ORDER tables must satisfy the relationship:
//    (count(*) from ORDER) - (count(*) from NEW-ORDER) = 2100
//    for each district defined by (O_W_ID, O_D_ID) = (NO_W_ID, NO_D_ID) = (C_W_ID, C_D_ID). 3.3.2.12

//12. Entries in the CUSTOMER and ORDER-LINE tables must satisfy the relationship:
//    C_BALANCE + C_YTD_PAYMENT = sum(OL_AMOUNT)
//    for any randomly selected customers and where OL_DELIVERY_D is not set to a null date/ time.