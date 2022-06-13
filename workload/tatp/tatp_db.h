// Author: Ming Zhang
// Copyright (c) 2022

#pragma once

#include <cassert>
#include <cstdint>
#include <vector>

#include "config/table_type.h"
#include "memstore/hash_store.h"
#include "util/fast_random.h"
#include "util/json_config.h"

/*
 * Up to 1 billion subscribers so that FastGetSubscribeNumFromSubscribeID() requires
 * only 3 modulo operations.
 */
#define TATP_MAX_SUBSCRIBERS 1000000000

/* STORED PROCEDURE EXECUTION FREQUENCIES (0-100) */
#define FREQUENCY_GET_SUBSCRIBER_DATA 35    // Single
#define FREQUENCY_GET_ACCESS_DATA 35        // Single
#define FREQUENCY_GET_NEW_DESTINATION 10    // Single
#define FREQUENCY_UPDATE_SUBSCRIBER_DATA 2  // Single
#define FREQUENCY_UPDATE_LOCATION 14        // Multi
#define FREQUENCY_INSERT_CALL_FORWARDING 2  // Multi
#define FREQUENCY_DELETE_CALL_FORWARDING 2  // Multi

/******************** TATP table definitions (Schemas of key and value) start **********************/
// All keys have been sized to 8 bytes
// All values have been sized to the next multiple of 8 bytes

/* A 64-bit encoding for 15-character decimal strings. */
union tatp_sub_number_t {
  struct {
    uint32_t dec_0 : 4;
    uint32_t dec_1 : 4;
    uint32_t dec_2 : 4;
    uint32_t dec_3 : 4;
    uint32_t dec_4 : 4;
    uint32_t dec_5 : 4;
    uint32_t dec_6 : 4;
    uint32_t dec_7 : 4;
    uint32_t dec_8 : 4;
    uint32_t dec_9 : 4;
    uint32_t dec_10 : 4;
    uint32_t dec_11 : 4;
    uint32_t dec_12 : 4;
    uint32_t dec_13 : 4;
    uint32_t dec_14 : 4;
    uint32_t dec_15 : 4;
  };

  struct {
    uint64_t dec_0_1_2 : 12;
    uint64_t dec_3_4_5 : 12;
    uint64_t dec_6_7_8 : 12;
    uint64_t dec_9_10_11 : 12;
    uint64_t unused : 16;
  };

  itemkey_t item_key;
};
static_assert(sizeof(tatp_sub_number_t) == sizeof(itemkey_t), "");

/*
 * SUBSCRIBER table
 * Primary key: <uint32_t s_id>
 * Value size: 40 bytes. Full value read in GET_SUBSCRIBER_DATA.
 */
union tatp_sub_key_t {
  struct {
    uint32_t s_id;
    uint8_t unused[4];
  };
  itemkey_t item_key;

  tatp_sub_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tatp_sub_key_t) == sizeof(itemkey_t), "");

struct tatp_sub_val_t {
  tatp_sub_number_t sub_number;
  char sub_number_unused[7]; /* sub_number should be 15 bytes. We used 8 above. */
  char hex[5];
  char bytes[10];
  short bits;
  uint32_t msc_location;
  uint32_t vlr_location;
};
static_assert(sizeof(tatp_sub_val_t) == 40, "");

/*
 * Secondary SUBSCRIBER table
 * Key: <tatp_sub_number_t>
 * Value size: 8 bytes
 */
union tatp_sec_sub_key_t {
  tatp_sub_number_t sub_number;
  itemkey_t item_key;

  tatp_sec_sub_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tatp_sec_sub_key_t) == sizeof(itemkey_t), "");

struct tatp_sec_sub_val_t {
  uint32_t s_id;
  uint8_t magic;
  uint8_t unused[3];
};
static_assert(sizeof(tatp_sec_sub_val_t) == 8, "");

/*
 * ACCESS INFO table
 * Primary key: <uint32_t s_id, uint8_t ai_type>
 * Value size: 16 bytes
 */
union tatp_accinf_key_t {
  struct {
    uint32_t s_id;
    uint8_t ai_type;
    uint8_t unused[3];
  };
  itemkey_t item_key;

  tatp_accinf_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tatp_accinf_key_t) == sizeof(itemkey_t), "");

struct tatp_accinf_val_t {
  char data1;
  char data2;
  char data3[3];
  char data4[5];
  uint8_t unused[6];
};
static_assert(sizeof(tatp_accinf_val_t) == 16, "");

/*
 * SPECIAL FACILITY table
 * Primary key: <uint32_t s_id, uint8_t sf_type>
 * Value size: 8 bytes
 */
union tatp_specfac_key_t {
  struct {
    uint32_t s_id;
    uint8_t sf_type;
    uint8_t unused[3];
  };
  itemkey_t item_key;

  tatp_specfac_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tatp_specfac_key_t) == sizeof(itemkey_t), "");

struct tatp_specfac_val_t {
  char is_active;
  char error_cntl;
  char data_a;
  char data_b[5];
};
static_assert(sizeof(tatp_specfac_val_t) == 8, "");

/*
 * CALL FORWARDING table
 * Primary key: <uint32_t s_id, uint8_t sf_type, uint8_t start_time>
 * Value size: 16 bytes
 */
union tatp_callfwd_key_t {
  struct {
    uint32_t s_id;
    uint8_t sf_type;
    uint8_t start_time;
    uint8_t unused[2];
  };
  itemkey_t item_key;

  tatp_callfwd_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tatp_callfwd_key_t) == sizeof(itemkey_t), "");

struct tatp_callfwd_val_t {
  uint8_t end_time;
  char numberx[15];
};
static_assert(sizeof(tatp_callfwd_val_t) == 16, "");

/******************** TATP table definitions (Schemas of key and value) end **********************/

// Magic numbers for debugging. These are unused in the spec.
#define TATP_MAGIC 97 /* Some magic number <= 255 */
#define tatp_sub_msc_location_magic (TATP_MAGIC)
#define tatp_sec_sub_magic (TATP_MAGIC + 1)
#define tatp_accinf_data1_magic (TATP_MAGIC + 2)
#define tatp_specfac_data_b0_magic (TATP_MAGIC + 3)
#define tatp_callfwd_numberx0_magic (TATP_MAGIC + 4)

// Transaction workload type
#define TATP_TX_TYPES 7
enum class TATPTxType : uint64_t {
  kGetSubsciberData = 0,
  kGetAccessData,
  kGetNewDestination,
  kUpdateSubscriberData,
  kUpdateLocation,
  kInsertCallForwarding,
  kDeleteCallForwarding,
};

const std::string TATP_TX_NAME[TATP_TX_TYPES] = {"GetSubsciberData", "GetAccessData", "GetNewDestination", \
"UpdateSubscriberData", "UpdateLocation", "InsertCallForwarding", "DeleteCallForwarding"};

// Table id
enum class TATPTableType : uint64_t {
  kSubscriberTable = TABLE_TATP,
  kSecSubscriberTable,
  kSpecialFacilityTable,
  kAccessInfoTable,
  kCallForwardingTable,
};

class TATP {
 public:
  std::string bench_name;

  /* Map 0--999 to 12b, 4b/digit decimal representation */
  uint16_t* map_1000;

  uint32_t subscriber_size;

  /* TATP spec parameter for non-uniform random generation */
  uint32_t A;

  /* Tables */
  HashStore* subscriber_table;

  HashStore* sec_subscriber_table;

  HashStore* special_facility_table;

  HashStore* access_info_table;

  HashStore* call_forwarding_table;

  std::vector<HashStore*> primary_table_ptrs;

  std::vector<HashStore*> backup_table_ptrs;

  // For server and client usage: Provide interfaces to servers for loading tables
  TATP() {
    bench_name = "TATP";
    /* Init the precomputed decimal map */
    map_1000 = (uint16_t*)malloc(1000 * sizeof(uint16_t));
    for (size_t i = 0; i < 1000; i++) {
      uint32_t dig_1 = (i / 1) % 10;
      uint32_t dig_2 = (i / 10) % 10;
      uint32_t dig_3 = (i / 100) % 10;
      map_1000[i] = (dig_3 << 8) | (dig_2 << 4) | dig_1;
    }

    std::string config_filepath = "../../../config/tatp_config.json";
    auto json_config = JsonConfig::load_file(config_filepath);
    auto conf = json_config.get("tatp");
    subscriber_size = conf.get("num_subscriber").get_uint64();

    assert(subscriber_size <= TATP_MAX_SUBSCRIBERS);
    /* Compute the "A" parameter for nurand distribution as per spec */
    if (subscriber_size <= 1000000) {
      A = 65535;
    } else if (subscriber_size <= 10000000) {
      A = 1048575;
    } else {
      A = 2097151;
    }

    subscriber_table = nullptr;
    sec_subscriber_table = nullptr;
    special_facility_table = nullptr;
    access_info_table = nullptr;
    call_forwarding_table = nullptr;
  }

  ~TATP() {
    if (subscriber_table) delete subscriber_table;
    if (sec_subscriber_table) delete sec_subscriber_table;
    if (special_facility_table) delete special_facility_table;
    if (access_info_table) delete access_info_table;
    if (call_forwarding_table) delete call_forwarding_table;
  }

  /* create workload generation array for benchmarking */
  ALWAYS_INLINE
  TATPTxType* CreateWorkgenArray() {
    TATPTxType* workgen_arr = new TATPTxType[100];

    int i = 0, j = 0;

    j += FREQUENCY_GET_SUBSCRIBER_DATA;
    for (; i < j; i++) workgen_arr[i] = TATPTxType::kGetSubsciberData;

    j += FREQUENCY_GET_ACCESS_DATA;
    for (; i < j; i++) workgen_arr[i] = TATPTxType::kGetAccessData;

    j += FREQUENCY_GET_NEW_DESTINATION;
    for (; i < j; i++) workgen_arr[i] = TATPTxType::kGetNewDestination;

    j += FREQUENCY_UPDATE_SUBSCRIBER_DATA;
    for (; i < j; i++) workgen_arr[i] = TATPTxType::kUpdateSubscriberData;

    j += FREQUENCY_UPDATE_LOCATION;
    for (; i < j; i++) workgen_arr[i] = TATPTxType::kUpdateLocation;

    j += FREQUENCY_INSERT_CALL_FORWARDING;
    for (; i < j; i++) workgen_arr[i] = TATPTxType::kInsertCallForwarding;

    j += FREQUENCY_DELETE_CALL_FORWARDING;
    for (; i < j; i++) workgen_arr[i] = TATPTxType::kDeleteCallForwarding;

    assert(i == 100 && j == 100);
    return workgen_arr;
  }

  /*
   * Get a non-uniform-random distributed subscriber ID according to spec.
   * To get a non-uniformly random number between 0 and y:
   * NURand(A, 0, y) = (get_random(0, A) | get_random(0, y)) % (y + 1)
   */
  ALWAYS_INLINE
  uint32_t GetNonUniformRandomSubscriber(uint64_t* thread_local_seed) const {
    return ((FastRand(thread_local_seed) % subscriber_size) |
            (FastRand(thread_local_seed) & A)) %
           subscriber_size;
  }

  /* Get a subscriber number from a subscriber ID, fast */
  ALWAYS_INLINE
  tatp_sub_number_t FastGetSubscribeNumFromSubscribeID(uint32_t s_id) const {
    tatp_sub_number_t sub_number;
    sub_number.item_key = 0;
    sub_number.dec_0_1_2 = map_1000[s_id % 1000];
    s_id /= 1000;
    sub_number.dec_3_4_5 = map_1000[s_id % 1000];
    s_id /= 1000;
    sub_number.dec_6_7_8 = map_1000[s_id % 1000];

    return sub_number;
  }

  /* Get a subscriber number from a subscriber ID, simple */
  tatp_sub_number_t SimpleGetSubscribeNumFromSubscribeID(uint32_t s_id) {
#define update_sid()     \
  do {                   \
    s_id = s_id / 10;    \
    if (s_id == 0) {     \
      return sub_number; \
    }                    \
  } while (false)

    tatp_sub_number_t sub_number;
    sub_number.item_key = 0; /* Zero out all digits */

    sub_number.dec_0 = s_id % 10;
    update_sid();

    sub_number.dec_1 = s_id % 10;
    update_sid();

    sub_number.dec_2 = s_id % 10;
    update_sid();

    sub_number.dec_3 = s_id % 10;
    update_sid();

    sub_number.dec_4 = s_id % 10;
    update_sid();

    sub_number.dec_5 = s_id % 10;
    update_sid();

    sub_number.dec_6 = s_id % 10;
    update_sid();

    sub_number.dec_7 = s_id % 10;
    update_sid();

    sub_number.dec_8 = s_id % 10;
    update_sid();

    sub_number.dec_9 = s_id % 10;
    update_sid();

    sub_number.dec_10 = s_id % 10;
    update_sid();

    assert(s_id == 0);
    return sub_number;
  }

  // For server-side usage
  void LoadTable(node_id_t node_id,
                 node_id_t num_server,
                 MemStoreAllocParam* mem_store_alloc_param,
                 MemStoreReserveParam* mem_store_reserve_param);

  void PopulateSubscriberTable(MemStoreReserveParam* mem_store_reserve_param);

  void PopulateSecondarySubscriberTable(MemStoreReserveParam* mem_store_reserve_param);

  void PopulateAccessInfoTable(MemStoreReserveParam* mem_store_reserve_param);

  void PopulateSpecfacAndCallfwdTable(MemStoreReserveParam* mem_store_reserve_param);

  int LoadRecord(HashStore* table,
                 itemkey_t item_key,
                 void* val_ptr,
                 size_t val_size,
                 table_id_t table_id,
                 MemStoreReserveParam* mem_store_reserve_param);

  std::vector<uint8_t> SelectUniqueItem(uint64_t* tmp_seed, std::vector<uint8_t> values, unsigned N, unsigned M);

  ALWAYS_INLINE
  std::vector<HashStore*> GetPrimaryHashStore() {
    return primary_table_ptrs;
  }

  ALWAYS_INLINE
  std::vector<HashStore*> GetBackupHashStore() {
    return backup_table_ptrs;
  }
};
