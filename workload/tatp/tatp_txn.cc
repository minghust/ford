// Author: Ming Zhang
// Copyright (c) 2022

#include "tatp/tatp_txn.h"

/******************** The business logic (Transaction) start ********************/

// Read 1 SUBSCRIBER row
bool TxGetSubsciberData(TATP* tatp_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  // RDMA_LOG(DBG) << "coro " << dtx->coro_id << " executes TxGetSubsciberData, tx_id=" << tx_id;
  dtx->TxBegin(tx_id);

  // Build key for the database record
  tatp_sub_key_t sub_key;
  sub_key.s_id = tatp_client->GetNonUniformRandomSubscriber(seed);

  // This empty data sub_obj will be filled by RDMA reading from remote when running transaction
  auto sub_obj = std::make_shared<DataItem>((table_id_t)TATPTableType::kSubscriberTable, sub_key.item_key);

  // Add r/w set and execute transaction
  dtx->AddToReadOnlySet(sub_obj);
  if (!dtx->TxExe(yield)) return false;

  // Get value
  auto* value = (tatp_sub_val_t*)(sub_obj->value);

  // Use value
  if (value->msc_location != tatp_sub_msc_location_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
  }

  // Commit transaction
  bool commit_status = dtx->TxCommit(yield);
  return commit_status;
}

// 1. Read 1 SPECIAL_FACILITY row
// 2. Read up to 3 CALL_FORWARDING rows
// 3. Validate up to 4 rows
bool TxGetNewDestination(TATP* tatp_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  // RDMA_LOG(DBG) << "coro " << dtx->coro_id << " executes TxGetNewDestination";
  dtx->TxBegin(tx_id);

  uint32_t s_id = tatp_client->GetNonUniformRandomSubscriber(seed);
  uint8_t sf_type = (FastRand(seed) % 4) + 1;
  uint8_t start_time = (FastRand(seed) % 3) * 8;
  uint8_t end_time = (FastRand(seed) % 24) * 1;

  unsigned cf_to_fetch = (start_time / 8) + 1;
  assert(cf_to_fetch >= 1 && cf_to_fetch <= 3);

  /* Fetch a single special facility record */
  tatp_specfac_key_t specfac_key;

  specfac_key.s_id = s_id;
  specfac_key.sf_type = sf_type;

  auto specfac_obj =
      std::make_shared<DataItem>((table_id_t)TATPTableType::kSpecialFacilityTable, specfac_key.item_key);

  dtx->AddToReadOnlySet(specfac_obj);
  if (!dtx->TxExe(yield)) return false;

  if (specfac_obj->value_size == 0) {
    dtx->TxAbortReadOnly();
    return false;
  }

  // Need to wait for reading specfac_obj from remote
  auto* specfac_val = (tatp_specfac_val_t*)(specfac_obj->value);
  if (specfac_val->data_b[0] != tatp_specfac_data_b0_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
  }
  if (specfac_val->is_active == 0) {
    // is_active is randomly generated at pm node side
    dtx->TxAbortReadOnly();
    return false;
  }

  /* Fetch possibly multiple call forwarding records. */
  DataItemPtr callfwd_obj[3];
  tatp_callfwd_key_t callfwd_key[3];

  for (unsigned i = 0; i < cf_to_fetch; i++) {
    callfwd_key[i].s_id = s_id;
    callfwd_key[i].sf_type = sf_type;
    callfwd_key[i].start_time = (i * 8);
    callfwd_obj[i] = std::make_shared<DataItem>(
        (table_id_t)TATPTableType::kCallForwardingTable,
        callfwd_key[i].item_key);
    dtx->AddToReadOnlySet(callfwd_obj[i]);
  }
  if (!dtx->TxExe(yield)) return false;

  bool callfwd_success = false;
  for (unsigned i = 0; i < cf_to_fetch; i++) {
    if (callfwd_obj[i]->value_size == 0) {
      continue;
    }

    auto* callfwd_val = (tatp_callfwd_val_t*)(callfwd_obj[i]->value);
    if (callfwd_val->numberx[0] != tatp_callfwd_numberx0_magic) {
      RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
    }
    if (callfwd_key[i].start_time <= start_time && end_time < callfwd_val->end_time) {
      /* All conditions satisfied */
      callfwd_success = true;
    }
  }

  if (callfwd_success) {
    bool commit_status = dtx->TxCommit(yield);
    return commit_status;
  } else {
    dtx->TxAbortReadOnly();
    return false;
  }
}

// Read 1 ACCESS_INFO row
bool TxGetAccessData(TATP* tatp_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  // RDMA_LOG(DBG) << "coro " << dtx->coro_id << " executes TxGetAccessData, tx_id=" << tx_id;
  dtx->TxBegin(tx_id);

  tatp_accinf_key_t key;
  key.s_id = tatp_client->GetNonUniformRandomSubscriber(seed);
  key.ai_type = (FastRand(seed) & 3) + 1;

  auto acc_obj = std::make_shared<DataItem>((table_id_t)TATPTableType::kAccessInfoTable, key.item_key);

  dtx->AddToReadOnlySet(acc_obj);
  if (!dtx->TxExe(yield)) return false;

  if (acc_obj->value_size > 0) {
    /* The key was found */
    auto* value = (tatp_accinf_val_t*)(acc_obj->value);
    if (value->data1 != tatp_accinf_data1_magic) {
      RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
    }

    bool commit_status = dtx->TxCommit(yield);
    return commit_status;
  } else {
    /* Key not found */
    dtx->TxAbortReadOnly();
    return false;
  }
}

// Update 1 SUBSCRIBER row and 1 SPECIAL_FACILTY row
bool TxUpdateSubscriberData(TATP* tatp_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  // RDMA_LOG(DBG) << "coro " << dtx->coro_id << " executes TxUpdateSubscriberData, tx_id=" << tx_id;
  dtx->TxBegin(tx_id);

  uint32_t s_id = tatp_client->GetNonUniformRandomSubscriber(seed);
  uint8_t sf_type = (FastRand(seed) % 4) + 1;

  /* Read + lock the subscriber record */
  tatp_sub_key_t sub_key;
  sub_key.s_id = s_id;

  auto sub_obj = std::make_shared<DataItem>((table_id_t)TATPTableType::kSubscriberTable, sub_key.item_key);
  dtx->AddToReadWriteSet(sub_obj);

  /* Read + lock the special facilty record */
  tatp_specfac_key_t specfac_key;
  specfac_key.s_id = s_id;
  specfac_key.sf_type = sf_type;

  auto specfac_obj = std::make_shared<DataItem>((table_id_t)TATPTableType::kSpecialFacilityTable, specfac_key.item_key);
  dtx->AddToReadWriteSet(specfac_obj);
  if (!dtx->TxExe(yield)) return false;

  /* If we are here, execution succeeded and we have locks */
  auto* sub_val = (tatp_sub_val_t*)(sub_obj->value);
  if (sub_val->msc_location != tatp_sub_msc_location_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
  }
  sub_val->bits = FastRand(seed); /* Update */

  auto* specfac_val = (tatp_specfac_val_t*)(specfac_obj->value);
  if (specfac_val->data_b[0] != tatp_specfac_data_b0_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
  }
  specfac_val->data_a = FastRand(seed); /* Update */

  bool commit_status = dtx->TxCommit(yield);
  return commit_status;
}

// 1. Read a SECONDARY_SUBSCRIBER row
// 2. Update a SUBSCRIBER row
bool TxUpdateLocation(TATP* tatp_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  // RDMA_LOG(DBG) << "coro " << dtx->coro_id << " executes TxUpdateLocation, tx_id=" << tx_id;

  dtx->TxBegin(tx_id);

  uint32_t s_id = tatp_client->GetNonUniformRandomSubscriber(seed);
  uint32_t vlr_location = FastRand(seed);

  /* Read the secondary subscriber record */
  tatp_sec_sub_key_t sec_sub_key;
  sec_sub_key.sub_number = tatp_client->FastGetSubscribeNumFromSubscribeID(s_id);

  auto sec_sub_obj = std::make_shared<DataItem>((table_id_t)TATPTableType::kSecSubscriberTable, sec_sub_key.item_key);

  dtx->AddToReadOnlySet(sec_sub_obj);
  if (!dtx->TxExe(yield)) return false;

  auto* sec_sub_val = (tatp_sec_sub_val_t*)(sec_sub_obj->value);
  if (sec_sub_val->magic != tatp_sec_sub_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
  }
  if (sec_sub_val->s_id != s_id) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
  }

  tatp_sub_key_t sub_key;
  sub_key.s_id = sec_sub_val->s_id;

  auto sub_obj = std::make_shared<DataItem>((table_id_t)TATPTableType::kSubscriberTable, sub_key.item_key);

  dtx->AddToReadWriteSet(sub_obj);
  if (!dtx->TxExe(yield)) return false;

  auto* sub_val = (tatp_sub_val_t*)(sub_obj->value);
  if (sub_val->msc_location != tatp_sub_msc_location_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
  }
  sub_val->vlr_location = vlr_location; /* Update */

  bool commit_status = dtx->TxCommit(yield);
  return commit_status;
}

// 1. Read a SECONDARY_SUBSCRIBER row
// 2. Read a SPECIAL_FACILTY row
// 3. Insert a CALL_FORWARDING row
bool TxInsertCallForwarding(TATP* tatp_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  // RDMA_LOG(DBG) << "coro " << dtx->coro_id << " executes TxInsertCallForwarding, tx_id=" << tx_id;
  dtx->TxBegin(tx_id);

  uint32_t s_id = tatp_client->GetNonUniformRandomSubscriber(seed);
  uint8_t sf_type = (FastRand(seed) % 4) + 1;
  uint8_t start_time = (FastRand(seed) % 3) * 8;
  uint8_t end_time = (FastRand(seed) % 24) * 1;

  // Read the secondary subscriber record
  tatp_sec_sub_key_t sec_sub_key;
  sec_sub_key.sub_number = tatp_client->FastGetSubscribeNumFromSubscribeID(s_id);

  auto sec_sub_obj = std::make_shared<DataItem>((table_id_t)TATPTableType::kSecSubscriberTable, sec_sub_key.item_key);

  dtx->AddToReadOnlySet(sec_sub_obj);
  if (!dtx->TxExe(yield)) return false;

  auto* sec_sub_val = (tatp_sec_sub_val_t*)(sec_sub_obj->value);
  if (sec_sub_val->magic != tatp_sec_sub_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
  }
  if (sec_sub_val->s_id != s_id) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
  }

  // Read the Special Facility record
  tatp_specfac_key_t specfac_key;
  specfac_key.s_id = s_id;
  specfac_key.sf_type = sf_type;

  auto specfac_obj = std::make_shared<DataItem>((table_id_t)TATPTableType::kSpecialFacilityTable, specfac_key.item_key);

  dtx->AddToReadOnlySet(specfac_obj);
  if (!dtx->TxExe(yield)) return false;

  /* The Special Facility record exists only 62.5% of the time */
  if (specfac_obj->value_size == 0) {
    dtx->TxAbortReadOnly();
    return false;
  }

  /* If we are here, the Special Facility record exists. */
  auto* specfac_val = (tatp_specfac_val_t*)(specfac_obj->value);
  if (specfac_val->data_b[0] != tatp_specfac_data_b0_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
  }

  // Lock the Call Forwarding record
  tatp_callfwd_key_t callfwd_key;
  callfwd_key.s_id = s_id;
  callfwd_key.sf_type = sf_type;
  callfwd_key.start_time = start_time;

  auto callfwd_obj = std::make_shared<DataItem>(
      (table_id_t)TATPTableType::kCallForwardingTable,
      sizeof(tatp_callfwd_val_t),
      callfwd_key.item_key,
      tx_id,  // Using tx_id as key's version
      1);

  // Handle Insert. Only read the remote offset of callfwd_obj
  dtx->AddToReadWriteSet(callfwd_obj);
  if (!dtx->TxExe(yield)) return false;

  // Fill callfwd_val by user
  auto* callfwd_val = (tatp_callfwd_val_t*)(callfwd_obj->value);
  callfwd_val->numberx[0] = tatp_callfwd_numberx0_magic;
  callfwd_val->end_time = end_time;

  bool commit_status = dtx->TxCommit(yield);
  return commit_status;
}

// 1. Read a SECONDARY_SUBSCRIBER row
// 2. Delete a CALL_FORWARDING row
bool TxDeleteCallForwarding(TATP* tatp_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  // RDMA_LOG(DBG) << "coro " << dtx->coro_id << " executes TxDeleteCallForwarding, tx_id=" << tx_id;
  dtx->TxBegin(tx_id);

  uint32_t s_id = tatp_client->GetNonUniformRandomSubscriber(seed);
  uint8_t sf_type = (FastRand(seed) % 4) + 1;
  uint8_t start_time = (FastRand(seed) % 3) * 8;

  // Read the secondary subscriber record
  tatp_sec_sub_key_t sec_sub_key;
  sec_sub_key.sub_number = tatp_client->FastGetSubscribeNumFromSubscribeID(s_id);
  auto sec_sub_obj = std::make_shared<DataItem>((table_id_t)TATPTableType::kSecSubscriberTable, sec_sub_key.item_key);
  dtx->AddToReadOnlySet(sec_sub_obj);
  if (!dtx->TxExe(yield)) return false;

  auto* sec_sub_val = (tatp_sec_sub_val_t*)(sec_sub_obj->value);
  if (sec_sub_val->magic != tatp_sec_sub_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
  }
  if (sec_sub_val->s_id != s_id) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
  }

  // Delete the Call Forwarding record if it exists
  tatp_callfwd_key_t callfwd_key;
  callfwd_key.s_id = s_id;
  callfwd_key.sf_type = sf_type;
  callfwd_key.start_time = start_time;

  auto callfwd_obj = std::make_shared<DataItem>(
      (table_id_t)TATPTableType::kCallForwardingTable,
      callfwd_key.item_key);

  dtx->AddToReadWriteSet(callfwd_obj);
  if (!dtx->TxExe(yield)) return false;
  /*
   * Delete is handled using set is_deleted = 1, so
   * we have a finished Call Forwarding record here.
   */
  auto* callfwd_val = (tatp_callfwd_val_t*)(callfwd_obj->value);
  if (callfwd_val->numberx[0] != tatp_callfwd_numberx0_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
  }
  callfwd_obj->valid = 0;  // 0 to indicate that this callfwd_obj will be deleted

  bool commit_status = dtx->TxCommit(yield);
  return commit_status;
}

/******************** The business logic (Transaction) end ********************/