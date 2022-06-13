// Author: Ming Zhang
// Copyright (c) 2022

#pragma once

#include <memory>

#include "dtx/dtx.h"
#include "tatp/tatp_db.h"

/******************** The business logic (Transaction) start ********************/

// Read 1 SUBSCRIBER row
bool TxGetSubsciberData(TATP* tatp_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);

// 1. Read 1 SPECIAL_FACILITY row
// 2. Read up to 3 CALL_FORWARDING rows
// 3. Validate up to 4 rows
bool TxGetNewDestination(TATP* tatp_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);

// Read 1 ACCESS_INFO row
bool TxGetAccessData(TATP* tatp_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);

// Update 1 SUBSCRIBER row and 1 SPECIAL_FACILTY row
bool TxUpdateSubscriberData(TATP* tatp_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);

// 1. Read a SECONDARY_SUBSCRIBER row
// 2. Update a SUBSCRIBER row
bool TxUpdateLocation(TATP* tatp_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);

// 1. Read a SECONDARY_SUBSCRIBER row
// 2. Read a SPECIAL_FACILTY row
// 3. Insert a CALL_FORWARDING row
bool TxInsertCallForwarding(TATP* tatp_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);

// 1. Read a SECONDARY_SUBSCRIBER row
// 2. Delete a CALL_FORWARDING row
bool TxDeleteCallForwarding(TATP* tatp_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);

/******************** The business logic (Transaction) end ********************/