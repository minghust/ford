// Author: Ming Zhang
// Copyright (c) 2022

#pragma once

#include <memory>

#include "dtx/dtx.h"
#include "smallbank/smallbank_db.h"

/******************** The business logic (Transaction) start ********************/

bool TxAmalgamate(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);
/* Calculate the sum of saving and checking kBalance */
bool TxBalance(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);
/* Add $1.3 to acct_id's checking account */
bool TxDepositChecking(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);
/* Send $5 from acct_id_0's checking account to acct_id_1's checking account */
bool TxSendPayment(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);
/* Add $20 to acct_id's saving's account */
bool TxTransactSaving(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);
/* Read saving and checking kBalance + update checking kBalance unconditionally */
bool TxWriteCheck(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);
/******************** The business logic (Transaction) end ********************/