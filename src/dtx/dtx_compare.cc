// Author: Ming Zhang
// Copyright (c) 2021

#include "dtx/dtx.h"

bool DTX::CompareExeRO(coro_yield_t& yield) {
  // You can read from primary or backup
  std::vector<DirectRead> pending_direct_ro;
  std::vector<HashRead> pending_hash_ro;

  // Issue reads
  if (!CompareIssueReadRO(pending_direct_ro, pending_hash_ro)) return false;

  // Yield to other coroutines when waiting for network replies
  coro_sched->Yield(yield, coro_id);

  // Receive data
  std::list<HashRead> pending_next_hash_ro;
  auto res = CompareCheckReadRO(pending_direct_ro, pending_hash_ro, pending_next_hash_ro, yield);
  return res;
}

bool DTX::CompareExeRW(coro_yield_t& yield) {
  is_ro_tx = false;
  std::vector<DirectRead> pending_direct_read;
  std::vector<HashRead> pending_hash_read;
  std::vector<InsertOffRead> pending_insert_off_read;

  std::list<HashRead> pending_next_hash_read;
  std::list<InsertOffRead> pending_next_off_read;

  if (!CompareIssueReadRO(pending_direct_read, pending_hash_read)) return false;
  if (!CompareIssueReadRW(pending_direct_read, pending_hash_read, pending_insert_off_read)) return false;

  // Yield to other coroutines when waiting for network replies
  coro_sched->Yield(yield, coro_id);

  auto res = CompareCheckReadRORW(pending_direct_read, pending_hash_read, pending_insert_off_read, pending_next_hash_read, pending_next_off_read,
                                  yield);

  return res;
}

bool DTX::CompareLocking(coro_yield_t& yield) {
  std::vector<Lock> pending_lock;
  if (!CompareIssueLocking(pending_lock)) return false;

  coro_sched->Yield(yield, coro_id);

  auto res = CompareCheckLocking(pending_lock);
  return res;
}

bool DTX::CompareValidation(coro_yield_t& yield) {
  std::vector<Version> pending_version_read;
  if (!CompareIssueValidation(pending_version_read)) return false;

  coro_sched->Yield(yield, coro_id);

  auto res = CompareCheckValidation(pending_version_read);
  return res;
}

bool DTX::CompareLockingValidation(coro_yield_t& yield) {
  // This is the same with our validation scheme, i.e., lock+read write set, read read set
  std::vector<ValidateRead> pending_validate;
  if (!CompareIssueLockValidation(pending_validate)) return false;

  coro_sched->Yield(yield, coro_id);

  auto res = CheckValidate(pending_validate);
  return res;
}

bool DTX::CompareCommitBackup(coro_yield_t& yield) {
  tx_status = TXStatus::TX_COMMIT;
  if (!CompareIssueCommitBackup()) return false;

  coro_sched->Yield(yield, coro_id);

  return true;
}

bool DTX::CompareCommitPrimary(coro_yield_t& yield) {
  std::vector<Unlock> pending_unlock;
  if (!CompareIssueCommitPrimary(pending_unlock)) {
    return false;
  }

  coro_sched->Yield(yield, coro_id);

  auto res = CompareCheckCommitPrimary(pending_unlock);
  return res;
}

bool DTX::CompareTruncateAsync(coro_yield_t& yield) {
  // Truncate: Update backup's data region in an async manner
  // i.e., don't wait for ack in this transaction
  if (!CompareIssueTruncate()) {
    return false;
  }
  return true;
}