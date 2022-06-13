// Author: Ming Zhang
// Copyright (c) 2022

#include "dtx/dtx.h"

bool DTX::CompareExeRO(coro_yield_t& yield) {
  std::vector<DirectRead> pending_direct_ro;
  std::vector<HashRead> pending_hash_ro;

  // Issue reads
  if (!CompareIssueReadRO(pending_direct_ro, pending_hash_ro)) return false;

  // Yield to other coroutines when waiting for network replies
  coro_sched->Yield(yield, coro_id);

  // Receive data
  std::list<HashRead> pending_next_hash_ro;
  std::list<InvisibleRead> pending_invisible_ro;
  auto res = CheckReadRO(pending_direct_ro, pending_hash_ro, pending_invisible_ro, pending_next_hash_ro, yield);
  return res;
}

bool DTX::CompareExeRW(coro_yield_t& yield) {
  std::vector<DirectRead> pending_direct_ro;
  std::vector<DirectRead> pending_direct_rw;

  std::vector<HashRead> pending_hash_ro;
  std::vector<HashRead> pending_hash_rw;

  std::list<HashRead> pending_next_hash_ro;
  std::list<HashRead> pending_next_hash_rw;

  std::vector<InsertOffRead> pending_insert_off_rw;
  std::list<InsertOffRead> pending_next_off_rw;

  std::list<InvisibleRead> pending_invisible_ro;

  if (!CompareIssueReadRO(pending_direct_ro, pending_hash_ro)) return false;
  if (!CompareIssueReadRW(pending_direct_rw, pending_hash_rw, pending_insert_off_rw)) return false;

  // Yield to other coroutines when waiting for network replies
  coro_sched->Yield(yield, coro_id);

  auto res = CompareCheckReadRORW(pending_direct_ro,
                                  pending_direct_rw,
                                  pending_hash_ro,
                                  pending_hash_rw,
                                  pending_next_hash_ro,
                                  pending_next_hash_rw,
                                  pending_insert_off_rw,
                                  pending_next_off_rw,
                                  pending_invisible_ro,
                                  yield);

  if (global_meta_man->txn_system == DTX_SYS::LOCAL) {
    ParallelUndoLog();
  }
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

#if RFLUSH == 0
  if (!CompareIssueCommitBackup()) return false;
#elif RFLUSH == 1
  if (!CompareIssueCommitBackupFullFlush()) return false;
#elif RFLUSH == 2
  if (!CompareIssueCommitBackupSelectiveFlush()) return false;
#endif

  coro_sched->Yield(yield, coro_id);

  return true;
}

bool DTX::CompareCommitPrimary(coro_yield_t& yield) {
  if (!CompareIssueCommitPrimary()) {
    return false;
  }
  coro_sched->Yield(yield, coro_id);
  return true;
}

bool DTX::CompareTruncateAsync(coro_yield_t& yield) {
  // Truncate: Update backup's data region in an async manner
  if (!CompareIssueTruncate()) {
    return false;
  }
  // No yield, not waiting for ack
  return true;
}