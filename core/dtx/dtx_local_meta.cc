// Author: Ming Zhang
// Copyright (c) 2022

#include "dtx/dtx.h"

bool DTX::LocalLock() {
  auto res = global_lcache->TryLock(read_write_set);
  if (!res) {
    global_lcache->Unlock(read_write_set);
    return false;
  }
  return true;
}

void DTX::LocalUnlock() {
  global_lcache->Unlock(read_write_set);
}

bool DTX::LocalValidate() {
  auto res = global_vcache->CheckVersion(read_only_set, tx_id);
  if (res == VersionStatus::VERSION_CHANGED) {
    global_lcache->Unlock(read_write_set);
    return false;
  }
  return true;
}