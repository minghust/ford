// Author: Ming Zhang
// Copyright (c) 2022

#pragma once

#include "util/spinlock.h"

// Sequence lock
class SeqLock {
 public:
  SeqLock() {
    spin_lock = new SpinLock();
  }

  void BeginWrite() {
    spin_lock->Lock();
  }

  void EndWrite() {
    spin_lock->Unlock();
  }

  void BeginRead() {
    // Wait the writer
    while (IsWriting())
      ;
  }

  void EndRead() {
    // Read again if a writer locks
    // if (IsWriting()) BeginRead();
  }

 private:
  SpinLock* spin_lock;
  bool IsWriting() {
    return spin_lock->Counter() % 2 == 1;
  }
};