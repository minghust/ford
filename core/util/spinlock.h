// Author: Ming Zhang
// Copyright (c) 2022

#pragma once

#include <atomic>

class SpinLock {
 public:
  SpinLock() {
    counter.store(0, std::memory_order_release);
  }

  void Lock() {
    int locked = 1;
    int unlocked = 0;

    // Wait for unlock
    while (counter.compare_exchange_strong(locked, unlocked, std::memory_order_acq_rel))
      ;
  }

  void Unlock() {
    int unlocked = 0;
    counter.exchange(unlocked, std::memory_order_acq_rel);
  }

  int Counter() {
    return counter.load(std::memory_order_acquire);
  }

 private:
  std::atomic_int counter;
};