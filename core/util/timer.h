// Author: Ming Zhang
// Copyright (c) 2022

#pragma once

#include <chrono>

using namespace std::chrono;

// Records one event's duration
class Timer {
 public:
  Timer() {}
  void Start() { start = high_resolution_clock::now(); }
  void Stop() { end = high_resolution_clock::now(); }

  double Duration_s() {
    return duration_cast<duration<double>>(end - start).count();
  }

  uint64_t Duration_ns() {
    return duration_cast<std::chrono::nanoseconds>(end - start).count();
  }

  uint64_t Duration_us() {
    return duration_cast<std::chrono::microseconds>(end - start).count();
  }

  uint64_t Duration_ms() {
    return duration_cast<std::chrono::milliseconds>(end - start).count();
  }

 private:
  high_resolution_clock::time_point start;
  high_resolution_clock::time_point end;
};
