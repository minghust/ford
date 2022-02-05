#include <chrono>
#include <cstring>
#include <iostream>

using namespace std::chrono;

#define SIZE 1024 * 1024 * 1024 * 5

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

int main() {
  int* a = new int[SIZE];
  int* b = new int[SIZE];

  for (size_t i = 0; i < SIZE; i++) a[i] = 'a' + (i % 24);

  Timer timer;
  timer.Start();
  std::memcpy(b, a, SIZE);
  timer.Stop();
  std::cout << "write bw GB/s : " << 5.0 / timer.Duration_s();

  delete[] a;
  delete[] b;
}
