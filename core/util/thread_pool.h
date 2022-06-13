// Author: Ming Zhang
// Copyright (c) 2022

#pragma once

#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <vector>

#include "base/common.h"

class ThreadPool {
 public:
  ThreadPool(size_t);
  template <class F, class... Args>
  auto Enqueue(F&& f, Args&&... args) -> std::future<decltype(f(args...))>;
  ~ThreadPool();

 private:
  std::vector<std::thread> workers;
  std::queue<std::function<void()>> tasks;
  std::mutex queue_mutex;
  std::condition_variable condition;
  bool stop;
};

ALWAYS_INLINE
ThreadPool::ThreadPool(size_t threads) : stop(false) {
  for (size_t i = 0; i < threads; ++i) {
    workers.emplace_back([this] {
      for (;;) {
        std::function<void()> task;
        {
          std::unique_lock<std::mutex> lock(this->queue_mutex);
          this->condition.wait(lock, [this] { return this->stop || !this->tasks.empty(); });
          if (this->stop && this->tasks.empty()) return;
          task = std::move(this->tasks.front());
          this->tasks.pop();
        }
        task();  // Execute the enqueued task
      }
    });
  }
}

// Add a task to the thread pool
template <class F, class... Args>
ALWAYS_INLINE 
auto ThreadPool::Enqueue(F&& f, Args&&... args) -> std::future<decltype(f(args...))> {
  auto task = std::make_shared<std::packaged_task<return_type()>>(
      std::bind(std::forward<F>(f), std::forward<Args>(args)...));
  {
    std::unique_lock<std::mutex> lock(queue_mutex);
    if (stop) throw std::runtime_error("Enqueue on stopped ThreadPool");
    tasks.emplace([task]() { (*task)(); });
  }
  condition.notify_one();
  return task->get_future();  // Return the results of the task
}

ALWAYS_INLINE
ThreadPool::~ThreadPool() {
  {
    std::unique_lock<std::mutex> lock(queue_mutex);
    stop = true;
  }
  condition.notify_all();
  for (std::thread& worker : workers) {
    if (worker.joinable()) {
      worker.join();
    }
  }
}