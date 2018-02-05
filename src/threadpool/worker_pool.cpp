//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// worker_pool.cpp
//
// Identification: src/threadpool/worker_pool.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "threadpool/worker_pool.h"

#include "common/logger.h"

namespace peloton {
namespace threadpool {

namespace {

void WorkerFunc(std::string thread_name, std::atomic_bool *is_running,
                TaskQueue *task_queue) {
  constexpr auto kMinPauseTime = std::chrono::microseconds(1);
  constexpr auto kMaxPauseTime = std::chrono::microseconds(1000);

  LOG_DEBUG("Thread %s starting ...", thread_name.c_str());

  auto pause_time = kMinPauseTime;
  while (is_running->load() || !task_queue->IsEmpty()) {
    std::function<void()> task;
    if (!task_queue->Dequeue(task)) {
      // Polling with exponential back-off
      std::this_thread::sleep_for(pause_time);
      pause_time = std::min(pause_time * 2, kMaxPauseTime);
    } else {
      task();
      pause_time = kMinPauseTime;
    }
  }

  LOG_DEBUG("Thread %s exiting ...", thread_name.c_str());
}

}  // namespace

WorkerPool::WorkerPool(const std::string &pool_name, size_t num_workers,
                       TaskQueue &task_queue)
    : pool_name_(pool_name),
      num_workers_(num_workers),
      is_running_(false),
      task_queue_(task_queue) {}

void WorkerPool::Startup() {
  bool running = false;
  if (is_running_.compare_exchange_strong(running, true)) {
    for (size_t i = 0; i < num_workers_; i++) {
      std::string name = pool_name_ + "-worker-" + std::to_string(i);
      workers_.emplace_back(WorkerFunc, name, &is_running_, &task_queue_);
    }
  }
}

void WorkerPool::Shutdown() {
  bool running = true;
  if (is_running_.compare_exchange_strong(running, false)) {
    is_running_ = false;
    for (auto &worker : workers_) {
      worker.join();
    }
    workers_.clear();
  }
}

}  // namespace threadpool
}  // namespace peloton
