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

namespace peloton {
namespace threadpool {

void WorkerFunc(WorkerPool *pool, TaskQueue *task_queue) {
  constexpr auto kMinPauseTime = std::chrono::microseconds(1);
  constexpr auto kMaxPauseTime = std::chrono::microseconds(1000);

  auto pause_time = kMinPauseTime;
  while (pool->ShouldShutdown() == false || !task_queue->IsEmpty()) {
    std::function<void()> task;

    // If we do not get a task, then we will want to sleep
    // and then try again. We use exponential backoff so that
    // the sleep time doubles each time we don't get a task
    // up to the max pause time.
    if (!task_queue->Dequeue(task)) {
      std::this_thread::sleep_for(pause_time);
      pause_time = std::min(pause_time * 2, kMaxPauseTime);
    } else {
      task();
      pause_time = kMinPauseTime;
    }
  }
}

void WorkerPool::Startup() {
  for (size_t i = 0; i < num_workers_; i++) {
    workers_.emplace_back(WorkerFunc, this, task_queue_);
  }
}

void WorkerPool::Shutdown() {
  should_shutdown_ = true;
  for (auto &worker : workers_) {
    worker.join();
  }
  workers_.clear();
}

}  // namespace threadpool
}  // namespace peloton
