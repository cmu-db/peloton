//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// worker_pool.h
//
// Identification: src/threadpool/worker_pool.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <functional>
#include <vector>

#include "common/container/lock_free_queue.h"

namespace peloton {
namespace threadpool {

using TaskQueue = peloton::LockFreeQueue<std::function<void()>>;

void WorkerFunc(std::atomic_bool *should_shutdown, TaskQueue *task_queue);

/**
 * @brief A worker pool that maintains a group of worker threads.
 */
class WorkerPool {
 public:
  WorkerPool(size_t num_workers, TaskQueue *task_queue)
      : num_workers_(num_workers),
        should_shutdown_(false),
        task_queue_(task_queue) {}

  void Startup() {
    for (size_t i = 0; i < num_workers_; i++) {
      workers_.emplace_back(WorkerFunc, &should_shutdown_, task_queue_);
    }
  }

  void Shutdown() {
    should_shutdown_ = true;
    for (auto &worker : workers_) {
      worker.join();
    }
    workers_.clear();
  }

 private:
  std::vector<std::thread> workers_;
  size_t num_workers_;
  std::atomic_bool should_shutdown_;
  TaskQueue *task_queue_;
};

}  // namespace threadpool
}  // namespace peloton
