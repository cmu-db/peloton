//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// worker_pool.h
//
// Identification: src/include/threadpool/worker_pool.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <functional>
#include <vector>

#include "common/container/lock_free_queue.h"

namespace peloton {
namespace threadpool {

using TaskQueue = peloton::LockFreeQueue<std::function<void()>>;

/**
 * @brief A worker pool that maintains a group of worker threads. This pool is
 * re-startable, meaning it can be started-up after it has been shutdown.
 * Calls to Startup() and Shutdown() can be called safely by multiple threads,
 * multiple times (i.e., they're thread-safe and idempotent).
 */
class WorkerPool {
 public:
  WorkerPool(const std::string &pool_name, size_t num_workers,
             TaskQueue &task_queue);

  /**
   * @brief Start this worker pool. Thread-safe and idempotent.
   */
  void Startup();

  /**
   * @brief Shutdown this worker pool. Thread-safe and idempotent.
   */
  void Shutdown();

 private:
  // The name of this pool
  std::string pool_name_;
  // The worker threads
  std::vector<std::thread> workers_;
  // The number of worker threads
  size_t num_workers_;
  // Flag indicating whether the pool is running
  std::atomic_bool is_running_;
  // The queue where workers pick up tasks
  TaskQueue &task_queue_;
};

}  // namespace threadpool
}  // namespace peloton
