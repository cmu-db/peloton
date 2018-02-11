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
 * restartable, meaning it can be started again after it has been shutdown.
 * Calls to Startup() and Shutdown() are thread-safe and idempotent.
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

  /**
   * @brief Access the number of worker threads in this pool
   *
   * @return The number of worker threads assigned to this pool
   */
  size_t NumWorkers() const { return num_workers_; }

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
