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

class WorkerPool;

using TaskQueue = peloton::LockFreeQueue<std::function<void()>>;

/**
 * Wrapper method for invoking a function inside of the worker pool
 * @param should_shutdown A pointer to boolean that tells whether to shutdown
 * @param task_queue Where to poll for tasks
 */
void WorkerFunc(WorkerPool *pool, TaskQueue *task_queue);

/**
 * @brief A worker pool that maintains a group of worker threads.
 */
class WorkerPool {
 public:
  WorkerPool(size_t num_workers, TaskQueue *task_queue)
      : num_workers_(num_workers),
        should_shutdown_(false),
        task_queue_(task_queue) {}

  /**
   * Start the threads to begin polling the task queue
   */
  void Startup();

  /**
   * Tell the threads to shutdown after the task queue is empty
   */
  void Shutdown();

  /**
   * Returns true if this pool has been marked for shutdown
   * @return
   */
  bool ShouldShutdown() const {
    return (should_shutdown_);
  }

 private:
  std::vector<std::thread> workers_;
  size_t num_workers_;

  // This used to be a std::atomic_bool
  // It doesn't need to be because don't care about updating
  // this flag atomically.
  bool should_shutdown_;

  TaskQueue *task_queue_;
};

}  // namespace threadpool
}  // namespace peloton
