//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// worker.h
//
// Identification: src/threadpool/worker.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <unistd.h>

#include "threadpool/task_queue.h"
#include "threadpool/worker.h"

namespace peloton{
namespace threadpool{

/**
 * @class WorkerPool
 * @brief A worker pool that maintains a group to worker thread
 */
class WorkerPool {
 public:
  // submit a threadpool for asynchronous execution.
  WorkerPool(const size_t num_threads, TaskQueue *task_queue);
  ~WorkerPool() { this->Shutdown(); }

  void Shutdown();

 private:
  friend class Worker;

  std::vector<std::unique_ptr<Worker>> worker_threads_;
  TaskQueue* task_queue_;
};

}  // namespace threadpool
}  // namespace peloton
