//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// worker.h
//
// Identification: src/threadpool/worker_pool.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <thread>
#include <memory>
#include <unistd.h>

#include "threadpool/task.h"

namespace peloton{
namespace threadpool{
//Forward declaration
class WorkerPool;
/**
 * @class Worker
 * @brief A worker that can execute task
 */
class Worker {
  friend class WorkerPool;

  void StartThread(WorkerPool* worker_pool);
  // poll work queue, until exiting
  static void PollForWork(Worker* current_thread, WorkerPool* current_pool);

  // wait for the current threadpool to complete and shutdown the thread;
  void Shutdown();


  volatile bool shutdown_thread_;
  std::thread worker_thread_;
};

/**
 * @class WorkerPool
 * @brief A worker pool that maintains a group to worker thread
 */
class WorkerPool {
  friend class Worker;
 public:
  // submit a threadpool for asynchronous execution.
  void Shutdown();
  WorkerPool(const size_t num_threads, TaskQueue *taskQueue);
  ~WorkerPool(){ this->Shutdown(); }
 private:
  std::vector<std::unique_ptr<Worker>> worker_threads_;
  TaskQueue* task_queue_;
};

}
}


