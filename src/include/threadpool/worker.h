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

#include <thread>

#include "threadpool/task_queue.h"

namespace peloton{
namespace threadpool{
/**
 * @class Worker
 * @brief A worker that can execute task
 */
class Worker {
 public:
  void Start(TaskQueue *task_queue);

  void Stop();

  // execute
  static void Execute(Worker *current_thread, TaskQueue *task_queue);

 private:
  volatile bool shutdown_thread_;
  std::thread worker_thread_;
};

}  // namespace threadpool
}  // namespace peloton
