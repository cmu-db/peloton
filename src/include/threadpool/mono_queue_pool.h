//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// mono_queue_pool.h
//
// Identification: test/threadpool/mono_queue_pool.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "worker_pool.h"

namespace peloton {
namespace threadpool {

// TODO: tune these variables
constexpr static size_t kDefaultTaskQueueSize = 32;
constexpr static size_t kDefaultWorkerPoolSize = 4;

/**
 * @brief Wrapper class for single queue and single pool.
 * One should use this if possible.
 */
class MonoQueuePool {
 public:
  MonoQueuePool()
      : task_queue_(kDefaultTaskQueueSize),
        worker_pool_(kDefaultWorkerPoolSize, &task_queue_),
        is_running_(false) {}

  ~MonoQueuePool() {
    if (is_running_) {
      Shutdown();
    }
  }

  void Startup() {
    worker_pool_.Startup();
    is_running_ = true;
  }

  void Shutdown() {
    worker_pool_.Shutdown();
    is_running_ = false;
  }

  void SubmitTask(std::function<void()> func) {
    if (!is_running_) {
      Startup();
    }
    task_queue_.Enqueue(std::move(func));
  }

  static MonoQueuePool &GetInstance() {
    static MonoQueuePool mono_queue_pool;
    return mono_queue_pool;
  }

 private:
  TaskQueue task_queue_;
  WorkerPool worker_pool_;
  bool is_running_;
};

}  // namespace threadpool
}  // namespace peloton
