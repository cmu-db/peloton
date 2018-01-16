//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// brain_thread_pool.h
//
// Identification: src/include/threadpool/brain_thread_pool.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "worker_pool.h"

namespace peloton {
namespace threadpool {

constexpr static size_t kDefaultBrainTaskQueueSize = 32;
constexpr static size_t kDefaultBrainWorkerPoolSize = 2;


/**
 * @brief Wrapper class for single queue and single pool.
 * One should use this if possible.
 */
class BrainThreadPool {
 public:
	BrainThreadPool()
      : task_queue_(kDefaultBrainTaskQueueSize),
        worker_pool_(kDefaultBrainWorkerPoolSize, &task_queue_),
        is_running_(false) {}

  ~BrainThreadPool() {
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

  static BrainThreadPool &GetInstance() {
    static BrainThreadPool brain_thread_pool;
    return brain_thread_pool;
  }

 private:
  TaskQueue task_queue_;
  WorkerPool worker_pool_;
  bool is_running_;
};

}  // namespace threadpool
}  // namespace peloton
