//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// logger_queue_pool.h
//
// Identification: test/threadpool/Logger_queue_pool.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include "worker_pool.h"

// TODO: tune these variables
constexpr static size_t kDefaultTaskQueueSize = 32;
constexpr static size_t kDefaultLoggerPoolSize = 1;

namespace peloton {
namespace threadpool {
/**
 * @class LoggerQueuePool
 * @brief Wrapper class for single queue and single pool
 * One should use this if possible.
 */
class LoggerQueuePool {
 public:
  LoggerQueuePool() : task_queue_(kDefaultTaskQueueSize),
                    worker_pool_(kDefaultLoggerPoolSize, &task_queue_),
                    is_running_(false) {}
  ~LoggerQueuePool() {
    if (is_running_ == true)
      Shutdown();
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
    if (is_running_ == false)
      Startup();
    task_queue_.Enqueue(std::move(func));
  }

  static LoggerQueuePool &GetInstance() {
    static LoggerQueuePool logger_queue_pool;
    return logger_queue_pool;
  }

 private:
  TaskQueue task_queue_;
  WorkerPool worker_pool_;
  bool is_running_;
};

} // namespace threadpool
} // namespace peloton
