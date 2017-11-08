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
#include <thread>
#include <memory>
#include <unistd.h>

#include "threadpool/log_task.h"

namespace peloton {
namespace threadpool {
// Forward declaration
class LoggerPool;
/**
 * @class Logger
 * @brief A Logger that can execute task
 */
class Logger {
  friend class LoggerPool;

  void StartThread(LoggerPool* logger_pool);
  // poll work queue, until exiting
  static void PollForWork(Logger* current_thread, LoggerPool* current_pool);

  // wait for the current threadpool to complete and shutdown the thread;
  void Shutdown();

  volatile bool shutdown_thread_;
  std::thread logger_thread_;
};

/**
 * @class WorkerPool
 * @brief A worker pool that maintains a group to worker thread
 */
class LoggerPool {
  friend class Logger;

 public:
  // submit a threadpool for asynchronous execution.
  void Shutdown();
  LoggerPool(const size_t num_threads, LogTaskQueue* taskQueue);
  ~LoggerPool() { this->Shutdown(); }

 private:
  std::vector<std::unique_ptr<Logger>> logger_threads_;
  LogTaskQueue* task_queue_;
};
}
}
