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
#include "task.h"
#include "logger.h"

// TODO: tune these variables
#define DEFAULT_LOGGER_POOL_SIZE 1
#define DEFAULT_TASK_QUEUE_SIZE 20000

namespace peloton {
namespace threadpool {
/**
 * @class MonoQueuePool
 * @brief Wrapper class for single queue and single pool
 * One should use this if possible.
 */
class LoggerQueuePool {
 public:
  inline LoggerQueuePool()
      : task_queue_(DEFAULT_TASK_QUEUE_SIZE),
        logger_pool_(DEFAULT_LOGGER_POOL_SIZE, &task_queue_) {}

  ~LoggerQueuePool();

  void SubmitTask(void (*task_ptr)(void*), void* task_arg,
                  void (*task_callback_ptr)(void*), void* task_callback_arg);

  static LoggerQueuePool& GetInstance();

 private:
  LogTaskQueue task_queue_;
  LoggerPool logger_pool_;
};
}  // namespace threadpool
}  // namespace peloton
