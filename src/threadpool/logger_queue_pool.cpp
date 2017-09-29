//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// mono_queue_pool.cpp
//
// Identification: test/threadpool/mono_queue_pool.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "threadpool/logger_queue_pool.h"

namespace peloton {
namespace threadpool {

LoggerQueuePool::~LoggerQueuePool() {
  logger_pool_.Shutdown();
}

void LoggerQueuePool::SubmitTask(void(*task_ptr)(void *), void* task_arg, void(*task_callback_ptr)(void*), void* task_callback_arg) {
  task_queue_.EnqueueTask(task_ptr, task_arg, task_callback_ptr, task_callback_arg);
}

LoggerQueuePool& LoggerQueuePool::GetInstance() {
  static LoggerQueuePool logger_queue_pool;
  return logger_queue_pool;
}
} // namespace threadpool
} // namespace peloton
