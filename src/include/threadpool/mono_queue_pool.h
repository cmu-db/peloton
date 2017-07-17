//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// worker_pool_test.cpp
//
// Identification: test/threadpool/worker_pool_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once
#include "task.h"
#include "worker.h"

// TODO: tune these variables
#define DEFAULT_WORKER_POOL_SIZE (std::thread::hardware_concurrency())
#define DEFAULT_TASK_QUEUE_SIZE (std::thread::hardware_concurrency() * 100)

namespace peloton {
namespace threadpool {
/**
 * @class MonoQueuePool
 * @brief Wrapper class for single queue and single pool
 * One should use this if possible.
 */
class MonoQueuePool {
 public:
  inline MonoQueuePool()
    : task_queue_(DEFAULT_TASK_QUEUE_SIZE),
      worker_pool_(DEFAULT_WORKER_POOL_SIZE, &task_queue_){}

  inline ~MonoQueuePool();
  void ExecuteSync(void(*func_ptr)(void *), void* func_arg);
  void ExecuteAsync(void(*func_ptr)(void *), void* func_arg);

  static MonoQueuePool& GetInstance();

 private:
  TaskQueue task_queue_;
  WorkerPool worker_pool_;
};
}
}
