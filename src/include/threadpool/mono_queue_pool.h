//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// mono_queue_pool.h
//
// Identification: test/threadpool/mono_queue_pool.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once
#include "task.h"
#include "worker.h"

// TODO: tune these variables
#define DEFAULT_WORKER_POOL_SIZE 4
#define DEFAULT_TASK_QUEUE_SIZE 20

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

  ~MonoQueuePool();

  void SubmitTask(void(*task_ptr)(void *), void* task_arg, void(*task_callback_ptr)(void *), void* task_callback_arg);

  static MonoQueuePool& GetInstance();

 private:
  TaskQueue task_queue_;
  WorkerPool worker_pool_;
};
} // namespace threadpool
} // namespace peloton

