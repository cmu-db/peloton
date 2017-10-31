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
#include "task_queue.h"
#include "worker_pool.h"

// TODO: tune these variables
#define DEFAULT_WORKER_POOL_SIZE 4
#define DEFAULT_TASK_QUEUE_SIZE 32

namespace peloton {
namespace threadpool {
/**
 * @class MonoQueuePool
 * @brief Wrapper class for single queue and single pool
 * One should use this if possible.
 */
class MonoQueuePool {
 public:
  MonoQueuePool() : task_queue_(DEFAULT_TASK_QUEUE_SIZE),
                    worker_pool_(DEFAULT_WORKER_POOL_SIZE, &task_queue_){}

  ~MonoQueuePool() {
    worker_pool_.Shutdown();
  }

  void SubmitTask(void (*task_ptr)(void *), void *task_arg,
                  void (*callback_ptr)(void *), void *callback_arg) {
    task_queue_.Enqueue(task_ptr, task_arg, callback_ptr, callback_arg);
  }

  static MonoQueuePool &GetInstance() {
    static MonoQueuePool mono_queue_pool;
    return mono_queue_pool;
  }

 private:
  TaskQueue task_queue_;
  WorkerPool worker_pool_;
};

} // namespace threadpool
} // namespace peloton
