//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// mono_queue_pool.cpp
//
// Identification: test/threadpool/mono_queue_pool.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "threadpool/mono_queue_pool.h"

namespace peloton {
namespace threadpool {

MonoQueuePool::~MonoQueuePool() {
  worker_pool_.Shutdown();
}

void MonoQueuePool::SubmitTask(void(*task_ptr)(void *), void* task_arg, void(*task_callback_ptr)(void*), void* task_callback_arg) {
  task_queue_.EnqueueTask(task_ptr, task_arg, task_callback_ptr, task_callback_arg);
}

MonoQueuePool& MonoQueuePool::GetInstance() {
  static MonoQueuePool mono_queue_pool;
  return mono_queue_pool;
}
} // namespace threadpool
} // namespace peloton
