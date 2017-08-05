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

void MonoQueuePool::QueueStatement(void(*func_ptr_)(void *),
                            void* func_arg_) {
  task_queue_.SubmitTask(func_ptr_, func_arg_);
}

MonoQueuePool& MonoQueuePool::GetInstance() {
  static MonoQueuePool mono_queue_pool;
  return mono_queue_pool;
}
} // namespace threadpool
} // namespace peloton
