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
#include "threadpool/mono_queue_pool.h"
namespace peloton {
namespace threadpool {

MonoQueuePool::~MonoQueuePool() {
  worker_pool_.Shutdown();
}

void MonoQueuePool::ExecuteAsync(void(*func_ptr_)(void *),
                            void* func_arg_) {
  task_queue_.SubmitAsync(func_ptr_, func_arg_);
}

void MonoQueuePool::ExecuteSync(void(*func_ptr_)(void *),
                            void* func_arg_) {
  task_queue_.SubmitSync(func_ptr_, func_arg_);
}


MonoQueuePool& MonoQueuePool::GetInstance() {
  static MonoQueuePool mono_queue_pool;
  return mono_queue_pool;
}
}
}
