//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// executor_thread_pool.cpp
//
// Identification: src/codegen/multi_thread/executor_thread_pool.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/multi_thread/executor_thread_pool.h"

namespace peloton {
namespace codegen {

ExecutorThreadPool *ExecutorThreadPool::GetInstance() {
  static ExecutorThreadPool instance;
  return &instance;
}

int32_t ExecutorThreadPool::GetNumThreads() {
  return std::thread::hardware_concurrency();
}

void ExecutorThreadPool::SubmitTask(char *runtime_state,
                                    TaskInfo *task_info,
                                    func_t func) {
  pool_.SubmitTask(func, std::move(runtime_state), std::move(task_info));
}

ExecutorThreadPool::ExecutorThreadPool() {
  auto num_threads = GetNumThreads();
  auto num_dedicated_threads = 0;
  pool_.Initialize(num_threads, num_dedicated_threads);
}

ExecutorThreadPool::~ExecutorThreadPool() {
  pool_.Shutdown();
}

}  // namespace codegen
}  // namespace peloton
