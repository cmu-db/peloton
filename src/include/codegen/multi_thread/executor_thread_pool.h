//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// executor_thread_pool.h
//
// Identification: src/include/codegen/multi_thread/executor_thread_pool.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/thread_pool.h"
#include "codegen/codegen.h"
#include "codegen/runtime_state.h"
#include "codegen/multi_thread/task_info.h"

namespace peloton {
namespace codegen {

// A singleton thread pool for the executor.
// Used by the compiled code.
class ExecutorThreadPool {
 public:
  // Get the singleton instance.
  static ExecutorThreadPool *GetInstance();

  // Number of threads available in the pool.
  static int32_t GetNumThreads();

  using func_t = void (*)(char *, TaskInfo *);

  // Submit a task to be run by a worker thread.
  void SubmitTask(char *runtime_state,
                  TaskInfo *task_info,
                  func_t func);

 private:
  ExecutorThreadPool();

  ~ExecutorThreadPool();

  ThreadPool pool_;
};

}  // namespace codegen
}  // namespace peloton
