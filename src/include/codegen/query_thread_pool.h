//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_thread_pool.h
//
// Identification: src/include/codegen/query_thread_pool.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/runtime_state.h"
#include "codegen/multi_thread_context.h"
#include "common/thread_pool.h"

namespace peloton {
namespace codegen {

class QueryThreadPool {
 public:

  static QueryThreadPool *GetInstance();
  static uint64_t GetThreadCount();

  QueryThreadPool() {
    pool.Initialize(GetThreadCount(), 0);
  }

  void SubmitQueryTask(RuntimeState *runtime_state, MultiThreadContext *multi_thread_context, void (*target_func)(RuntimeState*,MultiThreadContext*));

 private:

  ThreadPool pool;
};

}
}
