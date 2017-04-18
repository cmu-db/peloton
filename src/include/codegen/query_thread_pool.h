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

  ~QueryThreadPool() {
    pool.Shutdown();
  }

  void SubmitQueryTask(RuntimeState *runtime_state, MultiThreadContext *multi_thread_context, void (*target_func)(RuntimeState*,MultiThreadContext*));

 private:

  QueryThreadPool(size_t thread_nums) {
    pool.Initialize(thread_nums, 0);
  }

  ThreadPool pool;
};

}
}
