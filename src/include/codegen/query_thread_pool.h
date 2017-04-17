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
#include "codegen/multi_thread_context.h"
#include "common/thread_pool.h"

namespace peloton {
namespace codegen {

// a wrapper for boost worker thread pool.
class QueryThreadPool {
 public:

  static QueryThreadPool *GetInstance() {
    int thread_count = std::thread::hardware_concurrency() + 3;
    static std::unique_ptr<QueryThreadPool> global_query_thread_pool(new QueryThreadPool(thread_count));
    return global_query_thread_pool.get();
  }

  ~QueryThreadPool() {
    pool.Shutdown();
  }

  // submit a specialized query task to the thread pool
  // TODO(tq5124): the thread func should be: void (RuntimeState *, MultiThreadContext).
  void SubmitQueryTask(MultiThreadContext context, void (*produce_func)(MultiThreadContext)) {
    pool.SubmitTask(produce_func, std::move(context));
  }

 private:

  QueryThreadPool(size_t thread_nums) {
    pool.Initialize(thread_nums, 0);
  }

  ThreadPool pool;
};

}
}
