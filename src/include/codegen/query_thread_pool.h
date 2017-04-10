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

  QueryThreadPool(size_t thread_nums) {
    pool.Initialize(thread_nums, 0);
  }

  ~QueryThreadPool() {
    pool.Shutdown();
  }

  // submit a specialized query task to the thread pool
  void SubmitQueryTask(MultiThreadContext context, void (*produce_func)(MultiThreadContext)) {
    pool.SubmitTask(produce_func, std::move(context));
  }

 private:
  ThreadPool pool;
};

}
}
