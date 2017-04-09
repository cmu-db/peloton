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

#include "common/thread_pool.h"

namespace peloton {
// a wrapper for boost worker thread pool.
class QueryThreadPool : public ThreadPool {
 public:

  // submit a specialized query task to the thread pool
  template <typename FunctionType>
  void SubmitQueryTask(FunctionType &&func, int64_t start, int64_t end) {
    // add task to thread pool.
    SubmitTask(func, &start, &end);
  }
};
}