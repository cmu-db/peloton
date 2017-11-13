//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// task_info.h
//
// Identification: src/include/codegen/multi_thread/task_info.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"

namespace peloton {
namespace codegen {

// This class stores the per-thread information of a query, as opposed to cross-
// thread information stored in RuntimeState.
class TaskInfo {
 public:
  void Init(int32_t thread_id, int32_t nthreads);

  int32_t GetThreadId();

  int32_t GetNumThreads();

 private:
  // Can't construct
  TaskInfo() = default;

  int32_t thread_id_;
  int32_t nthreads_;
};

}  // namespace codegen
}  // namespace peloton
