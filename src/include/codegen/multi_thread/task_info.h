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
#include "common/logger.h"

namespace peloton {
namespace codegen {

// This class stores the per-thread information of a query, as opposed to cross-
// thread information stored in RuntimeState.
class TaskInfo {
 public:
  // "Constructor"
  void Init(int32_t task_id, int32_t ntasks, int64_t begin, int64_t end);

  // "Destructor"
  void Destroy();

  int32_t GetTaskId();

  int32_t GetNumTasks();

  int64_t GetBegin();

  int64_t GetEnd();

 private:
  // Use Init and Destroy on allocated memory instead.
  TaskInfo(int32_t task_id, int32_t ntasks, int64_t begin, int64_t end);

  int32_t task_id_;
  int32_t ntasks_;
  int64_t begin_;
  int64_t end_;
};

}  // namespace codegen
}  // namespace peloton
