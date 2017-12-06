//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// task_info.cpp
//
// Identification: src/codegen/multi_thread/task_info.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/multi_thread/task_info.h"

namespace peloton {
namespace codegen {

void TaskInfo::Init(int32_t task_id, int32_t ntasks,
                    int64_t begin, int64_t end) {
  // Call constructor explicitly on memory buffer.
  new (this) TaskInfo(task_id, ntasks, begin, end);
}

void TaskInfo::Destroy() {
  // Call destructor explicitly on memory buffer.
  this->~TaskInfo();
}

int32_t TaskInfo::GetTaskId() {
  return task_id_;
}

int32_t TaskInfo::GetNumTasks() {
  return ntasks_;
}

int64_t TaskInfo::GetBegin() {
  return begin_;
}

int64_t TaskInfo::GetEnd() {
  return end_;
}

TaskInfo::TaskInfo(int32_t task_id, int32_t ntasks, int64_t begin, int64_t end)
    : task_id_(task_id), ntasks_(ntasks), begin_(begin), end_(end) {
  PL_ASSERT(task_id >= 0 && task_id < ntasks);
}

}  // namespace codegen
}  // namespace peloton
