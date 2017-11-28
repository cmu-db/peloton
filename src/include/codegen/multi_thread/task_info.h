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
  // "Constructor"
  void Init(int32_t tile_group_id);

  // "Destructor"
  void Destroy();

  // TODO(zhixunt): Change this to task-related property.
  int32_t GetTileGroupId();

 private:
  // Use Init and Destroy on allocated memory instead.
  TaskInfo(int32_t tile_group_id);
  ~TaskInfo() = default;

  int32_t tile_group_id_;
};

}  // namespace codegen
}  // namespace peloton
