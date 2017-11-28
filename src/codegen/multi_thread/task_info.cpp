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

void TaskInfo::Init(int32_t tile_group_id) {
  // Call constructor explicitly on memory buffer.
  new (this) TaskInfo(tile_group_id);
}

void TaskInfo::Destroy() {
  // Call destructor explicitly on memory buffer.
  this->~TaskInfo();
}

int32_t TaskInfo::GetTileGroupId() {
  return tile_group_id_;
}

TaskInfo::TaskInfo(int32_t tile_group_id)
    : tile_group_id_(tile_group_id) {
  PL_ASSERT(tile_group_id >= 0);
}

}  // namespace codegen
}  // namespace peloton
