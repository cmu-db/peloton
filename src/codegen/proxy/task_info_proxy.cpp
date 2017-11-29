//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// task_info_proxy.cpp
//
// Identification: src/codegen/proxy/task_info_proxy.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/task_info_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_TYPE(TaskInfo, "codegen::TaskInfo", MEMBER(opaque));

DEFINE_METHOD(peloton::codegen, TaskInfo, Init);
DEFINE_METHOD(peloton::codegen, TaskInfo, Destroy);
DEFINE_METHOD(peloton::codegen, TaskInfo, GetTaskId);
DEFINE_METHOD(peloton::codegen, TaskInfo, GetNumTasks);
DEFINE_METHOD(peloton::codegen, TaskInfo, GetBegin);
DEFINE_METHOD(peloton::codegen, TaskInfo, GetEnd);

}  // namespace codegen
}  // namespace peloton
