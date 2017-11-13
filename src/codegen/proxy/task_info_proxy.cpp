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
DEFINE_METHOD(peloton::codegen, TaskInfo, GetThreadId);
DEFINE_METHOD(peloton::codegen, TaskInfo, GetNumThreads);

}  // namespace codegen
}  // namespace peloton
