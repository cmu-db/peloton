//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// task_info_proxy.h
//
// Identification: src/include/codegen/proxy/task_info_proxy.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/multi_thread/task_info.h"
#include "codegen/proxy/proxy.h"
#include "codegen/proxy/type_builder.h"

namespace peloton {
namespace codegen {

PROXY(TaskInfo) {
  DECLARE_MEMBER(0, char[sizeof(TaskInfo)], opaque);
  DECLARE_TYPE;

  DECLARE_METHOD(Init);
  DECLARE_METHOD(GetThreadId);
  DECLARE_METHOD(GetNumThreads);
};

TYPE_BUILDER(TaskInfo, codegen::TaskInfo);

}  // namespace codegen
}  // namespace peloton
