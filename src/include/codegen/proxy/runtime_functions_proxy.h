//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// runtime_functions_proxy.h
//
// Identification: src/include/codegen/proxy/runtime_functions_proxy.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"
#include "codegen/proxy/type_builder.h"
#include "codegen/proxy/task_info_proxy.h"
#include "codegen/runtime_functions.h"

namespace peloton {
namespace codegen {

PROXY(ColumnLayoutInfo) {
  DECLARE_MEMBER(0, char *, col_start_ptr);
  DECLARE_MEMBER(1, uint32_t, stride);
  DECLARE_MEMBER(2, bool, columnar);
  DECLARE_TYPE;
};

PROXY(RuntimeFunctions) {
  DECLARE_METHOD(HashCrc64);
  DECLARE_METHOD(GetTileGroup);
  DECLARE_METHOD(GetTileGroupLayout);
  DECLARE_METHOD(ThrowDivideByZeroException);
  DECLARE_METHOD(ThrowOverflowException);
  DECLARE_METHOD(NewTaskInfos);
  DECLARE_METHOD(DeleteTaskInfos);
};

TYPE_BUILDER(ColumnLayoutInfo, codegen::RuntimeFunctions::ColumnLayoutInfo);

}  // namespace codegen
}  // namespace peloton