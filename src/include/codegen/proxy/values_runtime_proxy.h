//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// values_runtime_proxy.h
//
// Identification: src/include/codegen/proxy/values_runtime_proxy.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"
#include "codegen/values_runtime.h"

namespace peloton {
namespace codegen {

PROXY(ValuesRuntime) {
  DECLARE_METHOD(OutputBoolean);
  DECLARE_METHOD(OutputTinyInt);
  DECLARE_METHOD(OutputSmallInt);
  DECLARE_METHOD(OutputInteger);
  DECLARE_METHOD(OutputBigInt);
  DECLARE_METHOD(OutputDate);
  DECLARE_METHOD(OutputTimestamp);
  DECLARE_METHOD(OutputDecimal);
  DECLARE_METHOD(OutputVarchar);
  DECLARE_METHOD(OutputVarbinary);
  DECLARE_METHOD(CompareStrings);
};

}  // namespace codegen
}  // namespace peloton