//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// values_runtime_proxy.cpp
//
// Identification: src/codegen/proxy/values_runtime_proxy.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/values_runtime_proxy.h"

#include "codegen/proxy/value_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_METHOD(peloton::codegen, ValuesRuntime, OutputBoolean);
DEFINE_METHOD(peloton::codegen, ValuesRuntime, OutputTinyInt);
DEFINE_METHOD(peloton::codegen, ValuesRuntime, OutputSmallInt);
DEFINE_METHOD(peloton::codegen, ValuesRuntime, OutputInteger);
DEFINE_METHOD(peloton::codegen, ValuesRuntime, OutputBigInt);
DEFINE_METHOD(peloton::codegen, ValuesRuntime, OutputDate);
DEFINE_METHOD(peloton::codegen, ValuesRuntime, OutputTimestamp);
DEFINE_METHOD(peloton::codegen, ValuesRuntime, OutputDecimal);
DEFINE_METHOD(peloton::codegen, ValuesRuntime, OutputVarchar);
DEFINE_METHOD(peloton::codegen, ValuesRuntime, OutputVarbinary);
DEFINE_METHOD(peloton::codegen, ValuesRuntime, CompareStrings);

}  // namespace codegen
}  // namespace peloton
