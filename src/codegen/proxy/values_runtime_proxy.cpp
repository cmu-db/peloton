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

DEFINE_METHOD(ValuesRuntime, OutputBoolean,
              &peloton::codegen::ValuesRuntime::OutputBoolean,
              "_ZN7peloton7codegen13ValuesRuntime13OutputBooleanEPcjb");

DEFINE_METHOD(ValuesRuntime, OutputTinyInt,
              &peloton::codegen::ValuesRuntime::OutputTinyInt,
              "_ZN7peloton7codegen13ValuesRuntime13OutputTinyIntEPcja");

DEFINE_METHOD(ValuesRuntime, OutputSmallInt,
              &peloton::codegen::ValuesRuntime::OutputSmallInt,
              "_ZN7peloton7codegen13ValuesRuntime14OutputSmallIntEPcjs");

DEFINE_METHOD(ValuesRuntime, OutputInteger,
              &peloton::codegen::ValuesRuntime::OutputInteger,
              "_ZN7peloton7codegen13ValuesRuntime13OutputIntegerEPcji");

DEFINE_METHOD(ValuesRuntime, OutputBigInt,
              &peloton::codegen::ValuesRuntime::OutputBigInt,
              "_ZN7peloton7codegen13ValuesRuntime12OutputBigIntEPcjl");

DEFINE_METHOD(ValuesRuntime, OutputDate,
              &peloton::codegen::ValuesRuntime::OutputDate,
              "_ZN7peloton7codegen13ValuesRuntime10OutputDateEPcji");

DEFINE_METHOD(ValuesRuntime, OutputTimestamp,
              &peloton::codegen::ValuesRuntime::OutputTimestamp,
              "_ZN7peloton7codegen13ValuesRuntime15OutputTimestampEPcjl");

DEFINE_METHOD(ValuesRuntime, OutputDouble,
              &peloton::codegen::ValuesRuntime::OutputDecimal,
              "_ZN7peloton7codegen13ValuesRuntime13OutputDecimalEPcjd");

DEFINE_METHOD(ValuesRuntime, OutputVarchar,
              &peloton::codegen::ValuesRuntime::OutputVarchar,
              "_ZN7peloton7codegen13ValuesRuntime13OutputVarcharEPcjS2_j");

DEFINE_METHOD(ValuesRuntime, OutputVarbinary,
              &peloton::codegen::ValuesRuntime::OutputVarbinary,
              "_ZN7peloton7codegen13ValuesRuntime15OutputVarbinaryEPcjS2_j");

DEFINE_METHOD(ValuesRuntime, CompareStrings,
              &peloton::codegen::ValuesRuntime::CompareStrings,
              "_ZN7peloton7codegen13ValuesRuntime14CompareStringsEPKcjS3_j");

}  // namespace codegen
}  // namespace peloton
