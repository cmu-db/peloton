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

#include "codegen/codegen.h"
#include "codegen/proxy/proxy.h"
#include "codegen/values_runtime.h"

namespace peloton {
namespace codegen {

PROXY(ValuesRuntime) {
  PROXY_METHOD(OutputBoolean, &peloton::codegen::ValuesRuntime::OutputBoolean,
               "_ZN7peloton7codegen13ValuesRuntime13OutputBooleanEPcjb");
  PROXY_METHOD(OutputTinyInt, &peloton::codegen::ValuesRuntime::OutputTinyInt,
               "_ZN7peloton7codegen13ValuesRuntime13OutputTinyIntEPcja");
  PROXY_METHOD(OutputSmallInt, &peloton::codegen::ValuesRuntime::OutputSmallInt,
               "_ZN7peloton7codegen13ValuesRuntime14OutputSmallIntEPcjs");
  PROXY_METHOD(OutputInteger, &peloton::codegen::ValuesRuntime::OutputInteger,
               "_ZN7peloton7codegen13ValuesRuntime13OutputIntegerEPcji");
  PROXY_METHOD(OutputBigInt, &peloton::codegen::ValuesRuntime::OutputBigInt,
               "_ZN7peloton7codegen13ValuesRuntime12OutputBigIntEPcjl");
  PROXY_METHOD(OutputDate, &peloton::codegen::ValuesRuntime::OutputDate,
               "_ZN7peloton7codegen13ValuesRuntime10OutputDateEPcji");
  PROXY_METHOD(OutputTimestamp,
               &peloton::codegen::ValuesRuntime::OutputTimestamp,
               "_ZN7peloton7codegen13ValuesRuntime15OutputTimestampEPcjl");
  PROXY_METHOD(OutputDouble, &peloton::codegen::ValuesRuntime::OutputDecimal,
               "_ZN7peloton7codegen13ValuesRuntime13OutputDecimalEPcjd");
  PROXY_METHOD(OutputVarchar, &peloton::codegen::ValuesRuntime::OutputVarchar,
               "_ZN7peloton7codegen13ValuesRuntime13OutputVarcharEPcjS2_j");
  PROXY_METHOD(OutputVarbinary,
               &peloton::codegen::ValuesRuntime::OutputVarbinary,
               "_ZN7peloton7codegen13ValuesRuntime15OutputVarbinaryEPcjS2_j");
  PROXY_METHOD(CompareStrings, &peloton::codegen::ValuesRuntime::CompareStrings,
               "_ZN7peloton7codegen13ValuesRuntime14CompareStringsEPKcjS3_j");
};

}  // namespace codegen
}  // namespace peloton
