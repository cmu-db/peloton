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

#include "codegen/value_proxy.h"
#include "type/value.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// OUTPUT BOOLEAN
//===----------------------------------------------------------------------===//

// Get the symbol name for ValuesRuntime::OutputBoolean()
const std::string &ValuesRuntimeProxy::_OutputBoolean::GetFunctionName() {
  static const std::string kOutputTinyIntFnName =
      "_ZN7peloton7codegen13ValuesRuntime13OutputBooleanEPcjb";
  return kOutputTinyIntFnName;
}

llvm::Function *ValuesRuntimeProxy::_OutputBoolean::GetFunction(
    CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto *value_type = ValueProxy::GetType(codegen);
  auto *fn_type = llvm::FunctionType::get(
      codegen.VoidType(),
      {value_type->getPointerTo(), codegen.Int64Type(), codegen.BoolType()},
      false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// OUTPUT TINYINT
//===----------------------------------------------------------------------===//

// Get the symbol name for ValuesRuntime::OutputTinyInt()
const std::string &ValuesRuntimeProxy::_OutputTinyInt::GetFunctionName() {
  static const std::string kOutputTinyIntFnName =
      "_ZN7peloton7codegen13ValuesRuntime13OutputTinyIntEPcja";
  return kOutputTinyIntFnName;
}

llvm::Function *ValuesRuntimeProxy::_OutputTinyInt::GetFunction(
    CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto *value_type = ValueProxy::GetType(codegen);
  auto *fn_type = llvm::FunctionType::get(
      codegen.VoidType(),
      {value_type->getPointerTo(), codegen.Int64Type(), codegen.Int8Type()},
      false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// OUTPUT SMALLINT
//===----------------------------------------------------------------------===//

const std::string &ValuesRuntimeProxy::_OutputSmallInt::GetFunctionName() {
  static const std::string kOutputSmallIntFnName =
      "_ZN7peloton7codegen13ValuesRuntime14OutputSmallIntEPcjs";
  return kOutputSmallIntFnName;
}

llvm::Function *ValuesRuntimeProxy::_OutputSmallInt::GetFunction(
    CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto *value_type = ValueProxy::GetType(codegen);
  auto *fn_type = llvm::FunctionType::get(
      codegen.VoidType(),
      {value_type->getPointerTo(), codegen.Int64Type(), codegen.Int16Type()},
      false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// OUTPUT INTEGER
//===----------------------------------------------------------------------===//

const std::string &ValuesRuntimeProxy::_OutputInteger::GetFunctionName() {
  static const std::string kOutputIntegerFnName =
      "_ZN7peloton7codegen13ValuesRuntime13OutputIntegerEPcji";
  return kOutputIntegerFnName;
}

llvm::Function *ValuesRuntimeProxy::_OutputInteger::GetFunction(
    CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto *value_type = ValueProxy::GetType(codegen);
  auto *fn_type = llvm::FunctionType::get(
      codegen.VoidType(),
      {value_type->getPointerTo(), codegen.Int64Type(), codegen.Int32Type()},
      false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// OUTPUT BIGINT
//===----------------------------------------------------------------------===//

const std::string &ValuesRuntimeProxy::_OutputBigInt::GetFunctionName() {
  static const std::string kOutputBigIntFnName =
#ifdef __APPLE__
      "_ZN7peloton7codegen13ValuesRuntime12OutputBigIntEPcjx";
#else
      "_ZN7peloton7codegen13ValuesRuntime12OutputBigIntEPcjl";
#endif

  return kOutputBigIntFnName;
}

llvm::Function *ValuesRuntimeProxy::_OutputBigInt::GetFunction(
    CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto *value_type = ValueProxy::GetType(codegen);
  auto *fn_type = llvm::FunctionType::get(
      codegen.VoidType(),
      {value_type->getPointerTo(), codegen.Int64Type(), codegen.Int64Type()},
      false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// OUTPUT DOUBLE
//===----------------------------------------------------------------------===//

const std::string &ValuesRuntimeProxy::_OutputDouble::GetFunctionName() {
  static const std::string kOutputDoubleFnName =
      "_ZN7peloton7codegen13ValuesRuntime13OutputDecimalEPcjd";
  return kOutputDoubleFnName;
}

llvm::Function *ValuesRuntimeProxy::_OutputDouble::GetFunction(
    CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto *value_type = ValueProxy::GetType(codegen);
  auto *fn_type = llvm::FunctionType::get(
      codegen.VoidType(),
      {value_type->getPointerTo(), codegen.Int64Type(), codegen.DoubleType()},
      false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// OUTPUT DATE
//===----------------------------------------------------------------------===//

const std::string &ValuesRuntimeProxy::_OutputDate::GetFunctionName() {
  static const std::string kOutputTimestampFnName =
      "_ZN7peloton7codegen13ValuesRuntime10OutputDateEPcji";
  return kOutputTimestampFnName;
}

llvm::Function *ValuesRuntimeProxy::_OutputDate::GetFunction(CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto *value_type = ValueProxy::GetType(codegen);
  auto *fn_type = llvm::FunctionType::get(
      codegen.VoidType(),
      {value_type->getPointerTo(), codegen.Int64Type(), codegen.Int32Type()},
      false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// OUTPUT TIMESTAMP
//===----------------------------------------------------------------------===//

const std::string &ValuesRuntimeProxy::_OutputTimestamp::GetFunctionName() {
  static const std::string kOutputTimestampFnName =
#ifdef __APPLE__
      "_ZN7peloton7codegen13ValuesRuntime15OutputTimestampEPcjx";
#else
      "_ZN7peloton7codegen13ValuesRuntime15OutputTimestampEPcjl";
#endif
  return kOutputTimestampFnName;
}

llvm::Function *ValuesRuntimeProxy::_OutputTimestamp::GetFunction(
    CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto *value_type = ValueProxy::GetType(codegen);
  auto *fn_type = llvm::FunctionType::get(
      codegen.VoidType(),
      {value_type->getPointerTo(), codegen.Int64Type(), codegen.Int64Type()},
      false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// OUTPUT VARCHAR
//===----------------------------------------------------------------------===//

const std::string &ValuesRuntimeProxy::_OutputVarchar::GetFunctionName() {
  static const std::string kOutputVarcharFnName =
#ifdef __APPLE__
      "_ZN7peloton7codegen13ValuesRuntime13OutputVarcharEPcjS2_j";
#else
      "_ZN7peloton7codegen13ValuesRuntime13OutputVarcharEPcjS2_j";
#endif
  return kOutputVarcharFnName;
}

llvm::Function *ValuesRuntimeProxy::_OutputVarchar::GetFunction(
    CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto *value_type = ValueProxy::GetType(codegen);
  std::vector<llvm::Type *> arg_types = {
      value_type->getPointerTo(), codegen.Int64Type(), codegen.CharPtrType(),
      codegen.Int32Type()};
  auto *fn_type = llvm::FunctionType::get(codegen.VoidType(), arg_types, false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// OUTPUT VARBINARY
//===----------------------------------------------------------------------===//

const std::string &ValuesRuntimeProxy::_OutputVarbinary::GetFunctionName() {
  static const std::string kOutputVarbinaryFnName =
      "_ZN7peloton7codegen13ValuesRuntime15OutputVarbinaryEPcjS2_j";
  return kOutputVarbinaryFnName;
}

llvm::Function *ValuesRuntimeProxy::_OutputVarbinary::GetFunction(
    CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto *value_type = ValueProxy::GetType(codegen);
  std::vector<llvm::Type *> arg_types = {
      value_type->getPointerTo(), codegen.Int64Type(), codegen.CharPtrType(),
      codegen.Int32Type()};
  auto *fn_type = llvm::FunctionType::get(codegen.VoidType(), arg_types, false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// COMPARE STRINGS
//===----------------------------------------------------------------------===//

const std::string &ValuesRuntimeProxy::_CompareStrings::GetFunctionName() {
  static const std::string kCompareStringsFnName =
#ifdef __APPLE__
      "_ZN7peloton7codegen13ValuesRuntime14CompareStringsEPKcjS3_j";
#else
      "_ZN7peloton7codegen13ValuesRuntime14CompareStringsEPKcjS3_j";
#endif
  return kCompareStringsFnName;
}

llvm::Function *ValuesRuntimeProxy::_CompareStrings::GetFunction(
    CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  std::vector<llvm::Type *> arg_types = {codegen.CharPtrType(),  // str1
                                         codegen.Int32Type(),    // str1 length
                                         codegen.CharPtrType(),  // str2
                                         codegen.Int32Type()};   // str2 length
  auto *fn_type =
      llvm::FunctionType::get(codegen.Int32Type(), arg_types, false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

}  // namespace codegen
}  // namespace peloton
