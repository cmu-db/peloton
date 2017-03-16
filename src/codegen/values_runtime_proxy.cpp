//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// values_runtime_proxy.cpp
//
// Identification: src/codegen/values_runtime_proxy.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/values_runtime_proxy.h"

#include "codegen/value_proxy.h"
#include "type/value.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// OUTPUT TINYINT
//===----------------------------------------------------------------------===//

// Get the symbol name for ValuesRuntime::OutputTinyInt()
const std::string& ValuesRuntimeProxy::_OutputTinyInt::GetFunctionName() {
  static const std::string kOutputTinyIntFnName =
      "_ZN7peloton7codegen13ValuesRuntime13OutputTinyIntEPcja";
  return kOutputTinyIntFnName;
}

llvm::Function* ValuesRuntimeProxy::_OutputTinyInt::GetFunction(
    CodeGen& codegen) {
  const std::string& fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function* llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto* value_type = ValueProxy::GetType(codegen);
  auto* fn_type = llvm::FunctionType::get(
      codegen.VoidType(),
      {value_type->getPointerTo(), codegen.Int64Type(), codegen.Int16Type()},
      false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// OUTPUT SMALLINT
//===----------------------------------------------------------------------===//

const std::string& ValuesRuntimeProxy::_OutputSmallInt::GetFunctionName() {
  static const std::string kOutputSmallIntFnName =
      "_ZN7peloton7codegen13ValuesRuntime14OutputSmallIntEPcjs";
  return kOutputSmallIntFnName;
}

llvm::Function* ValuesRuntimeProxy::_OutputSmallInt::GetFunction(
    CodeGen& codegen) {
  const std::string& fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function* llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto* value_type = ValueProxy::GetType(codegen);
  auto* fn_type = llvm::FunctionType::get(
      codegen.VoidType(),
      {value_type->getPointerTo(), codegen.Int64Type(), codegen.Int16Type()},
      false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// OUTPUT INTEGER
//===----------------------------------------------------------------------===//

const std::string& ValuesRuntimeProxy::_OutputInteger::GetFunctionName() {
  static const std::string kOutputIntegerFnName =
      "_ZN7peloton7codegen13ValuesRuntime13OutputIntegerEPcji";
  return kOutputIntegerFnName;
}

llvm::Function* ValuesRuntimeProxy::_OutputInteger::GetFunction(
    CodeGen& codegen) {
  const std::string& fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function* llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto* value_type = ValueProxy::GetType(codegen);
  auto* fn_type = llvm::FunctionType::get(
      codegen.VoidType(),
      {value_type->getPointerTo(), codegen.Int64Type(), codegen.Int32Type()},
      false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// OUTPUT BIGINT
//===----------------------------------------------------------------------===//

const std::string& ValuesRuntimeProxy::_OutputBigInt::GetFunctionName() {
  static const std::string kOutputBigIntFnName =
      "_ZN7peloton7codegen13ValuesRuntime12OutputBigIntEPcjl";
  return kOutputBigIntFnName;
}

llvm::Function* ValuesRuntimeProxy::_OutputBigInt::GetFunction(
    CodeGen& codegen) {
  const std::string& fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function* llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto* value_type = ValueProxy::GetType(codegen);
  auto* fn_type = llvm::FunctionType::get(
      codegen.VoidType(),
      {value_type->getPointerTo(), codegen.Int64Type(), codegen.Int64Type()},
      false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// OUTPUT DOUBLE
//===----------------------------------------------------------------------===//

const std::string& ValuesRuntimeProxy::_OutputDouble::GetFunctionName() {
  static const std::string kOutputDoubleFnName =
      "_ZN7peloton7codegen13ValuesRuntime13OutputDecimalEPcjd";
  return kOutputDoubleFnName;
}

llvm::Function* ValuesRuntimeProxy::_OutputDouble::GetFunction(
    CodeGen& codegen) {
  const std::string& fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function* llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto* value_type = ValueProxy::GetType(codegen);
  auto* fn_type = llvm::FunctionType::get(
      codegen.VoidType(),
      {value_type->getPointerTo(), codegen.Int64Type(), codegen.DoubleType()},
      false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// OUTPUT VARCHAR
//===----------------------------------------------------------------------===//

const std::string& ValuesRuntimeProxy::_OutputVarchar::GetFunctionName() {
  static const std::string kOutputVarcharFnName =
#ifdef __APPLE__
      "_ZN7peloton7codegen13ValuesRuntime13outputVarcharEPcjS2_j";
#else
      "_ZN7peloton7codegen13ValuesRuntime13OutputVarcharEPcjS2_j";
#endif
  return kOutputVarcharFnName;
}

llvm::Function* ValuesRuntimeProxy::_OutputVarchar::GetFunction(
    CodeGen& codegen) {
  const std::string& fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function* llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto* value_type = ValueProxy::GetType(codegen);
  std::vector<llvm::Type*> arg_types = {
      value_type->getPointerTo(), codegen.Int64Type(), codegen.CharPtrType(),
      codegen.Int32Type()};
  auto* fn_type = llvm::FunctionType::get(codegen.VoidType(), arg_types, false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

}  // namespace codegen
}  // namespace peloton