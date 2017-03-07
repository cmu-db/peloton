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

#include "type/value.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// Get the LLVM type for peloton::Value
//===----------------------------------------------------------------------===//
llvm::Type* ValuesRuntimeProxy::GetType(CodeGen& codegen) {
  static const std::string kValueTypeName = "peloton::Value";
  auto* value_type = codegen.LookupTypeByName(kValueTypeName);
  if (value_type != nullptr) {
    return value_type;
  }

  // Type isnt cached, create it
  auto* byte_array =
      llvm::ArrayType::get(codegen.Int8Type(), sizeof(type::Value));
  value_type = llvm::StructType::create(codegen.GetContext(), {byte_array},
                                        kValueTypeName);
  return value_type;
}

//===----------------------------------------------------------------------===//
// Get the symbol name for ValuesRuntime::outputTinyInt()
//===----------------------------------------------------------------------===//
const std::string& ValuesRuntimeProxy::_outputTinyInt::GetFunctionName() {
  static const std::string kOutputTinyIntFnName =
      "_ZN7peloton7codegen13ValuesRuntime13outputTinyIntEPcja";
  return kOutputTinyIntFnName;
}

llvm::Function* ValuesRuntimeProxy::_outputTinyInt::GetFunction(
    CodeGen& codegen) {
  const std::string& fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function* llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto* value_type = ValuesRuntimeProxy::GetType(codegen);
  auto* fn_type = llvm::FunctionType::get(
      codegen.VoidType(),
      {value_type->getPointerTo(), codegen.Int64Type(), codegen.Int16Type()},
      false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// Get the symbol name for ValuesRuntime::outputSmallInt()
//===----------------------------------------------------------------------===//
const std::string& ValuesRuntimeProxy::_outputSmallInt::GetFunctionName() {
  static const std::string kOutputSmallIntFnName =
      "_ZN7peloton7codegen13ValuesRuntime14outputSmallIntEPcjs";
  return kOutputSmallIntFnName;
}

llvm::Function* ValuesRuntimeProxy::_outputSmallInt::GetFunction(
    CodeGen& codegen) {
  const std::string& fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function* llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto* value_type = ValuesRuntimeProxy::GetType(codegen);
  auto* fn_type = llvm::FunctionType::get(
      codegen.VoidType(),
      {value_type->getPointerTo(), codegen.Int64Type(), codegen.Int16Type()},
      false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// Get the symbol name and LLVM function for ValuesRuntime::outputInteger()
//===----------------------------------------------------------------------===//
const std::string& ValuesRuntimeProxy::_outputInteger::GetFunctionName() {
  static const std::string kOutputIntegerFnName =
      "_ZN7peloton7codegen13ValuesRuntime13outputIntegerEPcji";
  return kOutputIntegerFnName;
}

llvm::Function* ValuesRuntimeProxy::_outputInteger::GetFunction(
    CodeGen& codegen) {
  const std::string& fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function* llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto* value_type = ValuesRuntimeProxy::GetType(codegen);
  auto* fn_type = llvm::FunctionType::get(
      codegen.VoidType(),
      {value_type->getPointerTo(), codegen.Int64Type(), codegen.Int32Type()},
      false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// Get the symbol name and LLVM function for ValuesRuntime::outputIntegerBatch()
//===----------------------------------------------------------------------===//
const std::string& ValuesRuntimeProxy::_outputBigInt::GetFunctionName() {
  static const std::string kOutputBigIntFnName =
      "_ZN7peloton7codegen13ValuesRuntime12outputBigIntEPcjl";
  return kOutputBigIntFnName;
}

llvm::Function* ValuesRuntimeProxy::_outputBigInt::GetFunction(
    CodeGen& codegen) {
  const std::string& fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function* llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto* value_type = ValuesRuntimeProxy::GetType(codegen);
  auto* fn_type = llvm::FunctionType::get(
      codegen.VoidType(),
      {value_type->getPointerTo(), codegen.Int64Type(), codegen.Int64Type()},
      false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// Get the symbol name and LLVM function proxy for ValuesRuntime::outputDouble()
//===----------------------------------------------------------------------===//
const std::string& ValuesRuntimeProxy::_outputDouble::GetFunctionName() {
  static const std::string kOutputDoubleFnName =
      "_ZN7peloton7codegen13ValuesRuntime12outputDoubleEPcjd";
  return kOutputDoubleFnName;
}

llvm::Function* ValuesRuntimeProxy::_outputDouble::GetFunction(
    CodeGen& codegen) {
  const std::string& fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function* llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto* value_type = ValuesRuntimeProxy::GetType(codegen);
  auto* fn_type = llvm::FunctionType::get(
      codegen.VoidType(),
      {value_type->getPointerTo(), codegen.Int64Type(), codegen.DoubleType()},
      false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// Get the symbol name and LLVM function proxy for
// ValuesRuntime::outputVarchar()
//===----------------------------------------------------------------------===//
const std::string& ValuesRuntimeProxy::_outputVarchar::GetFunctionName() {
  static const std::string kOutputVarcharFnName =
#ifdef __APPLE__
      "_ZN7peloton7codegen13ValuesRuntime13outputVarcharEPcjS2_j";
#else
      "_ZN7peloton7codegen13ValuesRuntime13outputVarcharEPcjS2_j";
#endif
  return kOutputVarcharFnName;
}

llvm::Function* ValuesRuntimeProxy::_outputVarchar::GetFunction(
    CodeGen& codegen) {
  const std::string& fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function* llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto* value_type = ValuesRuntimeProxy::GetType(codegen);
  std::vector<llvm::Type*> arg_types = {
      value_type->getPointerTo(), codegen.Int64Type(), codegen.CharPtrType(),
      codegen.Int32Type()};
  auto* fn_type = llvm::FunctionType::get(codegen.VoidType(), arg_types, false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

}  // namespace codegen
}  // namespace peloton