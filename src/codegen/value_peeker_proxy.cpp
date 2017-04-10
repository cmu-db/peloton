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

#include "codegen/value_peeker_proxy.h"

#include "codegen/value_proxy.h"

namespace peloton {
namespace codegen {

int8_t ValuePeekerProxy::PeekTinyInt(type::Value *value) {
  return type::ValuePeeker::PeekTinyInt(*value);
}

int16_t ValuePeekerProxy::PeekSmallInt(type::Value *value) {
  return type::ValuePeeker::PeekSmallInt(*value);
}

int32_t ValuePeekerProxy::PeekInteger(type::Value *value) {
  return type::ValuePeeker::PeekInteger(*value);
}

int64_t ValuePeekerProxy::PeekBigInt(type::Value *value) {
  return type::ValuePeeker::PeekBigInt(*value);
}

double ValuePeekerProxy::PeekDouble(type::Value *value) {
  return type::ValuePeeker::PeekDouble(*value);
}

int32_t ValuePeekerProxy::PeekDate(type::Value *value) {
  return type::ValuePeeker::PeekDate(*value);
}

uint64_t ValuePeekerProxy::PeekTimestamp(type::Value *value) {
  return type::ValuePeeker::PeekTimestamp(*value);
}

char *ValuePeekerProxy::PeekVarcharVal(type::Value *value) {
  std::string str_val = type::ValuePeeker::PeekVarchar(*value);
  char *str = new char[str_val.length() + 1];
  strcpy(str, str_val.c_str());
  return str;
}

size_t ValuePeekerProxy::PeekVarcharLen(type::Value *value) {
  std::string str = type::ValuePeeker::PeekVarchar(*value);
  return str.length();
}

//===----------------------------------------------------------------------===//
// PEEK TINYINT
//===----------------------------------------------------------------------===//

// Get the symbol name for ValuePeeker::PeekTinyInt()
const std::string &ValuePeekerProxy::_PeekTinyInt::GetFunctionName() {
  static const std::string kOutputTinyIntFnName =
      "_ZN7peloton7codegen16ValuePeekerProxy11PeekTinyIntEPNS_4type5ValueE";
  return kOutputTinyIntFnName;
}

llvm::Function *ValuePeekerProxy::_PeekTinyInt::GetFunction(
    CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto *value_type = ValueProxy::GetType(codegen);
  auto *fn_type = llvm::FunctionType::get(
      codegen.Int8Type(),
      {value_type->getPointerTo()},
      false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// PEEK SMALLINT
//===----------------------------------------------------------------------===//

const std::string &ValuePeekerProxy::_PeekSmallInt::GetFunctionName() {
  static const std::string kOutputSmallIntFnName =
      "_ZN7peloton7codegen16ValuePeekerProxy12PeekSmallIntEPNS_4type5ValueE";
  return kOutputSmallIntFnName;
}

llvm::Function *ValuePeekerProxy::_PeekSmallInt::GetFunction(
    CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto *value_type = ValueProxy::GetType(codegen);
  auto *fn_type = llvm::FunctionType::get(
      codegen.Int16Type(),
      {value_type->getPointerTo()},
      false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// PEEK INTEGER
//===----------------------------------------------------------------------===//

const std::string &ValuePeekerProxy::_PeekInteger::GetFunctionName() {
  static const std::string kOutputIntegerFnName =
      "_ZN7peloton7codegen16ValuePeekerProxy11PeekIntegerEPNS_4type5ValueE";
  return kOutputIntegerFnName;
}

llvm::Function *ValuePeekerProxy::_PeekInteger::GetFunction(
    CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto *value_type = ValueProxy::GetType(codegen);
  auto *fn_type = llvm::FunctionType::get(
      codegen.Int32Type(),
      {value_type->getPointerTo()},
      false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// PEEK BIGINT
//===----------------------------------------------------------------------===//

const std::string &ValuePeekerProxy::_PeekBigInt::GetFunctionName() {
  static const std::string kOutputBigIntFnName =
      "_ZN7peloton7codegen16ValuePeekerProxy10PeekBigIntEPNS_4type5ValueE";
  return kOutputBigIntFnName;
}

llvm::Function *ValuePeekerProxy::_PeekBigInt::GetFunction(
    CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto *value_type = ValueProxy::GetType(codegen);
  auto *fn_type = llvm::FunctionType::get(
      codegen.Int64Type(),
      {value_type->getPointerTo()},
      false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// PEEK DOUBLE
//===----------------------------------------------------------------------===//

const std::string &ValuePeekerProxy::_PeekDouble::GetFunctionName() {
  static const std::string kOutputDoubleFnName =
      "_ZN7peloton7codegen16ValuePeekerProxy10PeekDoubleEPNS_4type5ValueE";
  return kOutputDoubleFnName;
}

llvm::Function *ValuePeekerProxy::_PeekDouble::GetFunction(
    CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto *value_type = ValueProxy::GetType(codegen);
  auto *fn_type = llvm::FunctionType::get(
      codegen.DoubleType(),
      {value_type->getPointerTo()},
      false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// PEEK TIMESTAMP
//===----------------------------------------------------------------------===//

const std::string &ValuePeekerProxy::_PeekTimestamp::GetFunctionName() {
  static const std::string kOutputVarcharFnName =
#ifdef __APPLE__
        "_ZN7peloton7codegen13ValuesRuntime13outputVarcharEPcjS2_j";
#else
        "_ZN7peloton7codegen16ValuePeekerProxy13PeekTimestampEPNS_4type5ValueE";
#endif
  return kOutputVarcharFnName;
}

llvm::Function *ValuePeekerProxy::_PeekTimestamp::GetFunction(
    CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto *value_type = ValueProxy::GetType(codegen);
  auto *fn_type = llvm::FunctionType::get(
          codegen.Int64Type(),
          {value_type->getPointerTo()},
          false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// PEEK DATE
//===----------------------------------------------------------------------===//

const std::string &ValuePeekerProxy::_PeekDate::GetFunctionName() {
  static const std::string kOutputVarcharFnName =
#ifdef __APPLE__
        "_ZN7peloton7codegen13ValuesRuntime13outputVarcharEPcjS2_j";
#else
        "_ZN7peloton7codegen16ValuePeekerProxy8PeekDateEPNS_4type5ValueE";
#endif
  return kOutputVarcharFnName;
}

llvm::Function *ValuePeekerProxy::_PeekDate::GetFunction(
    CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto *value_type = ValueProxy::GetType(codegen);
  auto *fn_type = llvm::FunctionType::get(
          codegen.Int32Type(),
          {value_type->getPointerTo()},
          false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// PEEK VARCHAR VAL
//===----------------------------------------------------------------------===//

const std::string &ValuePeekerProxy::_PeekVarcharVal::GetFunctionName() {
  static const std::string kOutputVarcharFnName =
#ifdef __APPLE__
      "_ZN7peloton7codegen13ValuesRuntime13outputVarcharEPcjS2_j";
#else
      "_ZN7peloton7codegen16ValuePeekerProxy14PeekVarcharValEPNS_4type5ValueE";
#endif
  return kOutputVarcharFnName;
}

llvm::Function *ValuePeekerProxy::_PeekVarcharVal::GetFunction(
    CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto *value_type = ValueProxy::GetType(codegen);
  auto *fn_type = llvm::FunctionType::get(
          codegen.CharPtrType(),
          {value_type->getPointerTo()},
          false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// PEEK VARCHAR LEN
//===----------------------------------------------------------------------===//

const std::string &ValuePeekerProxy::_PeekVarcharLen::GetFunctionName() {
  static const std::string kOutputVarcharFnName =
#ifdef __APPLE__
        "_ZN7peloton7codegen13ValuesRuntime13outputVarcharEPcjS2_j";
#else
        "_ZN7peloton7codegen16ValuePeekerProxy14PeekVarcharLenEPNS_4type5ValueE";
#endif
  return kOutputVarcharFnName;
}

llvm::Function *ValuePeekerProxy::_PeekVarcharLen::GetFunction(
    CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto *value_type = ValueProxy::GetType(codegen);
  auto *fn_type = llvm::FunctionType::get(
          codegen.Int32Type(),
          {value_type->getPointerTo()},
          false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

}  // namespace codegen
}  // namespace peloton