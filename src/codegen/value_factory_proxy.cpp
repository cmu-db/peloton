//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// value_factory_proxy.cpp
//
// Identification: src/codegen/value_factory_proxy.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/value_factory_proxy.h"

#include "codegen/value_proxy.h"

namespace peloton {
namespace codegen {

void ValueFactoryProxy::GetTinyIntValue(type::Value *values, uint32_t offset,
                                        int8_t value) {
  values[offset] = type::ValueFactory::GetTinyIntValue(value);
}

void ValueFactoryProxy::GetSmallIntValue(type::Value *values, uint32_t offset,
                                         int16_t value) {
  values[offset] = type::ValueFactory::GetSmallIntValue(value);
}

void ValueFactoryProxy::GetIntegerValue(type::Value *values, uint32_t offset,
                                        int32_t value) {
  values[offset] = type::ValueFactory::GetIntegerValue(value);
}

void ValueFactoryProxy::GetBigIntValue(type::Value *values, uint32_t offset,
                                       int64_t value) {
  values[offset] = type::ValueFactory::GetBigIntValue(value);
}

void ValueFactoryProxy::GetDateValue(type::Value *values, uint32_t offset,
                                     uint32_t value) {
  values[offset] =
      type::ValueFactory::GetDateValue(static_cast<int32_t>(value));
}

void ValueFactoryProxy::GetTimestampValue(type::Value *values, uint32_t offset,
                                          int64_t value) {
  values[offset] = type::ValueFactory::GetTimestampValue(value);
}

void ValueFactoryProxy::GetDecimalValue(type::Value *values, uint32_t offset,
                                        double value) {
  values[offset] = type::ValueFactory::GetDecimalValue(value);
}

void ValueFactoryProxy::GetBooleanValue(type::Value *values, uint32_t offset,
                                        bool value) {
  values[offset] = type::ValueFactory::GetBooleanValue(value);
}

void ValueFactoryProxy::GetVarcharValue(type::Value *values, uint32_t offset,
                                        char *c_str, int len) {
  std::string str = c_str;
  str.resize(len);
  values[offset] = type::ValueFactory::GetVarcharValue(str);
}

void ValueFactoryProxy::GetVarbinaryValue(type::Value *values, uint32_t offset,
                                          char *c_str, int len) {
  std::string str = c_str;
  str.resize(len);
  values[offset] = type::ValueFactory::GetVarbinaryValue(str);
}

//===----------------------------------------------------------------------===//
// Get TINYINT
//===----------------------------------------------------------------------===//

// Get the symbol name for ValuePeeker::PeekTinyInt()
const std::string &ValueFactoryProxy::_GetTinyIntValue::GetFunctionName() {
  static const std::string kOutputTinyIntFnName =
      "_ZN7peloton7codegen17ValueFactoryProxy15GetTinyIntValueEPNS_"
      "4type5ValueEja";
  return kOutputTinyIntFnName;
}

llvm::Function *ValueFactoryProxy::_GetTinyIntValue::GetFunction(
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
// Get SMALLINT
//===----------------------------------------------------------------------===//

const std::string &ValueFactoryProxy::_GetSmallIntValue::GetFunctionName() {
  static const std::string kOutputSmallIntFnName =
      "_ZN7peloton7codegen17ValueFactoryProxy16GetSmallIntValueEPNS_"
      "4type5ValueEjs";
  return kOutputSmallIntFnName;
}

llvm::Function *ValueFactoryProxy::_GetSmallIntValue::GetFunction(
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
// Get INTEGER
//===----------------------------------------------------------------------===//

const std::string &ValueFactoryProxy::_GetIntegerValue::GetFunctionName() {
  static const std::string kOutputIntegerFnName =
      "_ZN7peloton7codegen17ValueFactoryProxy15GetIntegerValueEPNS_"
      "4type5ValueEji";
  return kOutputIntegerFnName;
}

llvm::Function *ValueFactoryProxy::_GetIntegerValue::GetFunction(
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
// Get BIGINT
//===----------------------------------------------------------------------===//

const std::string &ValueFactoryProxy::_GetBigIntValue::GetFunctionName() {
  static const std::string kOutputBigIntFnName =
      "_ZN7peloton7codegen17ValueFactoryProxy14GetBigIntValueEPNS_"
      "4type5ValueEjl";
  return kOutputBigIntFnName;
}

llvm::Function *ValueFactoryProxy::_GetBigIntValue::GetFunction(
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
// Get DOUBLE
//===----------------------------------------------------------------------===//

const std::string &ValueFactoryProxy::_GetDecimalValue::GetFunctionName() {
  static const std::string kOutputDoubleFnName =
      "_ZN7peloton7codegen17ValueFactoryProxy15GetDecimalValueEPNS_"
      "4type5ValueEjd";
  return kOutputDoubleFnName;
}

llvm::Function *ValueFactoryProxy::_GetDecimalValue::GetFunction(
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
// Get TIMESTAMP
//===----------------------------------------------------------------------===//

const std::string &ValueFactoryProxy::_GetTimestampValue::GetFunctionName() {
  static const std::string kOutputVarcharFnName =
#ifdef __APPLE__
      "_ZN7peloton7codegen13ValuesRuntime13outputVarcharEPcjS2_j";
#else
      "_ZN7peloton7codegen17ValueFactoryProxy17GetTimestampValueEPNS_"
      "4type5ValueEjl";
#endif
  return kOutputVarcharFnName;
}

llvm::Function *ValueFactoryProxy::_GetTimestampValue::GetFunction(
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
// Get DATE
//===----------------------------------------------------------------------===//

const std::string &ValueFactoryProxy::_GetDateValue::GetFunctionName() {
  static const std::string kOutputVarcharFnName =
#ifdef __APPLE__
      "_ZN7peloton7codegen13ValuesRuntime13outputVarcharEPcjS2_j";
#else
      "_ZN7peloton7codegen17ValueFactoryProxy12GetDateValueEPNS_4type5ValueEjj";
#endif
  return kOutputVarcharFnName;
}

llvm::Function *ValueFactoryProxy::_GetDateValue::GetFunction(
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
// Get BOOLEAN
//===----------------------------------------------------------------------===//

const std::string &ValueFactoryProxy::_GetBooleanValue::GetFunctionName() {
  static const std::string kOutputVarcharFnName =
#ifdef __APPLE__
      "_ZN7peloton7codegen13ValuesRuntime13outputVarcharEPcjS2_j";
#else
      "_ZN7peloton7codegen17ValueFactoryProxy15GetBooleanValueEPNS_"
      "4type5ValueEjb";
#endif
  return kOutputVarcharFnName;
}

llvm::Function *ValueFactoryProxy::_GetBooleanValue::GetFunction(
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
// Get VarChar
//===----------------------------------------------------------------------===//

const std::string &ValueFactoryProxy::_GetVarcharValue::GetFunctionName() {
  static const std::string kOutputVarcharFnName =
#ifdef __APPLE__
      "_ZN7peloton7codegen13ValuesRuntime13outputVarcharEPcjS2_j";
#else
      "_ZN7peloton7codegen17ValueFactoryProxy15GetVarcharValueEPNS_"
      "4type5ValueEjPci";
#endif
  return kOutputVarcharFnName;
}

llvm::Function *ValueFactoryProxy::_GetVarcharValue::GetFunction(
    CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto *value_type = ValueProxy::GetType(codegen);
  auto *fn_type = llvm::FunctionType::get(
      codegen.VoidType(), {value_type->getPointerTo(), codegen.Int64Type(),
                           codegen.CharPtrType(), codegen.Int32Type()},
      false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// Get VarBinary
//===----------------------------------------------------------------------===//

const std::string &ValueFactoryProxy::_GetVarbinaryValue::GetFunctionName() {
  static const std::string kOutputVarcharFnName =
#ifdef __APPLE__
      "_ZN7peloton7codegen13ValuesRuntime13outputVarcharEPcjS2_j";
#else
      "_ZN7peloton7codegen17ValueFactoryProxy17GetVarbinaryValueEPNS_"
      "4type5ValueEjPci";
#endif
  return kOutputVarcharFnName;
}

llvm::Function *ValueFactoryProxy::_GetVarbinaryValue::GetFunction(
    CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto *value_type = ValueProxy::GetType(codegen);
  auto *fn_type = llvm::FunctionType::get(
      codegen.VoidType(), {value_type->getPointerTo(), codegen.Int64Type(),
                           codegen.CharPtrType(), codegen.Int32Type()},
      false);
  return codegen.RegisterFunction(fn_name, fn_type);
}
}  // namespace codegen
}  // namespace peloton