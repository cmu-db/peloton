//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// primitive_value_proxy.cpp
//
// Identification: src/codegen/primitive_value_proxy.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/primitive_value_proxy.h"

namespace peloton {
namespace codegen {

int8_t PrimitiveValueProxy::GetTinyInt(char **values, uint32_t offset) {
  return *reinterpret_cast<int8_t *>(values[offset]);
}

int16_t PrimitiveValueProxy::GetSmallInt(char **values, uint32_t offset) {
  return *reinterpret_cast<int16_t *>(values[offset]);
}

int32_t PrimitiveValueProxy::GetInteger(char **values, uint32_t offset) {
  return *reinterpret_cast<int32_t *>(values[offset]);
}

int64_t PrimitiveValueProxy::GetBigInt(char **values, uint32_t offset) {
  return *reinterpret_cast<int64_t *>(values[offset]);
}

double PrimitiveValueProxy::GetDouble(char **values, uint32_t offset) {
  return *reinterpret_cast<double *>(values[offset]);
}

int32_t PrimitiveValueProxy::GetDate(char **values, uint32_t offset) {
  return *reinterpret_cast<int32_t *>(values[offset]);
}

uint64_t PrimitiveValueProxy::GetTimestamp(char **values, uint32_t offset) {
  return *reinterpret_cast<uint64_t *>(values[offset]);
}

char *PrimitiveValueProxy::GetVarcharVal(char **values, uint32_t offset) {
  return values[offset];
}

size_t PrimitiveValueProxy::GetVarcharLen(int32_t *values, uint32_t offset) {
  return values[offset];
}

//===----------------------------------------------------------------------===//
// GET TINYINT
//===----------------------------------------------------------------------===//

// Get the symbol name for PrimitiveValueProxy::GetTinyInt()
const std::string &PrimitiveValueProxy::_GetTinyInt::GetFunctionName() {
  static const std::string kGetTinyIntFnName =
      "_ZN7peloton7codegen19PrimitiveValueProxy10GetTinyIntEPPcj";
  return kGetTinyIntFnName;
}

llvm::Function *PrimitiveValueProxy::_GetTinyInt::GetFunction(
    CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto *fn_type = llvm::FunctionType::get(
      codegen.Int8Type(),
      {codegen.CharPtrType()->getPointerTo(), codegen.Int64Type()}, false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// GET SMALLINT
//===----------------------------------------------------------------------===//

const std::string &PrimitiveValueProxy::_GetSmallInt::GetFunctionName() {
  static const std::string kGetSmallIntFnName =
      "_ZN7peloton7codegen19PrimitiveValueProxy11GetSmallIntEPPcj";
  return kGetSmallIntFnName;
}

llvm::Function *PrimitiveValueProxy::_GetSmallInt::GetFunction(
    CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto *fn_type = llvm::FunctionType::get(
      codegen.Int16Type(),
      {codegen.CharPtrType()->getPointerTo(), codegen.Int64Type()}, false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// GET INTEGER
//===----------------------------------------------------------------------===//

const std::string &PrimitiveValueProxy::_GetInteger::GetFunctionName() {
  static const std::string kGetIntegerFnName =
      "_ZN7peloton7codegen19PrimitiveValueProxy10GetIntegerEPPcj";
  return kGetIntegerFnName;
}

llvm::Function *PrimitiveValueProxy::_GetInteger::GetFunction(
    CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto *fn_type = llvm::FunctionType::get(
      codegen.Int32Type(),
      {codegen.CharPtrType()->getPointerTo(), codegen.Int64Type()}, false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// GET BIGINT
//===----------------------------------------------------------------------===//

const std::string &PrimitiveValueProxy::_GetBigInt::GetFunctionName() {
  static const std::string kGetBigIntFnName =
      "_ZN7peloton7codegen19PrimitiveValueProxy9GetBigIntEPPcj";
  return kGetBigIntFnName;
}

llvm::Function *PrimitiveValueProxy::_GetBigInt::GetFunction(CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto *fn_type = llvm::FunctionType::get(
      codegen.Int64Type(),
      {codegen.CharPtrType()->getPointerTo(), codegen.Int64Type()}, false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// GET DOUBLE
//===----------------------------------------------------------------------===//

const std::string &PrimitiveValueProxy::_GetDouble::GetFunctionName() {
  static const std::string kGetDoubleFnName =
      "_ZN7peloton7codegen19PrimitiveValueProxy9GetDoubleEPPcj";
  return kGetDoubleFnName;
}

llvm::Function *PrimitiveValueProxy::_GetDouble::GetFunction(CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto *fn_type = llvm::FunctionType::get(
      codegen.DoubleType(),
      {codegen.CharPtrType()->getPointerTo(), codegen.Int64Type()}, false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// PEEK TIMESTAMP
//===----------------------------------------------------------------------===//

const std::string &PrimitiveValueProxy::_GetTimestamp::GetFunctionName() {
  static const std::string kGetTimestampFnName =
#ifdef __APPLE__
      "_ZN7peloton7codegen13ValuesRuntime13outputVarcharEPcjS2_j";
#else
      "_ZN7peloton7codegen19PrimitiveValueProxy12GetTimestampEPPcj";
#endif
  return kGetTimestampFnName;
}

llvm::Function *PrimitiveValueProxy::_GetTimestamp::GetFunction(
    CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto *fn_type = llvm::FunctionType::get(
      codegen.Int64Type(),
      {codegen.CharPtrType()->getPointerTo(), codegen.Int64Type()}, false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// GET DATE
//===----------------------------------------------------------------------===//

const std::string &PrimitiveValueProxy::_GetDate::GetFunctionName() {
  static const std::string kGetDateFnName =
#ifdef __APPLE__
      "_ZN7peloton7codegen13ValuesRuntime13outputVarcharEPcjS2_j";
#else
      "_ZN7peloton7codegen19PrimitiveValueProxy7GetDateEPPcj";
#endif
  return kGetDateFnName;
}

llvm::Function *PrimitiveValueProxy::_GetDate::GetFunction(CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto *fn_type = llvm::FunctionType::get(
      codegen.Int32Type(),
      {codegen.CharPtrType()->getPointerTo(), codegen.Int64Type()}, false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// GET VARCHAR VAL
//===----------------------------------------------------------------------===//

const std::string &PrimitiveValueProxy::_GetVarcharVal::GetFunctionName() {
  static const std::string kGetVarcharValFnName =
#ifdef __APPLE__
      "_ZN7peloton7codegen13ValuesRuntime13outputVarcharEPcjS2_j";
#else
      "_ZN7peloton7codegen19PrimitiveValueProxy13GetVarcharValEPPcj";
#endif
  return kGetVarcharValFnName;
}

llvm::Function *PrimitiveValueProxy::_GetVarcharVal::GetFunction(
    CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto *fn_type = llvm::FunctionType::get(
      codegen.CharPtrType(),
      {codegen.CharPtrType()->getPointerTo(), codegen.Int64Type()}, false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// GET VARCHAR LEN
//===----------------------------------------------------------------------===//

const std::string &PrimitiveValueProxy::_GetVarcharLen::GetFunctionName() {
  static const std::string kGetVarcharLenFnName =
#ifdef __APPLE__
      "_ZN7peloton7codegen13ValuesRuntime13outputVarcharEPcjS2_j";
#else
      "_ZN7peloton7codegen19PrimitiveValueProxy13GetVarcharLenEPij";
#endif
  return kGetVarcharLenFnName;
}

llvm::Function *PrimitiveValueProxy::_GetVarcharLen::GetFunction(
    CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto *fn_type = llvm::FunctionType::get(
      codegen.Int32Type(),
      {codegen.Int32Type()->getPointerTo(), codegen.Int64Type()}, false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

}  // namespace codegen
}  // namespace peloton