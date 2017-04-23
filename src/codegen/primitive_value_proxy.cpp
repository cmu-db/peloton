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

int8_t PrimitiveValueProxy::GetTinyInt(
        int8_t *values, uint32_t offset) {
  return values[offset];
}

int16_t PrimitiveValueProxy::GetSmallInt(
        int16_t *values, uint32_t offset) {
  return values[offset];
}

int32_t PrimitiveValueProxy::GetInteger(
        int32_t *values, uint32_t offset) {
  return values[offset];
}

int64_t PrimitiveValueProxy::GetBigInt(
        int64_t *values, uint32_t offset) {
  return values[offset];
}

double PrimitiveValueProxy::GetDouble(
        double *values, uint32_t offset) {
  return values[offset];
}

int32_t PrimitiveValueProxy::GetDate(
        int32_t *values, uint32_t offset) {
  return values[offset];
}

uint64_t PrimitiveValueProxy::GetTimestamp(
        int8_t *values, uint32_t offset) {
  return values[offset];
}

char *PrimitiveValueProxy::GetVarcharVal(
        char **values, uint32_t offset) {
  return values[offset];
}

size_t PrimitiveValueProxy::GetVarcharLen(
        int32_t *values, uint32_t offset) {
  return values[offset];
}

//===----------------------------------------------------------------------===//
// GET TINYINT
//===----------------------------------------------------------------------===//

// Get the symbol name for ValuePeeker::PeekTinyInt()
const std::string &PrimitiveValueProxy::_GetTinyInt::GetFunctionName() {
  static const std::string kOutputTinyIntFnName =
      "_ZN7peloton7codegen19PrimitiveValueProxy10GetTinyIntEPaj";
  return kOutputTinyIntFnName;
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
      {codegen.Int8Type()->getPointerTo(), codegen.Int64Type()},
      false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// PEEK SMALLINT
//===----------------------------------------------------------------------===//

const std::string &PrimitiveValueProxy::_GetSmallInt::GetFunctionName() {
  static const std::string kOutputSmallIntFnName =
      "_ZN7peloton7codegen19PrimitiveValueProxy11GetSmallIntEPsj";
  return kOutputSmallIntFnName;
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
      {codegen.Int16Type()->getPointerTo(), codegen.Int64Type()},
      false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// PEEK INTEGER
//===----------------------------------------------------------------------===//

const std::string &PrimitiveValueProxy::_GetInteger::GetFunctionName() {
  static const std::string kOutputIntegerFnName =
      "_ZN7peloton7codegen19PrimitiveValueProxy10GetIntegerEPij";
  return kOutputIntegerFnName;
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
      {codegen.Int32Type()->getPointerTo(), codegen.Int64Type()},
      false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// PEEK BIGINT
//===----------------------------------------------------------------------===//

const std::string &PrimitiveValueProxy::_GetBigInt::GetFunctionName() {
  static const std::string kOutputBigIntFnName =
      "_ZN7peloton7codegen19PrimitiveValueProxy9GetBigIntEPlj";
  return kOutputBigIntFnName;
}

llvm::Function *PrimitiveValueProxy::_GetBigInt::GetFunction(
    CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto *fn_type = llvm::FunctionType::get(
      codegen.Int64Type(),
      {codegen.Int64Type()->getPointerTo(), codegen.Int64Type()},
      false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// PEEK DOUBLE
//===----------------------------------------------------------------------===//

const std::string &PrimitiveValueProxy::_GetDouble::GetFunctionName() {
  static const std::string kOutputDoubleFnName =
      "_ZN7peloton7codegen19PrimitiveValueProxy9GetDoubleEPdj";
  return kOutputDoubleFnName;
}

llvm::Function *PrimitiveValueProxy::_GetDouble::GetFunction(
    CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto *fn_type = llvm::FunctionType::get(
      codegen.DoubleType(),
      {codegen.DoubleType()->getPointerTo(), codegen.Int64Type()},
      false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// PEEK TIMESTAMP
//===----------------------------------------------------------------------===//

const std::string &PrimitiveValueProxy::_GetTimestamp::GetFunctionName() {
  static const std::string kOutputVarcharFnName =
#ifdef __APPLE__
        "_ZN7peloton7codegen13ValuesRuntime13outputVarcharEPcjS2_j";
#else
        "_ZN7peloton7codegen19PrimitiveValueProxy12GetTimestampEPaj";
#endif
  return kOutputVarcharFnName;
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
          {codegen.Int64Type()->getPointerTo(), codegen.Int64Type()},
          false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// PEEK DATE
//===----------------------------------------------------------------------===//

const std::string &PrimitiveValueProxy::_GetDate::GetFunctionName() {
  static const std::string kOutputVarcharFnName =
#ifdef __APPLE__
        "_ZN7peloton7codegen13ValuesRuntime13outputVarcharEPcjS2_j";
#else
        "_ZN7peloton7codegen19PrimitiveValueProxy7GetDateEPij";
#endif
  return kOutputVarcharFnName;
}

llvm::Function *PrimitiveValueProxy::_GetDate::GetFunction(
    CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto *fn_type = llvm::FunctionType::get(
          codegen.Int32Type(),
          {codegen.Int32Type()->getPointerTo(), codegen.Int64Type()},
          false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// PEEK VARCHAR VAL
//===----------------------------------------------------------------------===//

const std::string &PrimitiveValueProxy::_GetVarcharVal::GetFunctionName() {
  static const std::string kOutputVarcharFnName =
#ifdef __APPLE__
      "_ZN7peloton7codegen13ValuesRuntime13outputVarcharEPcjS2_j";
#else
      "_ZN7peloton7codegen19PrimitiveValueProxy13GetVarcharValEPPcj";
#endif
  return kOutputVarcharFnName;
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
          {codegen.CharPtrType()->getPointerTo(), codegen.Int64Type()},
          false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// PEEK VARCHAR LEN
//===----------------------------------------------------------------------===//

const std::string &PrimitiveValueProxy::_GetVarcharLen::GetFunctionName() {
  static const std::string kOutputVarcharFnName =
#ifdef __APPLE__
        "_ZN7peloton7codegen13ValuesRuntime13outputVarcharEPcjS2_j";
#else
        "_ZN7peloton7codegen19PrimitiveValueProxy13GetVarcharLenEPij";
#endif
  return kOutputVarcharFnName;
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
          {codegen.Int32Type()->getPointerTo(), codegen.Int64Type()},
          false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

}  // namespace codegen
}  // namespace peloton