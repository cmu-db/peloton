//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// catalog_proxy.cpp
//
// Identification: src/codegen/catalog_proxy.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/value_proxy.h"

#include "type/type.h"
#include "type/value_peeker.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// Return the LLVM type that matches the memory layout of our Database class
//===----------------------------------------------------------------------===//
llvm::Type* ValueProxy::GetType(CodeGen& codegen) {
  static const std::string kValueTypeName = "peloton::Value";
  auto* value_type = codegen.LookupTypeByName(kValueTypeName);
  if (value_type != nullptr) {
    return value_type;
  }

  // Type isnt cached, create it
  auto* opaque_arr_type =
        codegen.VectorType(codegen.Int8Type(), sizeof(type::Value));
  return llvm::StructType::create(codegen.GetContext(), {opaque_arr_type},
                                kValueTypeName);
}

type::Value *ValueProxy::GetValue(type::Value *values, uint32_t offset) {
  return &values[offset];
}

void ValueProxy::PutValue(type::Value *values, uint32_t offset,
                          type::Value *value) {
  values[offset] = *value;
}

bool TranslateCmpResult(type::CmpBool result,
                        type::Type::TypeId typeId1,
                        type::Type::TypeId typeId2) {
  switch (result) {
    case type::CMP_TRUE:
      return true;
    case type::CMP_FALSE:
      return false;
    default: {
      throw Exception{"Error in Comparing type " +
                      TypeIdToString(typeId1) +
                      " and type " + TypeIdToString(typeId2)};
    }
  }
}

bool ValueProxy::CmpEqual(type::Value *val1, type::Value *val2){
  return TranslateCmpResult(val1->CastAs(val2->GetTypeId()).CompareEquals(*val2),
                            val1->GetTypeId(), val2->GetTypeId());
}

bool ValueProxy::CmpNotEqual(type::Value *val1, type::Value *val2){
  return TranslateCmpResult(val1->CastAs(val2->GetTypeId()).CompareNotEquals(*val2),
                            val1->GetTypeId(), val2->GetTypeId());
}

bool ValueProxy::CmpLess(type::Value *val1, type::Value *val2){
  return TranslateCmpResult(val1->CastAs(val2->GetTypeId()).CompareLessThan(*val2),
                            val1->GetTypeId(), val2->GetTypeId());
}

bool ValueProxy::CmpLessEqual(type::Value *val1, type::Value *val2){
  return TranslateCmpResult(val1->CastAs(val2->GetTypeId()).CompareLessThanEquals(*val2),
                            val1->GetTypeId(), val2->GetTypeId());
}

bool ValueProxy::CmpGreater(type::Value *val1, type::Value *val2){
  return TranslateCmpResult(val1->CastAs(val2->GetTypeId()).CompareGreaterThan(*val2),
                            val1->GetTypeId(), val2->GetTypeId());
}

bool ValueProxy::CmpGreaterEqual(type::Value *val1, type::Value *val2){
  return TranslateCmpResult(val1->CastAs(val2->GetTypeId()).CompareGreaterThanEquals(*val2),
                            val1->GetTypeId(), val2->GetTypeId());
}

//===----------------------------------------------------------------------===//
// Return the symbol of the Catalog::GetTableWithOid() function
//===----------------------------------------------------------------------===//
const std::string& ValueProxy::_GetValue::GetFunctionName() {
  static const std::string kGetTableByIdFnName =
#ifdef __APPLE__
      "_ZNK7peloton7catalog7Catalog15GetTableWithOidEjj";
#else
      "_ZN7peloton7codegen10ValueProxy8GetValueEPNS_4type5ValueEj";
#endif
  return kGetTableByIdFnName;
}

//===----------------------------------------------------------------------===//
// Return the LLVM function definition for Catalog::GetTableWithOid()
//===----------------------------------------------------------------------===//
llvm::Function* ValueProxy::_GetValue::GetFunction(CodeGen& codegen) {
  const std::string& fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function* llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  // The function hasn't been registered, let's do it now
  llvm::FunctionType* fn_type =
        llvm::FunctionType::get(
                ValueProxy::GetType(codegen)->getPointerTo(),
                {ValueProxy::GetType(codegen)->getPointerTo(), codegen.Int64Type()},
                false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// Return the symbol of the Catalog::GetTableWithOid() function
//===----------------------------------------------------------------------===//
const std::string& ValueProxy::_PutValue::GetFunctionName() {
  static const std::string kGetTableByIdFnName =
#ifdef __APPLE__
        "_ZNK7peloton7catalog7Catalog15GetTableWithOidEjj";
#else
        "_ZN7peloton7codegen10ValueProxy8PutValueEPNS_4type5ValueEjS4_";
#endif
  return kGetTableByIdFnName;
}

//===----------------------------------------------------------------------===//
// Return the LLVM function definition for Catalog::GetTableWithOid()
//===----------------------------------------------------------------------===//
llvm::Function* ValueProxy::_PutValue::GetFunction(CodeGen& codegen) {
  const std::string& fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function* llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  // The function hasn't been registered, let's do it now
  llvm::FunctionType* fn_type =
        llvm::FunctionType::get(
                codegen.VoidType(),
                {ValueProxy::GetType(codegen)->getPointerTo(), codegen.Int64Type(),
                 ValueProxy::GetType(codegen)->getPointerTo()},
                false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// Return the symbol of the Catalog::GetTableWithOid() function
//===----------------------------------------------------------------------===//
const std::string& ValueProxy::_CmpEqual::GetFunctionName() {
  static const std::string kGetTableByIdFnName =
#ifdef __APPLE__
        "_ZNK7peloton7catalog7Catalog15GetTableWithOidEjj";
#else
        "_ZN7peloton7codegen10ValueProxy8CmpEqualEPNS_4type5ValueES4_";
#endif
  return kGetTableByIdFnName;
}

//===----------------------------------------------------------------------===//
// Return the LLVM function definition for Catalog::GetTableWithOid()
//===----------------------------------------------------------------------===//
llvm::Function* ValueProxy::_CmpEqual::GetFunction(CodeGen& codegen) {
  const std::string& fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function* llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  // The function hasn't been registered, let's do it now
  llvm::FunctionType* fn_type =
        llvm::FunctionType::get(
                codegen.BoolType(),
                {ValueProxy::GetType(codegen)->getPointerTo(),
                 ValueProxy::GetType(codegen)->getPointerTo()},
                false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// Return the symbol of the Catalog::GetTableWithOid() function
//===----------------------------------------------------------------------===//
const std::string& ValueProxy::_CmpNotEqual::GetFunctionName() {
  static const std::string kGetTableByIdFnName =
#ifdef __APPLE__
        "_ZNK7peloton7catalog7Catalog15GetTableWithOidEjj";
#else
        "_ZN7peloton7codegen10ValueProxy11CmpNotEqualEPNS_4type5ValueES4_";
#endif
  return kGetTableByIdFnName;
}

//===----------------------------------------------------------------------===//
// Return the LLVM function definition for Catalog::GetTableWithOid()
//===----------------------------------------------------------------------===//
llvm::Function* ValueProxy::_CmpNotEqual::GetFunction(CodeGen& codegen) {
  const std::string& fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function* llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  // The function hasn't been registered, let's do it now
  llvm::FunctionType* fn_type =
        llvm::FunctionType::get(
                codegen.BoolType(),
                {ValueProxy::GetType(codegen)->getPointerTo(),
                 ValueProxy::GetType(codegen)->getPointerTo()},
                false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// Return the symbol of the Catalog::GetTableWithOid() function
//===----------------------------------------------------------------------===//
const std::string& ValueProxy::_CmpLess::GetFunctionName() {
  static const std::string kGetTableByIdFnName =
#ifdef __APPLE__
        "_ZNK7peloton7catalog7Catalog15GetTableWithOidEjj";
#else
        "_ZN7peloton7codegen10ValueProxy7CmpLessEPNS_4type5ValueES4_";
#endif
  return kGetTableByIdFnName;
}

//===----------------------------------------------------------------------===//
// Return the LLVM function definition for Catalog::GetTableWithOid()
//===----------------------------------------------------------------------===//
llvm::Function* ValueProxy::_CmpLess::GetFunction(CodeGen& codegen) {
  const std::string& fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function* llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  // The function hasn't been registered, let's do it now
  llvm::FunctionType* fn_type =
        llvm::FunctionType::get(
                codegen.BoolType(),
                {ValueProxy::GetType(codegen)->getPointerTo(),
                 ValueProxy::GetType(codegen)->getPointerTo()},
                false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// Return the symbol of the Catalog::GetTableWithOid() function
//===----------------------------------------------------------------------===//
const std::string& ValueProxy::_CmpLessEqual::GetFunctionName() {
  static const std::string kGetTableByIdFnName =
#ifdef __APPLE__
        "_ZNK7peloton7catalog7Catalog15GetTableWithOidEjj";
#else
        "_ZN7peloton7codegen10ValueProxy12CmpLessEqualEPNS_4type5ValueES4_";
#endif
  return kGetTableByIdFnName;
}

//===----------------------------------------------------------------------===//
// Return the LLVM function definition for Catalog::GetTableWithOid()
//===----------------------------------------------------------------------===//
llvm::Function* ValueProxy::_CmpLessEqual::GetFunction(CodeGen& codegen) {
  const std::string& fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function* llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  // The function hasn't been registered, let's do it now
  llvm::FunctionType* fn_type =
        llvm::FunctionType::get(
                codegen.BoolType(),
                {ValueProxy::GetType(codegen)->getPointerTo(),
                 ValueProxy::GetType(codegen)->getPointerTo()},
                false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// Return the symbol of the Catalog::GetTableWithOid() function
//===----------------------------------------------------------------------===//
const std::string& ValueProxy::_CmpGreater::GetFunctionName() {
  static const std::string kGetTableByIdFnName =
#ifdef __APPLE__
        "_ZNK7peloton7catalog7Catalog15GetTableWithOidEjj";
#else
        "_ZN7peloton7codegen10ValueProxy10CmpGreaterEPNS_4type5ValueES4_";
#endif
  return kGetTableByIdFnName;
}

//===----------------------------------------------------------------------===//
// Return the LLVM function definition for Catalog::GetTableWithOid()
//===----------------------------------------------------------------------===//
llvm::Function* ValueProxy::_CmpGreater::GetFunction(CodeGen& codegen) {
  const std::string& fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function* llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  // The function hasn't been registered, let's do it now
  llvm::FunctionType* fn_type =
        llvm::FunctionType::get(
                codegen.BoolType(),
                {ValueProxy::GetType(codegen)->getPointerTo(),
                 ValueProxy::GetType(codegen)->getPointerTo()},
                false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// Return the symbol of the Catalog::GetTableWithOid() function
//===----------------------------------------------------------------------===//
const std::string& ValueProxy::_CmpGreaterEqual::GetFunctionName() {
  static const std::string kGetTableByIdFnName =
#ifdef __APPLE__
        "_ZNK7peloton7catalog7Catalog15GetTableWithOidEjj";
#else
        "_ZN7peloton7codegen10ValueProxy15CmpGreaterEqualEPNS_4type5ValueES4_";
#endif
  return kGetTableByIdFnName;
}

//===----------------------------------------------------------------------===//
// Return the LLVM function definition for Catalog::GetTableWithOid()
//===----------------------------------------------------------------------===//
llvm::Function* ValueProxy::_CmpGreaterEqual::GetFunction(CodeGen& codegen) {
  const std::string& fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function* llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  // The function hasn't been registered, let's do it now
  llvm::FunctionType* fn_type =
        llvm::FunctionType::get(
                codegen.BoolType(),
                {ValueProxy::GetType(codegen)->getPointerTo(),
                 ValueProxy::GetType(codegen)->getPointerTo()},
                false);
  return codegen.RegisterFunction(fn_name, fn_type);
}
}  // namespace codegen
}  // namespace peloton
