//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// value_proxy.cpp
//
// Identification: src/codegen/value_proxy.cpp
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

type::Value* ValueProxy::GetValue(type::Value* values, uint32_t offset) {
  return &values[offset];
}

//===----------------------------------------------------------------------===//
// Return the symbol of the Catalog::GetTableWithOid() function
//===----------------------------------------------------------------------===//
const std::string& ValueProxy::_GetValue::GetFunctionName() {
  static const std::string kGetValueFnName =
#ifdef __APPLE__
      "_ZNK7peloton7catalog7Catalog15GetTableWithOidEjj";
#else
      "_ZN7peloton7codegen10ValueProxy8GetValueEPNS_4type5ValueEj";
#endif
  return kGetValueFnName;
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
  llvm::FunctionType* fn_type = llvm::FunctionType::get(
      ValueProxy::GetType(codegen)->getPointerTo(),
      {ValueProxy::GetType(codegen)->getPointerTo(), codegen.Int64Type()},
      false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

}  // namespace codegen
}  // namespace peloton
