//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// pool_runtime_proxy.cpp
//
// Identification: src/codegen/pool/pool_runtime_proxy.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/pool/pool_runtime_proxy.h"

namespace peloton {
namespace codegen {

llvm::Type *PoolRuntimeProxy::GetType(CodeGen &codegen) {
  static const std::string kPoolTypeName = "peloton::type::AbstractPool";
  // Check if the data table type has already been registered in the current
  // codegen context
  auto schema_type = codegen.LookupTypeByName(kPoolTypeName);
  if (schema_type != nullptr) {
    return schema_type;
  }

  // Type isn't cached, create a new one
  auto *byte_array =
      llvm::ArrayType::get(codegen.Int8Type(), sizeof(type::AbstractPool));
  schema_type = llvm::StructType::create(codegen.GetContext(), {byte_array},
                                         kPoolTypeName);
  return schema_type;
}

const std::string &PoolRuntimeProxy::_CreatePool::GetFunctionName() {
  static const std::string kInsertRawTupleFnName =
      "_ZN7peloton7codegen11PoolRuntime10CreatePoolEv";
  return kInsertRawTupleFnName;
}

llvm::Function *PoolRuntimeProxy::_CreatePool::GetFunction(CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto *fn_type = llvm::FunctionType::get(
      PoolRuntimeProxy::GetType(codegen)->getPointerTo(), {}, false);

  return codegen.RegisterFunction(fn_name, fn_type);
}

const std::string &PoolRuntimeProxy::_DeletePool::GetFunctionName() {
  static const std::string kDeletePoolFnName =
      "_ZN7peloton7codegen11PoolRuntime10DeletePoolEPNS_4type12AbstractPoolE";
  return kDeletePoolFnName;
}

llvm::Function *PoolRuntimeProxy::_DeletePool::GetFunction(CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto *fn_type = llvm::FunctionType::get(
      codegen.VoidType(), {PoolRuntimeProxy::GetType(codegen)->getPointerTo()},
      false);

  return codegen.RegisterFunction(fn_name, fn_type);
}

}  // namespace codegen
}  // namespace peloton
