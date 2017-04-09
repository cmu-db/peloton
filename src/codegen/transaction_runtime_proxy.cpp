//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_runtime_proxy.cpp
//
// Identification: src/codegen/transaction_runtime_proxy.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/transaction_runtime_proxy.h"

#include "codegen/tile_group_proxy.h"
#include "codegen/transaction_proxy.h"

namespace peloton {
namespace codegen {

const std::string &
TransactionRuntimeProxy::_PerformVectorizedRead::GetFunctionName() {
  static const std::string kPerformVectorizedReadFnName =
      "__ZN7peloton7codegen18TransactionRuntime21PerformVectorizedReadERNS_11concurrency11TransactionERNS_7storage9TileGroupEjjPj";
  return kPerformVectorizedReadFnName;
}

llvm::Function *TransactionRuntimeProxy::_PerformVectorizedRead::GetFunction(
    CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  llvm::Type *ret_type = codegen.Int32Type();
  std::vector<llvm::Type *> arg_types = {
      TransactionProxy::GetType(codegen)->getPointerTo(),  // txn *
      TileGroupProxy::GetType(codegen)->getPointerTo(),    // tile_group *
      codegen.Int32Type(),                                 // tid_start
      codegen.Int32Type(),                                 // tid_end
      codegen.Int32Type()->getPointerTo()};                // selection_vector
  auto *fn_type = llvm::FunctionType::get(ret_type, arg_types, false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

}  // namespace codegen
}  // namespace peloton