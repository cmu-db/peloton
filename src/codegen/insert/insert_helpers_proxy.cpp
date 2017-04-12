//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_group_by_translator.cpp
//
// Identification: src/codegen/insert/insert_helpers_proxy.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/transaction_proxy.h"
#include "codegen/data_table_proxy.h"
#include "codegen/insert/insert_helpers_proxy.h"
#include "codegen/value_proxy.h"

namespace peloton {
namespace codegen {

const std::string &InsertHelpersProxy::_InsertValue::GetFunctionName() {
  static const std::string kInsertValueFnName =
      "_ZN7peloton7codegen13InsertHelpers11InsertValueEPNS_11concurrency11TransactionEPNS_7storage9DataTableEPNS_4type5ValueE";
  return kInsertValueFnName;
}

llvm::Function *InsertHelpersProxy::_InsertValue::GetFunction(
    CodeGen &codegen) {

  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto *fn_type = llvm::FunctionType::get(
      codegen.VoidType(),
      {
          TransactionProxy::GetType(codegen)->getPointerTo(),
          DataTableProxy::GetType(codegen)->getPointerTo(),
          ValueProxy::GetType(codegen)->getPointerTo()
      },
      false);

  return codegen.RegisterFunction(fn_name, fn_type);

}

}  // namespace codegen
}  // namespace peloton
