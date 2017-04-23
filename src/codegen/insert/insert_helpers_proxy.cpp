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

#include "codegen/schema/schema_proxy.h"
#include "codegen/transaction_proxy.h"
#include "codegen/data_table_proxy.h"
#include "codegen/insert/insert_helpers_proxy.h"
#include "codegen/value_proxy.h"

namespace peloton {
namespace codegen {

const std::string &InsertHelpersProxy::_InsertRawTuple::GetFunctionName() {
  static const std::string kInsertRawTupleFnName =
      "_ZN7peloton7codegen13InsertHelpers14InsertRawTupleEPNS_11concurrency11TransactionEPNS_7storage9DataTableEPKNS5_5TupleE";
  return kInsertRawTupleFnName;
}

llvm::Function *InsertHelpersProxy::_InsertRawTuple::GetFunction(
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
          TransactionProxy::GetType(codegen)->getPointerTo(), // txn
          DataTableProxy::GetType(codegen)->getPointerTo(),   // table
          codegen.Int8Type()->getPointerTo(),                 // tuple
      },
      false);

  return codegen.RegisterFunction(fn_name, fn_type);
}

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

const std::string &InsertHelpersProxy::_CreateTuple::GetFunctionName() {
  static const std::string kInsertValueFnName =
      "_ZN7peloton7codegen13InsertHelpers11CreateTupleEPNS_7catalog6SchemaE";
  return kInsertValueFnName;
}

llvm::Function *InsertHelpersProxy::_CreateTuple::GetFunction(
    CodeGen &codegen) {

  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto *fn_type = llvm::FunctionType::get(
      codegen.Int8Type()->getPointerTo(),
      {
          SchemaProxy::GetType(codegen)->getPointerTo()
      },
      false);

  return codegen.RegisterFunction(fn_name, fn_type);
}

const std::string &InsertHelpersProxy::_GetTupleData::GetFunctionName() {
  static const std::string kInsertValueFnName =
      "_ZN7peloton7codegen13InsertHelpers12GetTupleDataEPNS_7storage5TupleE";
  return kInsertValueFnName;
}

llvm::Function *InsertHelpersProxy::_GetTupleData::GetFunction(
    CodeGen &codegen) {

  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto *fn_type = llvm::FunctionType::get(
      codegen.Int8Type()->getPointerTo(),
      {
          codegen.Int8Type()->getPointerTo()
      },
      false);

  return codegen.RegisterFunction(fn_name, fn_type);
}

const std::string &InsertHelpersProxy::_DeleteTuple::GetFunctionName() {
  static const std::string kInsertValueFnName =
      "_ZN7peloton7codegen13InsertHelpers11DeleteTupleEPNS_7storage5TupleE";
  return kInsertValueFnName;
}

llvm::Function *InsertHelpersProxy::_DeleteTuple::GetFunction(
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
          codegen.Int8Type()->getPointerTo()
      },
      false);

  return codegen.RegisterFunction(fn_name, fn_type);
}

}  // namespace codegen
}  // namespace peloton
