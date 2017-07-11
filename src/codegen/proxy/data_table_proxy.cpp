//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// data_table_proxy.cpp
//
// Identification: src/codegen/proxy/data_table_proxy.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/data_table_proxy.h"

#include "storage/data_table.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// Return the LLVM type that matches the memory layout of our DataTable class
//===----------------------------------------------------------------------===//
llvm::Type* DataTableProxy::GetType(CodeGen& codegen) {
  static const std::string kTableTypeName = "peloton::storage::DataTable";
  // Check if the data table type has already been registered in the current
  // codegen context
  auto table_type = codegen.LookupTypeByName(kTableTypeName);
  if (table_type != nullptr) {
    return table_type;
  }

  // Type isn't cached, create a new one
  auto* byte_array =
      llvm::ArrayType::get(codegen.Int8Type(), sizeof(storage::DataTable));
  table_type = llvm::StructType::create(codegen.GetContext(), {byte_array},
                                        kTableTypeName);
  return table_type;
}

const std::string& DataTableProxy::_GetTileGroupCount::GetFunctionName() {
  // TODO: FIX ME
  static const std::string kGetTileGroupCount =
#ifdef __APPLE__
      "_ZNK7peloton7storage9DataTable17GetTileGroupCountEv";
#else
      "_ZNK7peloton7storage9DataTable17GetTileGroupCountEv";
#endif
  return kGetTileGroupCount;
}

llvm::Function* DataTableProxy::_GetTileGroupCount::GetFunction(
    CodeGen& codegen) {
  const std::string& fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function* llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  // The function hasn't been registered, let's do it now
  llvm::Type* table_type = DataTableProxy::GetType(codegen);
  llvm::FunctionType* fn_type = llvm::FunctionType::get(
      codegen.Int64Type(), {table_type->getPointerTo()}, false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

}  // namespace codegen
}  // namespace peloton
