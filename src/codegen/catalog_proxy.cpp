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

#include "include/codegen/proxy/catalog_proxy.h"

#include "catalog/catalog.h"
#include "include/codegen/proxy/data_table_proxy.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// Return the LLVM type that matches the memory layout of our Database class
//===----------------------------------------------------------------------===//
llvm::Type* CatalogProxy::GetType(CodeGen& codegen) {
  static const std::string kCatalogTypeName = "peloton::catalog::Catalog";
  // Check if the type is already registered in the module, if so return it
  auto* catalog_type = codegen.LookupTypeByName(kCatalogTypeName);
  if (catalog_type != nullptr) {
    return catalog_type;
  }

  // Right now we don't need to define each individual field of storage::Catalog
  // since we only invoke functions on the class.
  static constexpr uint64_t catalog_obj_size = sizeof(catalog::Catalog);
  /*
  static constexpr uint64_t catalog_obj_size = sizeof(std::atomic<oid_t>) +
      sizeof(lookup_dir) +
      sizeof(std::mutex) +
      sizeof(std::vector<storage::Database*>) +
      sizeof(std::mutex);
  static_assert(
      catalog_obj_size == sizeof(catalog::Catalog),
      "LLVM memory layout of catalog::Catalog doesn't match precompiled"
      "version. Did you forget to update codegen/catalog_proxy.h?");
  */
  auto* byte_arr_type =
      llvm::ArrayType::get(codegen.Int8Type(), catalog_obj_size);
  catalog_type = llvm::StructType::create(codegen.GetContext(), {byte_arr_type},
                                          kCatalogTypeName);
  return catalog_type;
}

//===----------------------------------------------------------------------===//
// Return the symbol of the Catalog::GetTableWithOid() function
//===----------------------------------------------------------------------===//
const std::string& CatalogProxy::_GetTableWithOid::GetFunctionName() {
  static const std::string kGetTableByIdFnName =
#ifdef __APPLE__
      "_ZNK7peloton7catalog7Catalog15GetTableWithOidEjj";
#else
      "_ZNK7peloton7catalog7Catalog15GetTableWithOidEjj";
#endif
  return kGetTableByIdFnName;
}

//===----------------------------------------------------------------------===//
// Return the LLVM function definition for Catalog::GetTableWithOid()
//===----------------------------------------------------------------------===//
llvm::Function* CatalogProxy::_GetTableWithOid::GetFunction(CodeGen& codegen) {
  const std::string& fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function* llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  // The function hasn't been registered, let's do it now
  llvm::Type* catalog_type = CatalogProxy::GetType(codegen);
  llvm::Type* table_type = DataTableProxy::GetType(codegen);
  std::vector<llvm::Type*> fn_args{catalog_type->getPointerTo(),
                                   codegen.Int32Type(),   // database id
                                   codegen.Int32Type()};  // table id
  llvm::FunctionType* fn_type =
      llvm::FunctionType::get(table_type->getPointerTo(), fn_args, false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

}  // namespace codegen
}  // namespace peloton
