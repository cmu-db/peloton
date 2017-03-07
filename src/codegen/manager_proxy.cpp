//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// manager_proxy.cpp
//
// Identification: src/codegen/manager_proxy.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/manager_proxy.h"

#include "catalog/manager.h"
#include "codegen/data_table_proxy.h"
#include "storage/data_table.h"

#include <unordered_map>

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// Return the LLVM type that matches the memory layout of our Database class
//===----------------------------------------------------------------------===//
llvm::Type* ManagerProxy::GetType(CodeGen& codegen) {
  static const std::string kManagerTypeName = "peloton::catalog::Manager";
  // Check if the type is already registered in the module, if so return it
  auto* manager_type = codegen.LookupTypeByName(kManagerTypeName);
  if (manager_type != nullptr) {
    return manager_type;
  }

  // Right now we don't need to define each individual field of storage::Manager
  // since we only invoke functions on the class.
  static constexpr uint64_t manager_obj_size = sizeof(catalog::Manager);
  /*
  static constexpr uint64_t manager_obj_size = sizeof(std::atomic<oid_t>) +
      sizeof(lookup_dir) +
      sizeof(std::mutex) +
      sizeof(std::vector<storage::Database*>) +
      sizeof(std::mutex);
  static_assert(
      manager_obj_size == sizeof(catalog::Manager),
      "LLVM memory layout of catalog::Manager doesn't match precompiled"
      "version. Did you forget to update codegen/manager_proxy.h?");
  */
  auto* byte_arr_type =
      llvm::ArrayType::get(codegen.Int8Type(), manager_obj_size);
  manager_type = llvm::StructType::create(codegen.GetContext(), {byte_arr_type},
                                          kManagerTypeName);
  return manager_type;
}

//===----------------------------------------------------------------------===//
// Return the symbol of the Manager.GetTableWithOid() function
//===----------------------------------------------------------------------===//
const std::string& ManagerProxy::_GetTableWithOid::GetFunctionName() {
#ifdef __APPLE__
  static const std::string kGetTableByIdFnName =
      "_ZNK7peloton7catalog7Manager15GetTableWithOidEmy";
#else
  static const std::string kGetTableByIdFnName =
      "_ZNK7peloton7catalog7Manager15GetTableWithOidEjj";
#endif
  return kGetTableByIdFnName;
}

//===----------------------------------------------------------------------===//
// Return the LLVM function definition for Manager.GetTableWithOid()
//===----------------------------------------------------------------------===//
llvm::Function* ManagerProxy::_GetTableWithOid::GetFunction(CodeGen& codegen) {
  const std::string& fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function* llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  // The function hasn't been registered, let's do it now
  llvm::Type* manager_type = ManagerProxy::GetType(codegen);
  llvm::Type* table_type = DataTableProxy::GetType(codegen);
  std::vector<llvm::Type*> fn_args{manager_type->getPointerTo(),
                                   codegen.Int32Type(),   // database id
                                   codegen.Int32Type()};  // table id
  llvm::FunctionType* fn_type =
      llvm::FunctionType::get(table_type->getPointerTo(), fn_args, false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

}  // namespace codegen
}  // namespace peloton
