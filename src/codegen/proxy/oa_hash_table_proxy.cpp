//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// oa_hash_table_proxy.cpp
//
// Identification: src/codegen/proxy/oa_hash_table_proxy.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/oa_hash_table_proxy.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// Return the LLVM type that matches the memory layout of our HashTable
//===----------------------------------------------------------------------===//
llvm::Type *OAHashTableProxy::GetType(CodeGen &codegen) {
  static const std::string kHashTableTypeName = "peloton::OAHashTable";
  // Check if the hash table type has been registered/cached in the module
  // already
  auto *hash_table_type = codegen.LookupTypeByName(kHashTableTypeName);
  if (hash_table_type != nullptr) {
    return hash_table_type;
  }

  // Define and register the type
  std::vector<llvm::Type *> layout = {
      // HashEntry * (the open-addressing buckets array)
      OAHashEntryProxy::GetType(codegen)->getPointerTo(),

      // The number of buckets
      codegen.Int64Type(),

      // The bucket mask (used for modulo calculation)
      codegen.Int64Type(),

      // The number of occupied buckets
      codegen.Int64Type(),

      // The number of entries in the hash table
      codegen.Int64Type(),

      // The resize threshold
      codegen.Int64Type(),

      // The entry size (in bytes)
      codegen.Int64Type(),

      // The key size (in bytes)
      codegen.Int64Type(),

      // The value size (in bytes)
      codegen.Int64Type()};
  hash_table_type = llvm::StructType::create(codegen.GetContext(), layout,
                                             kHashTableTypeName);
  return hash_table_type;
}

//===----------------------------------------------------------------------===//
// INIT PROXY
//===----------------------------------------------------------------------===//

const std::string &OAHashTableProxy::_Init::GetFunctionName() {
  static const std::string kInitFnName =
#ifdef __APPLE__
      "_ZN7peloton7codegen4util11OAHashTable4InitEyyy";
#else
      "_ZN7peloton7codegen4util11OAHashTable4InitEmmm";
#endif
  return kInitFnName;
}

llvm::Function *OAHashTableProxy::_Init::GetFunction(CodeGen &codegen) {
  const std::string fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  // The function hasn't been registered, let's do it now

  // Setup the function arguments
  std::vector<llvm::Type *> arg_types = {
      // The OAHashTable * instance
      OAHashTableProxy::GetType(codegen)->getPointerTo(),

      // The key size (in bytes)
      codegen.Int64Type(),

      // The value size (in bytes)
      codegen.Int64Type(),

      // An estimate on the number of entries the table will store
      codegen.Int64Type()};

  // Now create the prototype and register it
  auto *fn_type = llvm::FunctionType::get(codegen.VoidType(), arg_types, false);
  return codegen.RegisterFunction(fn_name, fn_type);
};

//===----------------------------------------------------------------------===//
// DESTROY PROXY
//===----------------------------------------------------------------------===//

const std::string &OAHashTableProxy::_Destroy::GetFunctionName() {
  static const std::string kStoreTupleFnName =
#ifdef __APPLE__
      "_ZN7peloton7codegen4util11OAHashTable7DestroyEv";
#else
      "_ZN7peloton7codegen4util11OAHashTable7DestroyEv";
#endif
  return kStoreTupleFnName;
}

llvm::Function *OAHashTableProxy::_Destroy::GetFunction(CodeGen &codegen) {
  const std::string fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  // The function hasn't been registered, let's do it now

  // HashTable::Destroy() only needs the hash-table instance as an argument
  llvm::Type *ht_type = OAHashTableProxy::GetType(codegen)->getPointerTo();
  auto *fn_type = llvm::FunctionType::get(codegen.VoidType(), ht_type, false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// STORE TUPLE PROXY
//===----------------------------------------------------------------------===//

const std::string &OAHashTableProxy::_StoreTuple::GetFunctionName() {
  static const std::string kStoreTupleFnName =
#ifdef __APPLE__
      "_ZN7peloton7codegen4util11OAHashTable10StoreTupleEPNS2_9HashEntryEy";
#else
      "_ZN7peloton7codegen4util11OAHashTable10StoreTupleEPNS2_9HashEntryEm";
#endif
  return kStoreTupleFnName;
}

llvm::Function *OAHashTableProxy::_StoreTuple::GetFunction(CodeGen &codegen) {
  const std::string fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  // The function hasn't been registered, let's do it now

  // Setup the function arguments
  std::vector<llvm::Type *> arg_types = {
      // The OAHashTable * instance we're invoking the function on
      OAHashTableProxy::GetType(codegen)->getPointerTo(),

      // The HashEntry * we want to store into
      OAHashEntryProxy::GetType(codegen)->getPointerTo(),

      // The hash value of the key we're storing
      codegen.Int64Type()};

  // Now create the prototype and register it
  auto *fn_type =
      llvm::FunctionType::get(codegen.CharPtrType(), arg_types, false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// HASH ENTRY
//===----------------------------------------------------------------------===//

llvm::Type *OAHashEntryProxy::GetType(CodeGen &codegen) {
  static const std::string kHashEntryTypeName = "peloton::OAHashEntry";

  // Check if the hash entry is already defined in the module
  llvm::Type *hash_entry_type = codegen.LookupTypeByName(kHashEntryTypeName);
  if (hash_entry_type != nullptr) {
    return hash_entry_type;
  }

  // This is the type of kv list. We need a pointer to this field
  llvm::Type *kv_list_type = OAHashEntryProxy::GetKeyValueListType(codegen);

  // Define the thing.
  //
  // The first field is a pointer to KeyValueList
  //   1. If the pointer is nullptr then this entry is free.
  //   2. If the pointer is 0x0000000000000001 then this entry does not
  //      have a key value list.
  // The second field is the 64-bit hash value of the entry.
  std::vector<llvm::Type *> layout = {kv_list_type->getPointerTo(),
                                      codegen.Int64Type()};

  hash_entry_type = llvm::StructType::create(codegen.GetContext(), layout,
                                             kHashEntryTypeName);

  return hash_entry_type;
}

//===----------------------------------------------------------------------===//
// KEY VALUE LIST
//===----------------------------------------------------------------------===//

llvm::Type *OAHashEntryProxy::GetKeyValueListType(CodeGen &codegen) {
  static const std::string kKVListTypeName = "peloton::OAKeyValueList";

  // Check if the type is already defined in the module
  llvm::Type *kv_list_type = codegen.LookupTypeByName(kKVListTypeName);
  if (kv_list_type != nullptr) {
    return kv_list_type;
  }

  // First field is capacity and second field is size
  std::vector<llvm::Type *> layout = {
      // The capacity of the list (i.e., the max number of entries it can store)
      codegen.Int32Type(),
      // The size of the list (i.e., the number of entries actually stored)
      codegen.Int32Type()};

  // Construct the type
  kv_list_type =
      llvm::StructType::create(codegen.GetContext(), layout, kKVListTypeName);
  return kv_list_type;
}

}  // namespace codegen
}  // namespace peloton