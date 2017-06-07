//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cc_hash_table_proxy.cpp
//
// Identification: src/codegen/cc_hash_table_proxy.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "include/codegen/proxy/cc_hash_table_proxy.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// Return the LLVM type that matches the memory layout of our HashTable
//===----------------------------------------------------------------------===//
llvm::Type *CCHashTableProxy::GetType(CodeGen &codegen) {
  static const std::string kHashTableTypeName = "peloton::CCHashTable";
  // Check if the hash table type has been registered/cached in the module
  // already
  auto *hash_table_type = codegen.LookupTypeByName(kHashTableTypeName);
  if (hash_table_type != nullptr) {
    return hash_table_type;
  }

  // Define and register the type
  std::vector<llvm::Type *> layout{
      HashEntryProxy::GetType(codegen)->getPointerTo()->getPointerTo(),
      codegen.Int64Type(), codegen.Int64Type(), codegen.Int64Type()};
  hash_table_type = llvm::StructType::create(codegen.GetContext(), layout,
                                             kHashTableTypeName);
  return hash_table_type;
}

//===----------------------------------------------------------------------===//
// INIT PROXY
//===----------------------------------------------------------------------===//

const std::string &CCHashTableProxy::_Init::GetFunctionName() {
  static const std::string kInitFnName =
#ifdef __APPLE__
      "_ZN7peloton7codegen4util11CCHashTable4InitEv";
#else
      "_ZN7peloton7codegen4util11CCHashTable4InitEv";
#endif
  return kInitFnName;
}

llvm::Function *CCHashTableProxy::_Init::GetFunction(CodeGen &codegen) {
  const std::string fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  // The function hasn't been registered, let's do it now
  llvm::Type *ht_type = CCHashTableProxy::GetType(codegen)->getPointerTo();
  llvm::FunctionType *fn_type =
      llvm::FunctionType::get(codegen.VoidType(), ht_type);
  return codegen.RegisterFunction(fn_name, fn_type);
};

//===----------------------------------------------------------------------===//
// DESTROY PROXY
//===----------------------------------------------------------------------===//

const std::string &CCHashTableProxy::_Destroy::GetFunctionName() {
  static const std::string kStoreTupleFnName =
#ifdef __APPLE__
      "_ZN7peloton7codegen4util11CCHashTable7DestroyEv";
#else
      "_ZN7peloton7codegen4util11CCHashTable7DestroyEv";
#endif
  return kStoreTupleFnName;
}

llvm::Function *CCHashTableProxy::_Destroy::GetFunction(CodeGen &codegen) {
  const std::string fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  // The function hasn't been registered, let's do it now
  llvm::Type *ht_type = CCHashTableProxy::GetType(codegen)->getPointerTo();
  llvm::FunctionType *fn_type =
      llvm::FunctionType::get(codegen.VoidType(), ht_type);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// STORE TUPLE PROXY
//===----------------------------------------------------------------------===//

const std::string &CCHashTableProxy::_StoreTuple::GetFunctionName() {
  static const std::string kStoreTupleFnName =
#ifdef __APPLE__
      "_ZN7peloton7codegen4util11CCHashTable10StoreTupleEmj";
#else
      "_ZN7peloton7codegen4util11CCHashTable10StoreTupleEmj";
#endif
  return kStoreTupleFnName;
}

llvm::Function *CCHashTableProxy::_StoreTuple::GetFunction(CodeGen &codegen) {
  const std::string fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  // The function hasn't been registered, let's do it now
  llvm::Type *ht_type = CCHashTableProxy::GetType(codegen)->getPointerTo();
  std::vector<llvm::Type *> parameter_types{ht_type, codegen.Int64Type(),
                                            codegen.Int32Type()};
  llvm::FunctionType *fn_type =
      llvm::FunctionType::get(codegen.CharPtrType(), parameter_types, false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===----------------------------------------------------------------------===//
// HASH ENTRY
//===----------------------------------------------------------------------===//

llvm::Type *HashEntryProxy::GetType(CodeGen &codegen) {
  static const std::string kHashEntryTypeName = "peloton::CCHashEntry";

  // Check if the hash entry is already defined in the module
  auto *llvm_type = codegen.LookupTypeByName(kHashEntryTypeName);
  if (llvm_type != nullptr) {
    return llvm_type;
  }

  // Define the thing (the first field is the 64-bit hash, the second is the
  // next HashEntry* pointer)
  auto *hash_entry_type =
      llvm::StructType::create(codegen.GetContext(), kHashEntryTypeName);
  hash_entry_type->setBody(
      {codegen.Int64Type(), hash_entry_type->getPointerTo()},
      /*is_packed*/ false);
  return hash_entry_type;
}

}  // namespace codegen
}  // namespace peloton