//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cc_hash_table_proxy.cpp
//
// Identification: src/codegen/proxy/cc_hash_table_proxy.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/cc_hash_table_proxy.h"

namespace peloton {
namespace codegen {

// We need to manually define the type because it is recursive
llvm::Type *HashEntryProxy::GetType(CodeGen &codegen) {
  static const std::string kHashEntryTypeName = "peloton::CCHashEntry";

  // Check if the hash entry is already defined in the module
  auto *llvm_type = codegen.LookupType(kHashEntryTypeName);
  if (llvm_type != nullptr) {
    return llvm_type;
  }

  // Define the thing (the first field is the 64bit hash, the second is the
  // next HashEntry* pointer)
  auto *hash_entry_type =
      llvm::StructType::create(codegen.GetContext(), kHashEntryTypeName);
  std::vector<llvm::Type *> elements = {
      codegen.Int64Type(),             // The hash value
      hash_entry_type->getPointerTo()  // The next HashEntry* pointer
  };
  hash_entry_type->setBody(elements, /*is_packed*/ false);
  return hash_entry_type;
}

DEFINE_TYPE(CCHashTable, "peloton::CCHashTable", MEMBER(buckets),
            MEMBER(num_buckets), MEMBER(bucket_mask), MEMBER(num_elements));

DEFINE_METHOD(CCHashTable, Init, &util::CCHashTable::Init,
              "_ZN7peloton7codegen4util11CCHashTable4InitEv");

DEFINE_METHOD(CCHashTable, StoreTuple, &util::CCHashTable::StoreTuple,
              "_ZN7peloton7codegen4util11CCHashTable10StoreTupleEmj");

DEFINE_METHOD(CCHashTable, Destroy, &util::CCHashTable::Destroy,
              "_ZN7peloton7codegen4util11CCHashTable7DestroyEv");

}  // namespace codegen
}  // namespace peloton