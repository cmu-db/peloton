//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cc_hash_table_proxy.h
//
// Identification: src/include/codegen/proxy/cc_hash_table_proxy.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"
#include "codegen/util/cc_hash_table.h"

namespace peloton {
namespace codegen {

PROXY(HashEntry) {
  PROXY_MEMBER_FIELD(0, uint64_t, hash_val);
  PROXY_MEMBER_FIELD(1, util::CCHashTable::HashEntry *, next);

  // We can't use PROXY_TYPE macro because we need to define a recursive struct
  static llvm::Type *GetType(CodeGen &codegen) {
    static const std::string kHashEntryTypeName = "peloton::CCHashEntry";

    // Check if the hash entry is already defined in the module
    auto *llvm_type = codegen.LookupTypeByName(kHashEntryTypeName);
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
  };
};

PROXY(CCHashTable) {
  PROXY_MEMBER_FIELD(0, util::CCHashTable::HashEntry **, buckets);
  PROXY_MEMBER_FIELD(1, uint64_t, hash_val);
  PROXY_MEMBER_FIELD(2, uint64_t, hash_val1);
  PROXY_MEMBER_FIELD(3, uint64_t, hash_val2);

  PROXY_TYPE("peloton::CCHashTable", util::CCHashTable::HashEntry **, uint64_t,
             uint64_t, uint64_t);

  PROXY_METHOD(Init, &peloton::codegen::util::CCHashTable::Init,
               "_ZN7peloton7codegen4util11CCHashTable4InitEv");
  PROXY_METHOD(StoreTuple, &peloton::codegen::util::CCHashTable::StoreTuple,
               "_ZN7peloton7codegen4util11CCHashTable10StoreTupleEmj");
  PROXY_METHOD(Destroy, &peloton::codegen::util::CCHashTable::Destroy,
               "_ZN7peloton7codegen4util11CCHashTable7DestroyEv");
};

namespace proxy {
template <>
struct TypeBuilder<util::CCHashTable::HashEntry> {
  static llvm::Type *GetType(CodeGen &codegen) {
    return HashEntryProxy::GetType(codegen);
  }
};

template <>
struct TypeBuilder<util::CCHashTable> {
  static llvm::Type *GetType(CodeGen &codegen) ALWAYS_INLINE {
    return CCHashTableProxy::GetType(codegen);
  }
};
}  // namespace proxy

}  // namespace codegen
}  // namespace peloton