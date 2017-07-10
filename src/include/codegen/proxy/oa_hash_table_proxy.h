//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// oa_hash_table_proxy.h
//
// Identification: src/include/codegen/proxy/oa_hash_table_proxy.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/proxy/proxy.h"
#include "codegen/util/oa_hash_table.h"

namespace peloton {
namespace codegen {

PROXY(KeyValueList) {
  PROXY_MEMBER_FIELD(0, int32_t, capacity);
  PROXY_MEMBER_FIELD(1, int32_t, size);
  PROXY_TYPE("peloton::OAKeyValueList", int32_t, int32_t);
};

PROXY(OAHashEntry) {
  PROXY_MEMBER_FIELD(0, util::OAHashTable::KeyValueList *, kv_list);
  PROXY_MEMBER_FIELD(0, uint64_t, hash);
  PROXY_TYPE("peloton::OAHashEntry", util::OAHashTable::KeyValueList *,
             int64_t);
};

PROXY(OAHashTable) {
  PROXY_MEMBER_FIELD(0, util::OAHashTable::HashEntry *, buckets);
  PROXY_MEMBER_FIELD(1, int64_t, num_buckets);
  PROXY_MEMBER_FIELD(2, int64_t, bucket_mask);
  PROXY_MEMBER_FIELD(3, int64_t, num_occupied_buckets);
  PROXY_MEMBER_FIELD(4, int64_t, num_entries);
  PROXY_MEMBER_FIELD(5, int64_t, resize_threshold);
  PROXY_MEMBER_FIELD(6, int64_t, entry_size);
  PROXY_MEMBER_FIELD(7, int64_t, key_size);
  PROXY_MEMBER_FIELD(8, int64_t, value_size);

  PROXY_TYPE("peloton::OAHashTable", util::OAHashTable::HashEntry *, int64_t,
             int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, int64_t);

  PROXY_METHOD(Init, &peloton::codegen::util::OAHashTable::Init,
               "_ZN7peloton7codegen4util11OAHashTable4InitEmmm");
  PROXY_METHOD(
      StoreTuple, &peloton::codegen::util::OAHashTable::StoreTuple,
      "_ZN7peloton7codegen4util11OAHashTable10StoreTupleEPNS2_9HashEntryEm");
  PROXY_METHOD(Destroy, &peloton::codegen::util::OAHashTable::Destroy,
               "_ZN7peloton7codegen4util11OAHashTable7DestroyEv");
};

namespace proxy {
template <>
struct TypeBuilder<::peloton::codegen::util::OAHashTable::KeyValueList> {
  static llvm::Type *GetType(CodeGen &codegen) ALWAYS_INLINE {
    return KeyValueListProxy::GetType(codegen);
  }
};
template <>
struct TypeBuilder<::peloton::codegen::util::OAHashTable::HashEntry> {
  static llvm::Type *GetType(CodeGen &codegen) ALWAYS_INLINE {
    return OAHashEntryProxy::GetType(codegen);
  }
};
template <>
struct TypeBuilder<::peloton::codegen::util::OAHashTable> {
  static llvm::Type *GetType(CodeGen &codegen) ALWAYS_INLINE {
    return OAHashTableProxy::GetType(codegen);
  }
};
}  // namespace proxy

}  // namespace codegen
}  // namespace peloton