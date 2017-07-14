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

/// KeyValueList
DEFINE_TYPE(KeyValueList, "peloton::OAKeyValueList", MEMBER(capacity),
            MEMBER(size));

/// OAHashEntry
DEFINE_TYPE(OAHashEntry, "peloton::OAHashEntry", MEMBER(kv_list), MEMBER(hash));

/// OAHashTable
DEFINE_TYPE(OAHashTable, "peloton::OAHashTable", MEMBER(buckets),
            MEMBER(num_buckets), MEMBER(bucket_mask),
            MEMBER(num_occupied_buckets), MEMBER(num_entries),
            MEMBER(resize_threshold), MEMBER(entry_size), MEMBER(key_size),
            MEMBER(value_size));

DEFINE_METHOD(OAHashTable, Init, &peloton::codegen::util::OAHashTable::Init,
              "_ZN7peloton7codegen4util11OAHashTable4InitEmmm");

DEFINE_METHOD(
    OAHashTable, StoreTuple, &peloton::codegen::util::OAHashTable::StoreTuple,
    "_ZN7peloton7codegen4util11OAHashTable10StoreTupleEPNS2_9HashEntryEm");

DEFINE_METHOD(OAHashTable, Destroy,
              &peloton::codegen::util::OAHashTable::Destroy,
              "_ZN7peloton7codegen4util11OAHashTable7DestroyEv");

}  // namespace codegen
}  // namespace peloton