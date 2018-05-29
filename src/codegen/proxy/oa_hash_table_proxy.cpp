//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// oa_hash_table_proxy.cpp
//
// Identification: src/codegen/proxy/oa_hash_table_proxy.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/oa_hash_table_proxy.h"

namespace peloton {
namespace codegen {

/// KeyValueList
DEFINE_TYPE(KeyValueList, "peloton::OAKeyValueList", capacity, size);

/// OAHashEntry
DEFINE_TYPE(OAHashEntry, "peloton::OAHashEntry", kv_list, hash);

/// OAHashTable
DEFINE_TYPE(OAHashTable, "peloton::OAHashTable", buckets, num_buckets,
            bucket_mask, num_occupied_buckets, num_entries, resize_threshold,
            entry_size, key_size, value_size);

DEFINE_METHOD(peloton::codegen::util, OAHashTable, Init);
DEFINE_METHOD(peloton::codegen::util, OAHashTable, StoreTuple);
DEFINE_METHOD(peloton::codegen::util, OAHashTable, Destroy);

}  // namespace codegen
}  // namespace peloton