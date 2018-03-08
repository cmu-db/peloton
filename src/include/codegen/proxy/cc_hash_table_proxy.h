//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cc_hash_table_proxy.h
//
// Identification: src/include/codegen/proxy/cc_hash_table_proxy.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"
#include "codegen/util/cc_hash_table.h"

namespace peloton {
namespace codegen {

/// The proxy for CCHashTable::HashEntry
PROXY(HashEntry) {
  DECLARE_MEMBER(0, uint64_t, hash_val);
  DECLARE_MEMBER(1, util::CCHashTable::HashEntry *, next);
  DECLARE_TYPE;
};

/// The proxy for CCHashTable
PROXY(CCHashTable) {
  DECLARE_MEMBER(0, util::CCHashTable::HashEntry **, buckets);
  DECLARE_MEMBER(1, uint64_t, num_buckets);
  DECLARE_MEMBER(2, uint64_t, bucket_mask);
  DECLARE_MEMBER(3, uint64_t, num_elements);
  DECLARE_TYPE;

  // Proxy Init(), StoreTuple(), and Destroy() in util::CCHashTable
  DECLARE_METHOD(Init);
  DECLARE_METHOD(StoreTuple);
  DECLARE_METHOD(Destroy);
};

/// The type builders for HashEntry and CCHashTable
TYPE_BUILDER(HashEntry, util::CCHashTable::HashEntry);
TYPE_BUILDER(CCHashTable, util::CCHashTable);

}  // namespace codegen
}  // namespace peloton