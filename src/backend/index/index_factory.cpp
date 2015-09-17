//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// index_factory.cpp
//
// Identification: src/backend/index/index_factory.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <iostream>

#include "backend/common/types.h"
#include "backend/common/logger.h"
#include "backend/index/index_factory.h"
#include "backend/index/index_key.h"

#include "backend/index/btree_index.h"

namespace peloton {
namespace index {

Index *IndexFactory::GetInstance(IndexMetadata *metadata) {
  bool ints_only = false;

  LOG_TRACE("Creating index %s", metadata->GetName().c_str());
  const auto key_size = metadata->key_schema->GetLength();

  auto index_type = metadata->GetIndexMethodType();
  LOG_TRACE("Index type : %d", index_type);

  // no int specialization beyond this point
  if (key_size > sizeof(int64_t) * 4) {
    ints_only = false;
  }

  if (ints_only && (index_type == INDEX_TYPE_BTREE)) {
    if (key_size <= sizeof(uint64_t)) {
      return new BtreeIndex<IntsKey<1>, IntsComparator<1>,
                                 IntsEqualityChecker<1>>(metadata);
    } else if (key_size <= sizeof(int64_t) * 2) {
      return new BtreeIndex<IntsKey<2>, IntsComparator<2>,
                                 IntsEqualityChecker<2>>(metadata);
    } else if (key_size <= sizeof(int64_t) * 3) {
      return new BtreeIndex<IntsKey<3>, IntsComparator<3>,
                                 IntsEqualityChecker<3>>(metadata);
    } else if (key_size <= sizeof(int64_t) * 4) {
      return new BtreeIndex<IntsKey<4>, IntsComparator<4>,
                                 IntsEqualityChecker<4>>(metadata);
    } else {
      throw IndexException(
          "We currently only support tree index on non-unique "
          "integer keys of size 32 bytes or smaller...");
    }
  }

  if (index_type == INDEX_TYPE_BTREE) {
    if (key_size <= 4) {
      return new BtreeIndex<GenericKey<4>, GenericComparator<4>,
                                 GenericEqualityChecker<4>>(metadata);
    } else if (key_size <= 8) {
      return new BtreeIndex<GenericKey<8>, GenericComparator<8>,
                                 GenericEqualityChecker<8>>(metadata);
    } else if (key_size <= 12) {
      return new BtreeIndex<GenericKey<12>, GenericComparator<12>,
                                 GenericEqualityChecker<12>>(metadata);
    } else if (key_size <= 16) {
      return new BtreeIndex<GenericKey<16>, GenericComparator<16>,
                                 GenericEqualityChecker<16>>(metadata);
    } else if (key_size <= 24) {
      return new BtreeIndex<GenericKey<24>, GenericComparator<24>,
                                 GenericEqualityChecker<24>>(metadata);
    } else if (key_size <= 32) {
      return new BtreeIndex<GenericKey<32>, GenericComparator<32>,
                                 GenericEqualityChecker<32>>(metadata);
    } else if (key_size <= 48) {
      return new BtreeIndex<GenericKey<48>, GenericComparator<48>,
                                 GenericEqualityChecker<48>>(metadata);
    } else if (key_size <= 64) {
      return new BtreeIndex<GenericKey<64>, GenericComparator<64>,
                                 GenericEqualityChecker<64>>(metadata);
    } else if (key_size <= 96) {
      return new BtreeIndex<GenericKey<96>, GenericComparator<96>,
                                 GenericEqualityChecker<96>>(metadata);
    } else if (key_size <= 128) {
      return new BtreeIndex<GenericKey<128>, GenericComparator<128>,
                                 GenericEqualityChecker<128>>(metadata);
    } else if (key_size <= 256) {
      return new BtreeIndex<GenericKey<256>, GenericComparator<256>,
                                 GenericEqualityChecker<256>>(metadata);
    } else if (key_size <= 512) {
      return new BtreeIndex<GenericKey<512>, GenericComparator<512>,
                                 GenericEqualityChecker<512>>(metadata);
    } else {
      return new BtreeIndex<TupleKey, TupleKeyComparator,
                                 TupleKeyEqualityChecker>(metadata);
    }
  }

  throw IndexException("Unsupported index scheme.");
  return NULL;
}

}  // End index namespace
}  // End peloton namespace
