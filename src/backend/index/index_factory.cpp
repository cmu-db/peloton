/*-------------------------------------------------------------------------
 *
 * index_factory.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/index/index_factory.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <cassert>
#include <iostream>

#include "backend/common/types.h"
#include "backend/common/logger.h"
#include "backend/index/index_factory.h"
#include "backend/index/index_key.h"
#include "backend/index/btree_unique_index.h"
#include "backend/index/btree_multi_index.h"

namespace peloton {
namespace index {

Index *IndexFactory::GetInstance(IndexMetadata *metadata) {

  bool unique = metadata->unique_keys;
  bool ints_only = false;
  LOG_TRACE("Creating index %s.\n", metadata->GetName().c_str());
  const int keySize = metadata->key_schema->GetLength();
  auto index_type = metadata->GetIndexMethodType();

  // no int specialization beyond this point
  if (keySize > sizeof(int64_t) * 4) {
    ints_only = false;
  }

  if ((ints_only) && (index_type == INDEX_TYPE_BTREE_UNIQUE) && (unique)) {
    if (keySize <= sizeof(uint64_t)) {
      return new BtreeUniqueIndex<IntsKey<1>, IntsComparator<1>, IntsEqualityChecker<1> >(metadata);
    } else if (keySize <= sizeof(int64_t) * 2) {
      return new BtreeUniqueIndex<IntsKey<2>, IntsComparator<2>, IntsEqualityChecker<2> >(metadata);
    } else if (keySize <= sizeof(int64_t) * 3) {
      return new BtreeUniqueIndex<IntsKey<3>, IntsComparator<3>, IntsEqualityChecker<3> >(metadata);
    } else if (keySize <= sizeof(int64_t) * 4) {
      return new BtreeUniqueIndex<IntsKey<4>, IntsComparator<4>, IntsEqualityChecker<4> >(metadata);
    } else {
      throw IndexException("We currently only support tree index on unique integer keys of size 32 bytes or smaller...");
    }
  }

  if ((ints_only) && (index_type == INDEX_TYPE_BTREE_MULTI) && (!unique)) {
    if (keySize <= sizeof(uint64_t)) {
      return new BtreeMultiIndex<IntsKey<1>, IntsComparator<1>, IntsEqualityChecker<1> >(metadata);
    } else if (keySize <= sizeof(int64_t) * 2) {
      return new BtreeMultiIndex<IntsKey<2>, IntsComparator<2>, IntsEqualityChecker<2> >(metadata);
    } else if (keySize <= sizeof(int64_t) * 3) {
      return new BtreeMultiIndex<IntsKey<3>, IntsComparator<3>, IntsEqualityChecker<3> >(metadata);
    } else if (keySize <= sizeof(int64_t) * 4) {
      return new BtreeMultiIndex<IntsKey<4>, IntsComparator<4>, IntsEqualityChecker<4> >(metadata);
    } else {
      throw IndexException( "We currently only support tree index on non-unique integer keys of size 32 bytes or smaller..." );
    }
  }

  if ((index_type == INDEX_TYPE_BTREE_UNIQUE) && (unique)) {

    if (keySize <= 4) {
      return new BtreeUniqueIndex<GenericKey<4>, GenericComparator<4>, GenericEqualityChecker<4> >(metadata);
    } else if (keySize <= 8) {
      return new BtreeUniqueIndex<GenericKey<8>, GenericComparator<8>, GenericEqualityChecker<8> >(metadata);
    } else if (keySize <= 12) {
      return new BtreeUniqueIndex<GenericKey<12>, GenericComparator<12>, GenericEqualityChecker<12> >(metadata);
    } else if (keySize <= 16) {
      return new BtreeUniqueIndex<GenericKey<16>, GenericComparator<16>, GenericEqualityChecker<16> >(metadata);
    } else if (keySize <= 24) {
      return new BtreeUniqueIndex<GenericKey<24>, GenericComparator<24>, GenericEqualityChecker<24> >(metadata);
    } else if (keySize <= 32) {
      return new BtreeUniqueIndex<GenericKey<32>, GenericComparator<32>, GenericEqualityChecker<32> >(metadata);
    } else if (keySize <= 48) {
      return new BtreeUniqueIndex<GenericKey<48>, GenericComparator<48>, GenericEqualityChecker<48> >(metadata);
    } else if (keySize <= 64) {
      return new BtreeUniqueIndex<GenericKey<64>, GenericComparator<64>, GenericEqualityChecker<64> >(metadata);
    } else if (keySize <= 96) {
      return new BtreeUniqueIndex<GenericKey<96>, GenericComparator<96>, GenericEqualityChecker<96> >(metadata);
    } else if (keySize <= 128) {
      return new BtreeUniqueIndex<GenericKey<128>, GenericComparator<128>, GenericEqualityChecker<128> >(metadata);
    } else if (keySize <= 256) {
      return new BtreeUniqueIndex<GenericKey<256>, GenericComparator<256>, GenericEqualityChecker<256> >(metadata);
    } else if (keySize <= 512) {
      return new BtreeUniqueIndex<GenericKey<512>, GenericComparator<512>, GenericEqualityChecker<512> >(metadata);
    } else {
      return new BtreeUniqueIndex<TupleKey, TupleKeyComparator, TupleKeyEqualityChecker>(metadata);
    }
  }

  if ((index_type == INDEX_TYPE_BTREE_MULTI) && (!unique)) {

    if (keySize <= 4) {
      return new BtreeMultiIndex<GenericKey<4>, GenericComparator<4>, GenericEqualityChecker<4> >(metadata);
    } else if (keySize <= 8) {
      return new BtreeMultiIndex<GenericKey<8>, GenericComparator<8>, GenericEqualityChecker<8> >(metadata);
    } else if (keySize <= 12) {
      return new BtreeMultiIndex<GenericKey<12>, GenericComparator<12>, GenericEqualityChecker<12> >(metadata);
    } else if (keySize <= 16) {
      return new BtreeMultiIndex<GenericKey<16>, GenericComparator<16>, GenericEqualityChecker<16> >(metadata);
    } else if (keySize <= 24) {
      return new BtreeMultiIndex<GenericKey<24>, GenericComparator<24>, GenericEqualityChecker<24> >(metadata);
    } else if (keySize <= 32) {
      return new BtreeMultiIndex<GenericKey<32>, GenericComparator<32>, GenericEqualityChecker<32> >(metadata);
    } else if (keySize <= 48) {
      return new BtreeMultiIndex<GenericKey<48>, GenericComparator<48>, GenericEqualityChecker<48> >(metadata);
    } else if (keySize <= 64) {
      return new BtreeMultiIndex<GenericKey<64>, GenericComparator<64>, GenericEqualityChecker<64> >(metadata);
    } else if (keySize <= 96) {
      return new BtreeMultiIndex<GenericKey<96>, GenericComparator<96>, GenericEqualityChecker<96> >(metadata);
    } else if (keySize <= 128) {
      return new BtreeMultiIndex<GenericKey<128>, GenericComparator<128>, GenericEqualityChecker<128> >(metadata);
    } else if (keySize <= 256) {
      return new BtreeMultiIndex<GenericKey<256>, GenericComparator<256>, GenericEqualityChecker<256> >(metadata);
    } else if (keySize <= 512) {
      return new BtreeMultiIndex<GenericKey<512>, GenericComparator<512>, GenericEqualityChecker<512> >(metadata);
    } else {
      return new BtreeMultiIndex<TupleKey, TupleKeyComparator, TupleKeyEqualityChecker>(metadata);
    }
  }

  throw IndexException("Unsupported index scheme..." );
  return NULL;
}

}  // End index namespace
}  // End peloton namespace
