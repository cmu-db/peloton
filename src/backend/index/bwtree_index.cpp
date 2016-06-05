//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bwtree_index.cpp
//
// Identification: src/backend/index/bwtree_index.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/common/logger.h"
#include "backend/index/bwtree_index.h"
#include "backend/index/index_key.h"
#include "backend/storage/tuple.h"

namespace peloton {
namespace index {

BWTREE_TEMPLATE_ARGUMENTS
BWTREE_INDEX_TYPE::BWTreeIndex(IndexMetadata *metadata) :
      // Base class
      Index{metadata},
      // Key "less than" relation comparator
      comparator{metadata},
      // Key equality checker
      equals{metadata},
      // NOTE: These two arguments need to be constructed in advance
      // and do not have trivial constructor
      container{comparator,
                equals} {
  return;
}

BWTREE_TEMPLATE_ARGUMENTS
BWTREE_INDEX_TYPE::~BWTreeIndex() {
  return;
}

/*
 * InsertEntry() - insert a key-value pair into the map
 *
 * If the key value pair already exists in the map, just return false
 */
BWTREE_TEMPLATE_ARGUMENTS
bool
BWTREE_INDEX_TYPE::InsertEntry(UNUSED_ATTRIBUTE const storage::Tuple *key,
                               UNUSED_ATTRIBUTE const ItemPointer &location) {
  bool ret = container.Insert(key, location);
  
  return ret;
}

/*
 * DeleteEntry() - Removes a key-value pair
 *
 * If the key-value pair does not exists yet in the map return false
 */
BWTREE_TEMPLATE_ARGUMENTS
bool
BWTREE_INDEX_TYPE::DeleteEntry(UNUSED_ATTRIBUTE const storage::Tuple *key,
                               UNUSED_ATTRIBUTE const ItemPointer &location) {
  bool ret = container.Delete(key, location);
  
  return ret;
}

BWTREE_TEMPLATE_ARGUMENTS
bool
BWTREE_INDEX_TYPE::CondInsertEntry(
    UNUSED_ATTRIBUTE const storage::Tuple *key,
    UNUSED_ATTRIBUTE const ItemPointer &location,
    UNUSED_ATTRIBUTE std::function<bool(const ItemPointer &)> predicate,
    UNUSED_ATTRIBUTE ItemPointer **itemptr_ptr ) {
  return false;
}

BWTREE_TEMPLATE_ARGUMENTS
void
BWTREE_INDEX_TYPE::Scan(
    UNUSED_ATTRIBUTE const std::vector<Value> &values,
    UNUSED_ATTRIBUTE const std::vector<oid_t> &key_column_ids,
    UNUSED_ATTRIBUTE const std::vector<ExpressionType> &expr_types,
    UNUSED_ATTRIBUTE const ScanDirectionType &scan_direction,
    UNUSED_ATTRIBUTE std::vector<ItemPointer> &result) {
  return;
}

BWTREE_TEMPLATE_ARGUMENTS
void
BWTREE_INDEX_TYPE::ScanAllKeys(
    UNUSED_ATTRIBUTE std::vector<ItemPointer> &result) {
  return;
}

BWTREE_TEMPLATE_ARGUMENTS
void
BWTREE_INDEX_TYPE::ScanKey(
    UNUSED_ATTRIBUTE const storage::Tuple *key,
    UNUSED_ATTRIBUTE std::vector<ItemPointer> &result) {
  return;
}

BWTREE_TEMPLATE_ARGUMENTS
void
BWTREE_INDEX_TYPE::Scan(
    UNUSED_ATTRIBUTE const std::vector<Value> &values,
    UNUSED_ATTRIBUTE const std::vector<oid_t> &key_column_ids,
    UNUSED_ATTRIBUTE const std::vector<ExpressionType> &expr_types,
    UNUSED_ATTRIBUTE const ScanDirectionType &scan_direction,
    UNUSED_ATTRIBUTE std::vector<ItemPointer *> &result) {
  return;
}

BWTREE_TEMPLATE_ARGUMENTS
void
BWTREE_INDEX_TYPE::ScanAllKeys(
    UNUSED_ATTRIBUTE std::vector<ItemPointer *> &result) {
  return;
}

BWTREE_TEMPLATE_ARGUMENTS
void
BWTREE_INDEX_TYPE::ScanKey(
    UNUSED_ATTRIBUTE const storage::Tuple *key,
    UNUSED_ATTRIBUTE std::vector<ItemPointer *> &result) {
  return;
}

BWTREE_TEMPLATE_ARGUMENTS
std::string
BWTREE_INDEX_TYPE::GetTypeName() const {
  return "BWTree";
}

// Explicit template instantiation
// Integer key
template class BWTreeIndex<IntsKey<1>,
                           ItemPointer *,
                           IntsComparator<1>,
                           IntsEqualityChecker<1>>;
template class BWTreeIndex<IntsKey<2>,
                           ItemPointer *,
                           IntsComparator<2>,
                           IntsEqualityChecker<2>>;
template class BWTreeIndex<IntsKey<3>,
                           ItemPointer *,
                           IntsComparator<3>,
                           IntsEqualityChecker<3>>;
template class BWTreeIndex<IntsKey<4>,
                           ItemPointer *,
                           IntsComparator<4>,
                           IntsEqualityChecker<4>>;

// Generic key
template class BWTreeIndex<GenericKey<4>,
                           ItemPointer *,
                           GenericComparator<4>,
                           GenericEqualityChecker<4>>;
template class BWTreeIndex<GenericKey<8>,
                           ItemPointer *,
                           GenericComparator<8>,
                           GenericEqualityChecker<8>>;
template class BWTreeIndex<GenericKey<12>,
                           ItemPointer *,
                           GenericComparator<12>,
                           GenericEqualityChecker<12>>;
template class BWTreeIndex<GenericKey<16>,
                           ItemPointer *,
                           GenericComparator<16>,
                           GenericEqualityChecker<16>>;
template class BWTreeIndex<GenericKey<24>,
                           ItemPointer *,
                           GenericComparator<24>,
                           GenericEqualityChecker<24>>;
template class BWTreeIndex<GenericKey<32>,
                           ItemPointer *,
                           GenericComparator<32>,
                           GenericEqualityChecker<32>>;
template class BWTreeIndex<GenericKey<48>,
                           ItemPointer *,
                           GenericComparator<48>,
                           GenericEqualityChecker<48>>;
template class BWTreeIndex<GenericKey<64>,
                           ItemPointer *,
                           GenericComparator<64>,
                           GenericEqualityChecker<64>>;
template class BWTreeIndex<GenericKey<96>,
                           ItemPointer *,
                           GenericComparator<96>,
                           GenericEqualityChecker<96>>;
template class BWTreeIndex<GenericKey<128>,
                           ItemPointer *,
                           GenericComparator<128>,
                           GenericEqualityChecker<128>>;
template class BWTreeIndex<GenericKey<256>,
                           ItemPointer *,
                           GenericComparator<256>,
                           GenericEqualityChecker<256>>;
template class BWTreeIndex<GenericKey<512>,
                           ItemPointer *,
                           GenericComparator<512>,
                           GenericEqualityChecker<512>>;

// Tuple key
template class BWTreeIndex<TupleKey,
                           ItemPointer *,
                           TupleKeyComparator,
                           TupleKeyEqualityChecker>;

}  // End index namespace
}  // End peloton namespace
