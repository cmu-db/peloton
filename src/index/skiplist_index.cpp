//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// skiplist_index.cpp
//
// Identification: src/backend/index/skiplist_index.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "index/skiplist_index.h"

#include "common/logger.h"
#include "index/index_key.h"
#include "index/scan_optimizer.h"
#include "statistics/stats_aggregator.h"
#include "storage/tuple.h"

namespace peloton {
namespace index {

SKIPLIST_TEMPLATE_ARGUMENTS
SKIPLIST_INDEX_TYPE::SkipListIndex(IndexMetadata *metadata)
    :  // Base class
      Index{metadata},
      // Key "less than" relation comparator
      comparator{},
      // Key equality checker
      equals{},
      // Key hash function
      hash_func{} {
      // NOTE: These two arguments need to be constructed in advance
      // and do not have trivial constructor
      //
      // NOTE 2: We set the first parameter to false to disable automatic GC
      //
      //container{false, comparator, equals, hash_func} {
  return;
}

SKIPLIST_TEMPLATE_ARGUMENTS
SKIPLIST_INDEX_TYPE::~SkipListIndex() {}

/*
 * InsertEntry() - insert a key-value pair into the map
 *
 * If the key value pair already exists in the map, just return false
 */
SKIPLIST_TEMPLATE_ARGUMENTS
bool SKIPLIST_INDEX_TYPE::InsertEntry(UNUSED_ATTRIBUTE const storage::Tuple *key,
                                       UNUSED_ATTRIBUTE ItemPointer *value) {
  bool ret = false;
  // TODO: Add your implementation here
  return ret;
}

/*
 * DeleteEntry() - Removes a key-value pair
 *
 * If the key-value pair does not exists yet in the map return false
 */
SKIPLIST_TEMPLATE_ARGUMENTS
bool SKIPLIST_INDEX_TYPE::DeleteEntry(UNUSED_ATTRIBUTE const storage::Tuple *key,
                                       UNUSED_ATTRIBUTE ItemPointer *value) {
  bool ret = false;
  // TODO: Add your implementation here
  return ret;
}

SKIPLIST_TEMPLATE_ARGUMENTS
bool SKIPLIST_INDEX_TYPE::CondInsertEntry(
    UNUSED_ATTRIBUTE const storage::Tuple *key,
    UNUSED_ATTRIBUTE ItemPointer *value,
    UNUSED_ATTRIBUTE std::function<bool(const void *)> predicate) {
  bool ret = false;
  // TODO: Add your implementation here
  return ret;
}

/*
 * Scan() - Scans a range inside the index using index scan optimizer
 *
 * The scan optimizer specifies whether a scan is point query, full scan
 * or interval scan. For all of these cases the corresponding functions from
 * the index is called, and all elements are returned in result vector
 */
SKIPLIST_TEMPLATE_ARGUMENTS
void SKIPLIST_INDEX_TYPE::Scan(
    UNUSED_ATTRIBUTE const std::vector<type::Value> &value_list,
    UNUSED_ATTRIBUTE const std::vector<oid_t> &tuple_column_id_list,
    UNUSED_ATTRIBUTE const std::vector<ExpressionType> &expr_list,
    UNUSED_ATTRIBUTE ScanDirectionType scan_direction,
    UNUSED_ATTRIBUTE std::vector<ValueType> &result,
    UNUSED_ATTRIBUTE const ConjunctionScanPredicate *csp_p) {
  // TODO: Add your implementation here
  return;
}

/*
 * ScanLimit() - Scan the index with predicate and limit/offset
 *
 * This function scans the index using the given index optimizer's low key and
 * high key. In addition to merely doing the scan, it checks scan direction
 * and uses either begin() or end() iterator to scan the index, and stops
 * after offset + limit elements are scanned, and limit elements are finally
 * returned
 */
SKIPLIST_TEMPLATE_ARGUMENTS
void SKIPLIST_INDEX_TYPE::ScanLimit(
    UNUSED_ATTRIBUTE const std::vector<type::Value> &value_list,
    UNUSED_ATTRIBUTE const std::vector<oid_t> &tuple_column_id_list,
    UNUSED_ATTRIBUTE const std::vector<ExpressionType> &expr_list,
    UNUSED_ATTRIBUTE ScanDirectionType scan_direction,
    UNUSED_ATTRIBUTE std::vector<ValueType> &result,
    UNUSED_ATTRIBUTE const ConjunctionScanPredicate *csp_p,
    UNUSED_ATTRIBUTE uint64_t limit,
    UNUSED_ATTRIBUTE uint64_t offset) {
  // TODO: Add your implementation here
  return;
}

SKIPLIST_TEMPLATE_ARGUMENTS
void SKIPLIST_INDEX_TYPE::ScanAllKeys(UNUSED_ATTRIBUTE std::vector<ValueType> &result) {
  // TODO: Add your implementation here
  return;
}

SKIPLIST_TEMPLATE_ARGUMENTS
void SKIPLIST_INDEX_TYPE::ScanKey(UNUSED_ATTRIBUTE const storage::Tuple *key,
                                  UNUSED_ATTRIBUTE std::vector<ValueType> &result) {
  // TODO: Add your implementation here
  return;
}

SKIPLIST_TEMPLATE_ARGUMENTS
std::string SKIPLIST_INDEX_TYPE::GetTypeName() const { return "SkipList"; }

// IMPORTANT: Make sure you don't exceed CompactIntegerKey_MAX_SLOTS

template class SkipListIndex<CompactIntsKey<1>, ItemPointer *,
                           CompactIntsComparator<1>,
                           CompactIntsEqualityChecker<1>, CompactIntsHasher<1>,
                           ItemPointerComparator, ItemPointerHashFunc>;
template class SkipListIndex<CompactIntsKey<2>, ItemPointer *,
                           CompactIntsComparator<2>,
                           CompactIntsEqualityChecker<2>, CompactIntsHasher<2>,
                           ItemPointerComparator, ItemPointerHashFunc>;
template class SkipListIndex<CompactIntsKey<3>, ItemPointer *,
                           CompactIntsComparator<3>,
                           CompactIntsEqualityChecker<3>, CompactIntsHasher<3>,
                           ItemPointerComparator, ItemPointerHashFunc>;
template class SkipListIndex<CompactIntsKey<4>, ItemPointer *,
                           CompactIntsComparator<4>,
                           CompactIntsEqualityChecker<4>, CompactIntsHasher<4>,
                           ItemPointerComparator, ItemPointerHashFunc>;

// Generic key
template class SkipListIndex<GenericKey<4>, ItemPointer *,
                           FastGenericComparator<4>, GenericEqualityChecker<4>,
                           GenericHasher<4>, ItemPointerComparator,
                           ItemPointerHashFunc>;
template class SkipListIndex<GenericKey<8>, ItemPointer *,
                           FastGenericComparator<8>, GenericEqualityChecker<8>,
                           GenericHasher<8>, ItemPointerComparator,
                           ItemPointerHashFunc>;
template class SkipListIndex<GenericKey<16>, ItemPointer *,
                           FastGenericComparator<16>,
                           GenericEqualityChecker<16>, GenericHasher<16>,
                           ItemPointerComparator, ItemPointerHashFunc>;
template class SkipListIndex<GenericKey<64>, ItemPointer *,
                           FastGenericComparator<64>,
                           GenericEqualityChecker<64>, GenericHasher<64>,
                           ItemPointerComparator, ItemPointerHashFunc>;
template class SkipListIndex<GenericKey<256>, ItemPointer *,
                           FastGenericComparator<256>,
                           GenericEqualityChecker<256>, GenericHasher<256>,
                           ItemPointerComparator, ItemPointerHashFunc>;

// Tuple key
template class SkipListIndex<TupleKey, ItemPointer *, TupleKeyComparator,
                           TupleKeyEqualityChecker, TupleKeyHasher,
                           ItemPointerComparator, ItemPointerHashFunc>;

}  // End index namespace
}  // End peloton namespace
