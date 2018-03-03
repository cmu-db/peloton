//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// skiplist_index.cpp
//
// Identification: src/index/skiplist_index.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
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
    container{!metadata->HasUniqueKeys(), 50, comparator, equals} {
  // TODO: Add your implementation here
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
bool SKIPLIST_INDEX_TYPE::InsertEntry(const storage::Tuple *key,
                                      ItemPointer *value) {

  KeyType index_key;
  index_key.SetFromKey(key);

  bool ret = container.Insert(index_key, value);

  LOG_TRACE("InsertEntry(key=%s, val=%s) [%s]",
            index_key.GetInfo().c_str(),
            IndexUtil::GetInfo(value).c_str(),
            (ret ? "SUCCESS" : "FAIL"));

  return ret;
}

/*
 * DeleteEntry() - Removes a key-value pair
 *
 * If the key-value pair does not exists yet in the map return false
 */
SKIPLIST_TEMPLATE_ARGUMENTS
bool SKIPLIST_INDEX_TYPE::DeleteEntry(const storage::Tuple *key,
                                      ItemPointer *value) {

  KeyType index_key;
  index_key.SetFromKey(key);

  // In Delete() since we just use the value for comparison (i.e. read-only)
  // it is unnecessary for us to allocate memory
  bool ret = container.Delete(index_key);

  LOG_TRACE("DeleteEntry(key=%s, val=%s) [%s]",
            index_key.GetInfo().c_str(),
            IndexUtil::GetInfo(value).c_str(),
            (ret ? "SUCCESS" : "FAIL"));
  return ret;
}

SKIPLIST_TEMPLATE_ARGUMENTS
bool SKIPLIST_INDEX_TYPE::CondInsertEntry(const storage::Tuple *key,
                                          ItemPointer *value, std::function<bool(const void *)> predicate) {
  KeyType index_key;
  index_key.SetFromKey(key);

  bool predicate_satisfied = false;

  // This function will complete them in one step
  // predicate will be set to nullptr if the predicate
  // returns true for some value
  bool ret = container.ConditionalInsert(index_key, value, predicate,
                                         &predicate_satisfied);

  // If predicate is not satisfied then we know insertion successes
  if (predicate_satisfied == false) {
    // So it should always succeed?
    assert(ret == true);
  } else {
    assert(ret == false);
  }

  return ret;
}

/*
 * Scan() - Scans a range inside the index using index scan optimizer
 *
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
 */
SKIPLIST_TEMPLATE_ARGUMENTS
void SKIPLIST_INDEX_TYPE::ScanLimit(
    UNUSED_ATTRIBUTE const std::vector<type::Value> &value_list,
    UNUSED_ATTRIBUTE const std::vector<oid_t> &tuple_column_id_list,
    UNUSED_ATTRIBUTE const std::vector<ExpressionType> &expr_list,
    UNUSED_ATTRIBUTE ScanDirectionType scan_direction,
    UNUSED_ATTRIBUTE std::vector<ValueType> &result,
    UNUSED_ATTRIBUTE const ConjunctionScanPredicate *csp_p,
    UNUSED_ATTRIBUTE uint64_t limit, UNUSED_ATTRIBUTE uint64_t offset) {
  // TODO: Add your implementation here
  return;
}

SKIPLIST_TEMPLATE_ARGUMENTS
void SKIPLIST_INDEX_TYPE::ScanAllKeys(
    UNUSED_ATTRIBUTE std::vector<ValueType> &result) {
  // TODO: Add your implementation here
  return;
}

SKIPLIST_TEMPLATE_ARGUMENTS
void SKIPLIST_INDEX_TYPE::ScanKey(
    UNUSED_ATTRIBUTE const storage::Tuple *key,
    UNUSED_ATTRIBUTE std::vector<ValueType> &result) {
  // TODO: Add your implementation here
  return;
}

SKIPLIST_TEMPLATE_ARGUMENTS
std::string SKIPLIST_INDEX_TYPE::GetTypeName() const { return "SkipList"; }

// IMPORTANT: Make sure you don't exceed CompactIntegerKey_MAX_SLOTS

template class SkipListIndex<
    CompactIntsKey<1>, ItemPointer *, CompactIntsComparator<1>,
    CompactIntsEqualityChecker<1>, ItemPointerComparator>;
template class SkipListIndex<
    CompactIntsKey<2>, ItemPointer *, CompactIntsComparator<2>,
    CompactIntsEqualityChecker<2>, ItemPointerComparator>;
template class SkipListIndex<
    CompactIntsKey<3>, ItemPointer *, CompactIntsComparator<3>,
    CompactIntsEqualityChecker<3>, ItemPointerComparator>;
template class SkipListIndex<
    CompactIntsKey<4>, ItemPointer *, CompactIntsComparator<4>,
    CompactIntsEqualityChecker<4>, ItemPointerComparator>;

// Generic key
template class SkipListIndex<GenericKey<4>, ItemPointer *,
                             FastGenericComparator<4>,
                             GenericEqualityChecker<4>, ItemPointerComparator>;
template class SkipListIndex<GenericKey<8>, ItemPointer *,
                             FastGenericComparator<8>,
                             GenericEqualityChecker<8>, ItemPointerComparator>;
template class SkipListIndex<GenericKey<16>, ItemPointer *,
                             FastGenericComparator<16>,
                             GenericEqualityChecker<16>, ItemPointerComparator>;
template class SkipListIndex<GenericKey<64>, ItemPointer *,
                             FastGenericComparator<64>,
                             GenericEqualityChecker<64>, ItemPointerComparator>;
template class SkipListIndex<
    GenericKey<256>, ItemPointer *, FastGenericComparator<256>,
    GenericEqualityChecker<256>, ItemPointerComparator>;

// Tuple key
template class SkipListIndex<TupleKey, ItemPointer *, TupleKeyComparator,
                             TupleKeyEqualityChecker, ItemPointerComparator>;

}  // namespace index
}  // namespace peloton
