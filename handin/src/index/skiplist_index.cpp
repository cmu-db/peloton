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

#include <stack>

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
      container{true, metadata->HasUniqueKeys(), comparator, equals, ValueEqualityChecker{}}
    {
  // TODO: Add your implementation here
      // xingyuj1
      // whether or not support duplicate keys
      LOG_TRACE("unique_key=%d", metadata->HasUniqueKeys());
      // initialize container as a skiplist MapType here
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
  bool ret = container.Delete(index_key, value);
  return ret;
}

SKIPLIST_TEMPLATE_ARGUMENTS
bool SKIPLIST_INDEX_TYPE::CondInsertEntry(
    const storage::Tuple *key, ItemPointer *value,
    std::function<bool(const void *)> predicate) {
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
    ScanDirectionType scan_direction, std::vector<ValueType> &result,
    const ConjunctionScanPredicate *csp_p) {
  if (scan_direction == ScanDirectionType::INVALID) {
    throw Exception("Invalid scan direction \n");
  }

  LOG_TRACE("Scan() Point Query = %d; Full Scan = %d ", csp_p->IsPointQuery(),
            csp_p->IsFullIndexScan());

  if (csp_p->IsPointQuery() == true) {
    const storage::Tuple *point_query_key_p = csp_p->GetPointQueryKey();

    KeyType point_query_key;
    point_query_key.SetFromKey(point_query_key_p);

    // Note: We could call ScanKey() to achieve better modularity
    // (slightly less code), but since ScanKey() is a virtual function
    // this would induce an overhead for point query, which must be highly
    // optimized and super fast
    container.GetValue(point_query_key, result);
  } else if (csp_p->IsFullIndexScan() == true) {
    // If it is a full index scan, then just do the scan
    // until we have reached the end of the index by the same
    // we take the snapshot of the last leaf node
    for (auto scan_itr = container.Begin(); (scan_itr.IsEnd() == false);
         scan_itr++) {
      result.push_back(scan_itr.getValue());
    }  // for it from begin() to end()
  } else {
    const storage::Tuple *low_key_p = csp_p->GetLowKey();
    const storage::Tuple *high_key_p = csp_p->GetHighKey();

    LOG_TRACE("Partial scan low key: %s\n high key: %s",
              low_key_p->GetInfo().c_str(), high_key_p->GetInfo().c_str());

    // Construct low key and high key in KeyType form, rather than
    // the standard in-memory tuple
    KeyType index_low_key;
    KeyType index_high_key;
    index_low_key.SetFromKey(low_key_p);
    index_high_key.SetFromKey(high_key_p);

    // Also we keep scanning until we have reached the end of the index
    // or we have seen a key higher than the high key
    if (scan_direction == ScanDirectionType::FORWARD) {
      for (auto scan_itr = container.Begin(index_low_key);
           (scan_itr.IsEnd() == false) &&
           (container.KeyCmpLessEqual(scan_itr.getKey(), index_high_key));
           scan_itr++) {
        result.push_back(scan_itr.getValue());
      }
    } else {
      assert(scan_direction == ScanDirectionType::BACKWARD);
      std::stack<ValueType> value_stack;
      for (auto scan_itr = container.Begin(index_low_key);
           (scan_itr.IsEnd() == false) &&
           (container.KeyCmpLessEqual(scan_itr.getKey(), index_high_key));
           scan_itr++) {
        value_stack.push(scan_itr.getValue());
      }
      while(!value_stack.empty()) {
        result.push_back(value_stack.top());
        value_stack.pop();
      }
    }

  }  // if is full scan


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
    ScanDirectionType scan_direction,
    std::vector<ValueType> &result,
    const ConjunctionScanPredicate *csp_p, uint64_t limit, uint64_t offset) {

  if (csp_p->IsPointQuery() == true) {
    const storage::Tuple *point_query_key_p = csp_p->GetPointQueryKey();

    KeyType point_query_key;
    point_query_key.SetFromKey(point_query_key_p);

    // Note: We could call ScanKey() to achieve better modularity
    // (slightly less code), but since ScanKey() is a virtual function
    // this would induce an overhead for point query, which must be highly
    // optimized and super fast
    container.GetValue(point_query_key, result);
  } else if (csp_p->IsFullIndexScan() == true) {
    // If it is a full index scan, then just do the scan
    // until we have reached the end of the index by the same
    // we take the snapshot of the last leaf node
    for (auto scan_itr = container.Begin(); (scan_itr.IsEnd() == false);
         scan_itr++) {
      result.push_back(scan_itr.getValue());
    }  // for it from begin() to end()
  } else {
    const storage::Tuple *low_key_p = csp_p->GetLowKey();
    const storage::Tuple *high_key_p = csp_p->GetHighKey();

    LOG_TRACE("Partial scan low key: %s\n high key: %s, offset = %lld, limit = %lld",
              low_key_p->GetInfo().c_str(), high_key_p->GetInfo().c_str(), offset, limit);

    // Construct low key and high key in KeyType form, rather than
    // the standard in-memory tuple
    KeyType index_low_key;
    KeyType index_high_key;
    index_low_key.SetFromKey(low_key_p);
    index_high_key.SetFromKey(high_key_p);

    uint64_t current = 0;

    // Also we keep scanning until we have reached the end of the index
    // or we have seen a key higher than the high key
    if (scan_direction == ScanDirectionType::FORWARD) {
      for (auto scan_itr = container.Begin(index_low_key);
           (scan_itr.IsEnd() == false) &&
           (container.KeyCmpLessEqual(scan_itr.getKey(), index_high_key));
           scan_itr++) {
        if (current == offset) {
          if (result.size() < limit) {
            result.push_back(scan_itr.getValue());
          } else { // result.size == limit, reach upper bound
            return;
          }
        } else {
          current++;
        }
      }
    } else {
      assert(scan_direction == ScanDirectionType::BACKWARD);
      std::stack<ValueType> value_stack;
      for (auto scan_itr = container.Begin(index_low_key);
           (scan_itr.IsEnd() == false) &&
           (container.KeyCmpLessEqual(scan_itr.getKey(), index_high_key));
           scan_itr++) {
        value_stack.push(scan_itr.getValue());
      }
      while(!value_stack.empty()) {
        if (current == offset) {
          if (result.size() < limit) {
            result.push_back(value_stack.top());
            value_stack.pop();
          } else { // result.size == limit, reach upper bound
            return;
          }
        } else {
          current++;
          value_stack.pop();
        }
      }
    }

  }
  return;
}

SKIPLIST_TEMPLATE_ARGUMENTS
void SKIPLIST_INDEX_TYPE::ScanAllKeys(std::vector<ValueType> &result) {
  auto it = container.Begin();

  // scan all entries
  while (!it.IsEnd()) {
    result.push_back(it.getValue());
    it++;
  }
  return;
}

SKIPLIST_TEMPLATE_ARGUMENTS
void SKIPLIST_INDEX_TYPE::ScanKey(const storage::Tuple *key,
                                  std::vector<ValueType> &result) {
  KeyType index_key;
  index_key.SetFromKey(key);

  // This function in BwTree fills a given vector
  container.GetValue(index_key, result);
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
