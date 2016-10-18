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

#include "common/logger.h"
#include "common/config.h"
#include "index/bwtree_index.h"
#include "index/index_key.h"
#include "storage/tuple.h"

#include "index/scan_optimizer.h"
#include "statistics/stats_aggregator.h"

namespace peloton {
namespace index {

BWTREE_TEMPLATE_ARGUMENTS
BWTREE_INDEX_TYPE::BWTreeIndex(IndexMetadata *metadata)
    :
      // Base class
      Index{metadata},
      // Key "less than" relation comparator
      comparator{},
      // Key equality checker
      equals{},
      // Key hash function
      hash_func{},
      // NOTE: These two arguments need to be constructed in advance
      // and do not have trivial constructor
      //
      // NOTE 2: We set the first parameter to false to disable automatic GC
      //
      container{false, comparator, equals, hash_func} {

  return;
}

BWTREE_TEMPLATE_ARGUMENTS
BWTREE_INDEX_TYPE::~BWTreeIndex() {}

/*
 * InsertEntry() - insert a key-value pair into the map
 *
 * If the key value pair already exists in the map, just return false
 */
BWTREE_TEMPLATE_ARGUMENTS
bool BWTREE_INDEX_TYPE::InsertEntry(const storage::Tuple *key,
                                    ItemPointer *value) {
  KeyType index_key;
  index_key.SetFromKey(key);

  bool ret = container.Insert(index_key, value);

  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementIndexInserts(metadata);
  }

  return ret;
}

/*
 * DeleteEntry() - Removes a key-value pair
 *
 * If the key-value pair does not exists yet in the map return false
 */
BWTREE_TEMPLATE_ARGUMENTS
bool BWTREE_INDEX_TYPE::DeleteEntry(const storage::Tuple *key,
                                    ItemPointer *value) {
  KeyType index_key;
  index_key.SetFromKey(key);
  size_t delete_count = 0;

  // In Delete() since we just use the value for comparison (i.e. read-only)
  // it is unnecessary for us to allocate memory
  bool ret = container.Delete(index_key, value);

  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementIndexDeletes(
        delete_count, metadata);
  }
  return ret;
}

BWTREE_TEMPLATE_ARGUMENTS
bool BWTREE_INDEX_TYPE::CondInsertEntry(
    const storage::Tuple *key, ItemPointer *value,
    std::function<bool(const void*)> predicate) {
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

  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementIndexInserts(metadata);
  }

  return ret;
}

BWTREE_TEMPLATE_ARGUMENTS
void BWTREE_INDEX_TYPE::Scan(const std::vector<common::Value> &value_list,
                             const std::vector<oid_t> &tuple_column_id_list,
                             const std::vector<ExpressionType> &expr_list,
                             const ScanDirectionType &scan_direction,
                             std::vector<ValueType> &result,
                             const ConjunctionScanPredicate *csp_p) {
  // First make sure all three components of the scan predicate are
  // of the same length
  // Since there is a 1-to-1 correspondense between these three vectors
  PL_ASSERT(tuple_column_id_list.size() == expr_list.size());
  PL_ASSERT(tuple_column_id_list.size() == value_list.size());

  // This is a hack - we do not support backward scan
  if (scan_direction == SCAN_DIRECTION_TYPE_INVALID) {
    throw Exception("Invalid scan direction \n");
  }

  LOG_TRACE("Point Query = %d; Full Scan = %d ", csp_p->IsPointQuery(),
            csp_p->IsFullIndexScan());

  if (csp_p->IsPointQuery() == true) {
    // For point query we construct the key and use equal_range

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
      // Unpack the key as a standard tuple for comparison
      auto scan_current_key = scan_itr->first;
      auto tuple =
          scan_current_key.GetTupleForComparison(metadata->GetKeySchema());

      // Compare whether the current key satisfies the predicate
      // since we just narrowed down search range using low key and
      // high key for scan, it is still possible that there are tuples
      // for which the predicate is not true
      if (Compare(tuple, tuple_column_id_list, expr_list, value_list) == true) {
        result.push_back(scan_itr->second);
      }
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

    // We use bwtree Begin() to first reach the lower bound
    // of the search key
    // Also we keep scanning until we have reached the end of the index
    // or we have seen a key higher than the high key
    for (auto scan_itr = container.Begin(index_low_key);
         (scan_itr.IsEnd() == false) &&
             (container.KeyCmpLessEqual(scan_itr->first, index_high_key));
         scan_itr++) {
      auto scan_current_key = scan_itr->first;
      auto tuple =
          scan_current_key.GetTupleForComparison(metadata->GetKeySchema());

      if (Compare(tuple, tuple_column_id_list, expr_list, value_list) == true) {
        result.push_back(scan_itr->second);
      }
    }
  }  // if is full scan

  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementIndexReads(result.size(),
                                                                  metadata);
  }
  return;
}

BWTREE_TEMPLATE_ARGUMENTS
void BWTREE_INDEX_TYPE::ScanAllKeys(std::vector<ValueType> &result) {
  auto it = container.Begin();

  // scan all entries
  while (it.IsEnd() == false) {
    result.push_back(it->second);
    it++;
  }

  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementIndexReads(result.size(),
                                                                  metadata);
  }
  return;
}

BWTREE_TEMPLATE_ARGUMENTS
void BWTREE_INDEX_TYPE::ScanKey(const storage::Tuple *key,
                                std::vector<ValueType> &result) {
  KeyType index_key;
  index_key.SetFromKey(key);

  // This function in BwTree fills a given vector
  container.GetValue(index_key, result);

  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementIndexReads(result.size(),
                                                                  metadata);
  }

  return;
}

BWTREE_TEMPLATE_ARGUMENTS
std::string BWTREE_INDEX_TYPE::GetTypeName() const { return "BWTree"; }

// NOTE: If ints key is used as an optimization just uncommend
// the following in order to instanciation the template before it is
// linked in linking stage

/*
template class BWTreeIndex<IntsKey<1>,
                           ItemPointer *,
                           IntsComparator<1>,
                           IntsEqualityChecker<1>,
                           IntsHasher<1>,
                           ItemPointerComparator,
                           ItemPointerHashFunc>;
template class BWTreeIndex<IntsKey<2>,
                           ItemPointer *,
                           IntsComparator<2>,
                           IntsEqualityChecker<2>,
                           IntsHasher<2>,
                           ItemPointerComparator,
                           ItemPointerHashFunc>;
template class BWTreeIndex<IntsKey<3>,
                           ItemPointer *,
                           IntsComparator<3>,
                           IntsEqualityChecker<3>,
                           IntsHasher<3>,
                           ItemPointerComparator,
                           ItemPointerHashFunc>;
template class BWTreeIndex<IntsKey<4>,
                           ItemPointer *,
                           IntsComparator<4>,
                           IntsEqualityChecker<4>,
                           IntsHasher<4>,
                           ItemPointerComparator,
                           ItemPointerHashFunc>;
*/

// Generic key
template class BWTreeIndex<GenericKey<4>, ItemPointer *, GenericComparator<4>,
                           GenericEqualityChecker<4>, GenericHasher<4>,
                           ItemPointerComparator, ItemPointerHashFunc>;
template class BWTreeIndex<GenericKey<8>, ItemPointer *, GenericComparator<8>,
                           GenericEqualityChecker<8>, GenericHasher<8>,
                           ItemPointerComparator, ItemPointerHashFunc>;
template class BWTreeIndex<GenericKey<16>, ItemPointer *, GenericComparator<16>,
                           GenericEqualityChecker<16>, GenericHasher<16>,
                           ItemPointerComparator, ItemPointerHashFunc>;
template class BWTreeIndex<GenericKey<64>, ItemPointer *, GenericComparator<64>,
                           GenericEqualityChecker<64>, GenericHasher<64>,
                           ItemPointerComparator, ItemPointerHashFunc>;
template class BWTreeIndex<GenericKey<256>, ItemPointer *,
                           GenericComparator<256>, GenericEqualityChecker<256>,
                           GenericHasher<256>, ItemPointerComparator,
                           ItemPointerHashFunc>;

// Tuple key
template class BWTreeIndex<TupleKey, ItemPointer *, TupleKeyComparator,
                           TupleKeyEqualityChecker, TupleKeyHasher,
                           ItemPointerComparator, ItemPointerHashFunc>;

}  // End index namespace
}  // End peloton namespace
