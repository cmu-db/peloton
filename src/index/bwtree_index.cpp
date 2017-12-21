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
#include "index/bwtree_index.h"

#include "common/logger.h"
#include "index/index_key.h"
#include "index/scan_optimizer.h"
#include "statistics/stats_aggregator.h"
#include "storage/tuple.h"
#include "settings/settings_manager.h"

namespace peloton {
namespace index {

BWTREE_TEMPLATE_ARGUMENTS
BWTREE_INDEX_TYPE::BWTreeIndex(IndexMetadata *metadata)
    :  // Base class
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

  if (static_cast<StatsType>(settings::SettingsManager::GetInt(settings::SettingId::stats_mode)) != StatsType::INVALID) {
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

  if (static_cast<StatsType>(settings::SettingsManager::GetInt(settings::SettingId::stats_mode)) != StatsType::INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementIndexDeletes(
        delete_count, metadata);
  }
  return ret;
}

BWTREE_TEMPLATE_ARGUMENTS
bool BWTREE_INDEX_TYPE::CondInsertEntry(
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

  if (static_cast<StatsType>(settings::SettingsManager::GetInt(settings::SettingId::stats_mode)) != StatsType::INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementIndexInserts(metadata);
  }

  return ret;
}

/*
 * Scan() - Scans a range inside the index using index scan optimizer
 *
 * The scan optimizer specifies whether a scan is point query, full scan
 * or interval scan. For all of these cases the corresponding functions from
 * the index is called, and all elements are returned in result vector
 */
BWTREE_TEMPLATE_ARGUMENTS
void BWTREE_INDEX_TYPE::Scan(
    UNUSED_ATTRIBUTE const std::vector<type::Value> &value_list,
    UNUSED_ATTRIBUTE const std::vector<oid_t> &tuple_column_id_list,
    UNUSED_ATTRIBUTE const std::vector<ExpressionType> &expr_list,
    ScanDirectionType scan_direction, std::vector<ValueType> &result,
    const ConjunctionScanPredicate *csp_p) {
  // This is a hack - we do not support backward scan
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
      result.push_back(scan_itr->second);
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
      result.push_back(scan_itr->second);
    }
  }  // if is full scan

  if (static_cast<StatsType>(settings::SettingsManager::GetInt(settings::SettingId::stats_mode)) != StatsType::INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementIndexReads(
        result.size(), metadata);
  }

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
BWTREE_TEMPLATE_ARGUMENTS
void BWTREE_INDEX_TYPE::ScanLimit(
    const std::vector<type::Value> &value_list,
    const std::vector<oid_t> &tuple_column_id_list,
    const std::vector<ExpressionType> &expr_list,
    ScanDirectionType scan_direction, std::vector<ValueType> &result,
    const ConjunctionScanPredicate *csp_p, uint64_t limit, uint64_t offset) {

  // Only work with limit == 1 and offset == 0
  // Because that gets translated to "min"
  // But still since we could not access tuples in the table
  // the index just fetches the first qualified key without further checking
  // including checking for non-exact bounds!!!
  if (csp_p->IsPointQuery() == false && limit == 1 && offset == 0 &&
      scan_direction == ScanDirectionType::FORWARD) {
    const storage::Tuple *low_key_p = csp_p->GetLowKey();
    const storage::Tuple *high_key_p = csp_p->GetHighKey();

    LOG_TRACE("ScanLimit() special case (limit = 1; offset = 0; ASCENDING): %s",
              low_key_p->GetInfo().c_str());

    KeyType index_low_key;
    KeyType index_high_key;
    index_low_key.SetFromKey(low_key_p);
    index_high_key.SetFromKey(high_key_p);

    auto scan_itr = container.Begin(index_low_key);
    if ((scan_itr.IsEnd() == false) &&
        (container.KeyCmpLessEqual(scan_itr->first, index_high_key))) {

      result.push_back(scan_itr->second);
    }
  } else {
    Scan(value_list, tuple_column_id_list, expr_list, scan_direction, result,
         csp_p);
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

  if (static_cast<StatsType>(settings::SettingsManager::GetInt(settings::SettingId::stats_mode)) != StatsType::INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementIndexReads(
        result.size(), metadata);
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

  if (static_cast<StatsType>(settings::SettingsManager::GetInt(settings::SettingId::stats_mode)) != StatsType::INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementIndexReads(
        result.size(), metadata);
  }

  return;
}

BWTREE_TEMPLATE_ARGUMENTS
std::string BWTREE_INDEX_TYPE::GetTypeName() const { return "BWTree"; }

// IMPORTANT: Make sure you don't exceed CompactIntegerKey_MAX_SLOTS

template class BWTreeIndex<CompactIntsKey<1>, ItemPointer *,
                           CompactIntsComparator<1>,
                           CompactIntsEqualityChecker<1>, CompactIntsHasher<1>,
                           ItemPointerComparator, ItemPointerHashFunc>;
template class BWTreeIndex<CompactIntsKey<2>, ItemPointer *,
                           CompactIntsComparator<2>,
                           CompactIntsEqualityChecker<2>, CompactIntsHasher<2>,
                           ItemPointerComparator, ItemPointerHashFunc>;
template class BWTreeIndex<CompactIntsKey<3>, ItemPointer *,
                           CompactIntsComparator<3>,
                           CompactIntsEqualityChecker<3>, CompactIntsHasher<3>,
                           ItemPointerComparator, ItemPointerHashFunc>;
template class BWTreeIndex<CompactIntsKey<4>, ItemPointer *,
                           CompactIntsComparator<4>,
                           CompactIntsEqualityChecker<4>, CompactIntsHasher<4>,
                           ItemPointerComparator, ItemPointerHashFunc>;

// Generic key
template class BWTreeIndex<GenericKey<4>, ItemPointer *,
                           FastGenericComparator<4>, GenericEqualityChecker<4>,
                           GenericHasher<4>, ItemPointerComparator,
                           ItemPointerHashFunc>;
template class BWTreeIndex<GenericKey<8>, ItemPointer *,
                           FastGenericComparator<8>, GenericEqualityChecker<8>,
                           GenericHasher<8>, ItemPointerComparator,
                           ItemPointerHashFunc>;
template class BWTreeIndex<GenericKey<16>, ItemPointer *,
                           FastGenericComparator<16>,
                           GenericEqualityChecker<16>, GenericHasher<16>,
                           ItemPointerComparator, ItemPointerHashFunc>;
template class BWTreeIndex<GenericKey<64>, ItemPointer *,
                           FastGenericComparator<64>,
                           GenericEqualityChecker<64>, GenericHasher<64>,
                           ItemPointerComparator, ItemPointerHashFunc>;
template class BWTreeIndex<GenericKey<256>, ItemPointer *,
                           FastGenericComparator<256>,
                           GenericEqualityChecker<256>, GenericHasher<256>,
                           ItemPointerComparator, ItemPointerHashFunc>;

// Tuple key
template class BWTreeIndex<TupleKey, ItemPointer *, TupleKeyComparator,
                           TupleKeyEqualityChecker, TupleKeyHasher,
                           ItemPointerComparator, ItemPointerHashFunc>;

}  // namespace index
}  // namespace peloton
