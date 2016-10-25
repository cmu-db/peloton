//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// btree_index.cpp
//
// Identification: src/index/btree_index.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "index/btree_index.h"
#include "index/index_key.h"
#include "index/index_util.h"
#include "common/logger.h"
#include "common/config.h"
#include "storage/tuple.h"
#include "statistics/stats_aggregator.h"

namespace peloton {
namespace index {

#define BTREE_TEMPLATE_ARGUMENT                                           \
  template <typename KeyType, typename ValueType, typename KeyComparator, \
            typename KeyEqualityChecker>

#define BTREE_TEMPLATE_TYPE \
  BTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>

BTREE_TEMPLATE_ARGUMENT
BTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::BTreeIndex(
    IndexMetadata *metadata)
    : Index(metadata), container(KeyComparator()), equals(), comparator() {}

BTREE_TEMPLATE_ARGUMENT
BTREE_TEMPLATE_TYPE::~BTreeIndex() {}

/////////////////////////////////////////////////////////////////////
// Mutating operations
/////////////////////////////////////////////////////////////////////

BTREE_TEMPLATE_ARGUMENT
bool BTREE_TEMPLATE_TYPE::InsertEntry(const storage::Tuple *key,
                                      ItemPointer *value) {
  KeyType index_key;

  index_key.SetFromKey(key);

  std::pair<KeyType, ValueType> entry(index_key, value);
  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementIndexInserts(metadata);
  }

  return true;
}

BTREE_TEMPLATE_ARGUMENT
bool BTREE_TEMPLATE_TYPE::DeleteEntry(const storage::Tuple *key,
                                      ItemPointer *value) {
  KeyType index_key;
  index_key.SetFromKey(key);
  size_t delete_count = 0;

  {
    index_lock.WriteLock();

    // Delete the < key, location > pair
    bool try_again = true;
    while (try_again == true) {
      // Unset try again
      try_again = false;

      // Lookup matching entries
      auto entries = container.equal_range(index_key);
      for (auto iterator = entries.first; iterator != entries.second;
           iterator++) {
        ValueType ret_value = iterator->second;

        if (ret_value == value) {
          container.erase(iterator);
          // We could not proceed here since erase() may invalidate
          // iterators by one or more node merge
          try_again = true;
          delete_count++;

          break;
        }
      }
    }

    index_lock.Unlock();
  }

  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementIndexDeletes(
        delete_count, metadata);
  }
  return true;
}

BTREE_TEMPLATE_ARGUMENT
bool BTREE_TEMPLATE_TYPE::CondInsertEntry(const storage::Tuple *key,
                                          ItemPointer *value,
                                          std::function<bool(const void *)> predicate) {

  KeyType index_key;
  index_key.SetFromKey(key);

  {
    index_lock.WriteLock();

    // find the <key, location> pair
    auto entries = container.equal_range(index_key);
    for (auto entry = entries.first; entry != entries.second; ++entry) {
      ValueType tmp_value = entry->second;

      if (predicate(tmp_value)) {
        // this key is already visible or dirty in the index
        index_lock.Unlock();

        return false;
      }
    }

    // Insert the key, val pair
    container.insert(
        std::pair<KeyType, ValueType>(index_key, value));

    index_lock.Unlock();
  }

  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementIndexInserts(metadata);
  }

  return true;
}

/////////////////////////////////////////////////////////////////////
// Scan operations
/////////////////////////////////////////////////////////////////////

BTREE_TEMPLATE_ARGUMENT
void BTREE_TEMPLATE_TYPE::Scan(const std::vector<common::Value> &value_list,
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

  index_lock.ReadLock();

  if (csp_p->IsPointQuery() == true) {
    // For point query we construct the key and use equal_range

    const storage::Tuple *point_query_key_p = csp_p->GetPointQueryKey();

    KeyType point_query_key;
    point_query_key.SetFromKey(point_query_key_p);

    // Use equal_range to mark two ends of the scan
    auto scan_itr_pair = container.equal_range(point_query_key);

    auto scan_begin_itr = scan_itr_pair.first;
    auto scan_end_itr = scan_itr_pair.second;

    for (auto scan_itr = scan_begin_itr; scan_itr != scan_end_itr; scan_itr++) {
      auto scan_current_key = scan_itr->first;
      auto tuple =
          scan_current_key.GetTupleForComparison(metadata->GetKeySchema());

      if (Compare(tuple, tuple_column_id_list, expr_list, value_list) == true) {
        result.push_back(scan_itr->second);
      }
    }
  } else if (csp_p->IsFullIndexScan() == true) {
    // If it is a full index scan, then just do the scan

    for (auto scan_itr = container.begin(); scan_itr != container.end();
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

    // Construct low key and high key in KeyType form, rather than
    // the standard in-memory tuple
    KeyType index_low_key;
    KeyType index_high_key;
    index_low_key.SetFromKey(low_key_p);
    index_high_key.SetFromKey(high_key_p);

    // It is good that we have equal_range
    auto scan_begin_itr = container.equal_range(index_low_key).first;
    auto scan_end_itr = container.equal_range(index_high_key).second;

    for (auto scan_itr = scan_begin_itr; scan_itr != scan_end_itr; scan_itr++) {
      auto scan_current_key = scan_itr->first;
      auto tuple =
          scan_current_key.GetTupleForComparison(metadata->GetKeySchema());

      if (Compare(tuple, tuple_column_id_list, expr_list, value_list) == true) {
        result.push_back(scan_itr->second);
      }
    }
  }  // if is full scan

  index_lock.Unlock();

  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementIndexReads(result.size(),
                                                                  metadata);
  }
  return;
}

BTREE_TEMPLATE_ARGUMENT
void BTREE_TEMPLATE_TYPE::ScanAllKeys(std::vector<ValueType> &result) {
  {
    index_lock.ReadLock();

    auto itr = container.begin();

    // scan all entries
    while (itr != container.end()) {
      ItemPointer *location = itr->second;
      result.push_back(location);
      itr++;
    }

    index_lock.Unlock();
  }

  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementIndexReads(result.size(),
                                                                  metadata);
  }
}

/**
 * @brief Return all locations related to this key.
 */
BTREE_TEMPLATE_ARGUMENT
void BTREE_TEMPLATE_TYPE::ScanKey(const storage::Tuple *key,
                                  std::vector<ValueType> &result) {
  KeyType index_key;
  index_key.SetFromKey(key);

  {
    index_lock.ReadLock();

    // find the <key, location> pair
    auto entries = container.equal_range(index_key);
    for (auto entry = entries.first; entry != entries.second; ++entry) {
      result.push_back(entry->second);
    }

    index_lock.Unlock();
  }
  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementIndexReads(result.size(),
                                                                  metadata);
  }
}

///////////////////////////////////////////////////////////////////////////////////////////

BTREE_TEMPLATE_ARGUMENT
std::string BTREE_TEMPLATE_TYPE::GetTypeName() const { return "Btree"; }

// Explicit template instantiation

template class BTreeIndex<GenericKey<4>, ItemPointer *, GenericComparator<4>,
                          GenericEqualityChecker<4>>;
template class BTreeIndex<GenericKey<8>, ItemPointer *, GenericComparator<8>,
                          GenericEqualityChecker<8>>;
template class BTreeIndex<GenericKey<16>, ItemPointer *, GenericComparator<16>,
                          GenericEqualityChecker<16>>;
template class BTreeIndex<GenericKey<64>, ItemPointer *, GenericComparator<64>,
                          GenericEqualityChecker<64>>;
template class BTreeIndex<GenericKey<256>, ItemPointer *,
                          GenericComparator<256>, GenericEqualityChecker<256>>;

template class BTreeIndex<TupleKey, ItemPointer *, TupleKeyComparator,
                          TupleKeyEqualityChecker>;

}  // End index namespace
}  // End peloton namespace
