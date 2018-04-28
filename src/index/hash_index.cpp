//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_index.cpp
//
// Identification: src/index/hash_index.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "index/hash_index.h"
#include "common/container/cuckoo_map.h"

#include "common/logger.h"
#include "index/index_key.h"
#include "index/scan_optimizer.h"
#include "statistics/stats_aggregator.h"
#include "settings/settings_manager.h"
#include "storage/tuple.h"

namespace peloton {
namespace index {

CUCKOO_MAP_TEMPLATE_ARGUMENTS
HASH_INDEX_TYPE::HashIndex(IndexMetadata *metadata)
    :  // Base class
      Index{metadata},
      container{} {
  return;
}

CUCKOO_MAP_TEMPLATE_ARGUMENTS
HASH_INDEX_TYPE::~HashIndex() {}

/*
 * InsertEntry() - insert a key-value pair into the map
 *
 * If the key value pair already exists in the map, just return false
 */
CUCKOO_MAP_TEMPLATE_ARGUMENTS
bool HASH_INDEX_TYPE::InsertEntry(UNUSED_ATTRIBUTE const storage::Tuple *key,
                                  UNUSED_ATTRIBUTE ItemPointer *value) {
  KeyType index_key;
  index_key.SetFromKey(key);
  bool ret = container.Insert(index_key, value);
  LOG_TRACE("Insert Key : %s, res : %s", index_key.GetInfo().c_str(),
            ret ? "Success" : "Failed");
  return ret;
}

/*
 * DeleteEntry() - Removes a key-value pair
 *
 * If the key does not exists yet in the map return false
 * CuckooHash does not support delete with key-value pair match
 */
CUCKOO_MAP_TEMPLATE_ARGUMENTS
bool HASH_INDEX_TYPE::DeleteEntry(UNUSED_ATTRIBUTE const storage::Tuple *key,
                                  UNUSED_ATTRIBUTE ItemPointer *value) {
  KeyType index_key;
  index_key.SetFromKey(key);

  bool ret = container.Erase(index_key);
  return ret;
}

CUCKOO_MAP_TEMPLATE_ARGUMENTS
bool HASH_INDEX_TYPE::CondInsertEntry(
    UNUSED_ATTRIBUTE const storage::Tuple *key,
    UNUSED_ATTRIBUTE ItemPointer *value,
    UNUSED_ATTRIBUTE std::function<bool(const void *)> predicate) {
  KeyType index_key;
  index_key.SetFromKey(key);

  ValueType found_val;
  bool find_status = container.Find(index_key, found_val);
  if (find_status) {
    if (predicate(found_val)) {
      return false;
    }
  }
  container.Upsert(index_key, value);

  return true;
}

/*
 * Scan() - Scans a range inside the index using index scan optimizer
 *
 */
CUCKOO_MAP_TEMPLATE_ARGUMENTS
void HASH_INDEX_TYPE::Scan(
    UNUSED_ATTRIBUTE const std::vector<type::Value> &value_list,
    UNUSED_ATTRIBUTE const std::vector<oid_t> &tuple_column_id_list,
    UNUSED_ATTRIBUTE const std::vector<ExpressionType> &expr_list,
    UNUSED_ATTRIBUTE ScanDirectionType scan_direction,
    UNUSED_ATTRIBUTE std::vector<ValueType> &result,
    UNUSED_ATTRIBUTE const ConjunctionScanPredicate *csp_p) {
  if (scan_direction == ScanDirectionType::INVALID) {
    throw Exception("Invalid scan direction \n");
  }

  LOG_TRACE("Scan() Point Query = %d; Full Scan = %d ", csp_p->IsPointQuery(),
            csp_p->IsFullIndexScan());

  if (csp_p->IsPointQuery() == true) {
    // This is a hack: CuckooHash does not support scan, should only support PointQuery.
    LOG_INFO("Scan Index Point Query is only supported for cuckoo hash.");
    const storage::Tuple *point_query_key_p = csp_p->GetPointQueryKey();

    KeyType point_query_key;
    point_query_key.SetFromKey(point_query_key_p);

    ValueType val;
    bool ret = container.Find(point_query_key, val);
    if (ret) {
      result.push_back(val);
    }
  } else if (csp_p->IsFullIndexScan() == true) {
    throw Exception{ExceptionType::NOT_IMPLEMENTED,
                    "Scan(FullIndexScan) not supported for CuckooHash."};
  } else {
    throw Exception{ExceptionType::NOT_IMPLEMENTED,
                    "Scan(RangeIndexScan) not supported for CuckooHash."};
  }  // if is full scan

  if (static_cast<StatsType>(settings::SettingsManager::GetInt(
      settings::SettingId::stats_mode)) != StatsType::INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementIndexReads(
        result.size(), metadata);
  }

  return;
}

/*
 * ScanLimit() - Scan the index with predicate and limit/offset
 *
 */
CUCKOO_MAP_TEMPLATE_ARGUMENTS
void HASH_INDEX_TYPE::ScanLimit(
    UNUSED_ATTRIBUTE const std::vector<type::Value> &value_list,
    UNUSED_ATTRIBUTE const std::vector<oid_t> &tuple_column_id_list,
    UNUSED_ATTRIBUTE const std::vector<ExpressionType> &expr_list,
    UNUSED_ATTRIBUTE ScanDirectionType scan_direction,
    UNUSED_ATTRIBUTE std::vector<ValueType> &result,
    UNUSED_ATTRIBUTE const ConjunctionScanPredicate *csp_p,
    UNUSED_ATTRIBUTE uint64_t limit, UNUSED_ATTRIBUTE uint64_t offset) {
  throw Exception{ExceptionType::NOT_IMPLEMENTED,
                  "ScanLimit not supported for CuckooHash."};
  return;
}

CUCKOO_MAP_TEMPLATE_ARGUMENTS
void HASH_INDEX_TYPE::ScanAllKeys(
    UNUSED_ATTRIBUTE std::vector<ValueType> &result) {
  throw Exception{ExceptionType::NOT_IMPLEMENTED,
                  "ScanAllKeys not supported for CuckooHash."};
  return;
}

CUCKOO_MAP_TEMPLATE_ARGUMENTS
void HASH_INDEX_TYPE::ScanKey(UNUSED_ATTRIBUTE const storage::Tuple *key,
                              UNUSED_ATTRIBUTE std::vector<ValueType> &result) {
  KeyType index_key;
  index_key.SetFromKey(key);
  LOG_TRACE("Scan Key : %s", index_key.GetInfo().c_str());

  ValueType val;
  bool ret = container.Find(index_key, val);
  if (ret) {
    result.push_back(val);
  }
  return;
}

CUCKOO_MAP_TEMPLATE_ARGUMENTS
std::string HASH_INDEX_TYPE::GetTypeName() const { return "HashIndex"; }

// IMPORTANT: Make sure you don't exceed CompactIntegerKey_MAX_SLOTS
template class HashIndex<CompactIntsKey<1>, ItemPointer *, CompactIntsHasher<1>,
                         CompactIntsEqualityChecker<1>>;
template class HashIndex<CompactIntsKey<2>, ItemPointer *, CompactIntsHasher<2>,
                         CompactIntsEqualityChecker<2>>;
template class HashIndex<CompactIntsKey<3>, ItemPointer *, CompactIntsHasher<3>,
                         CompactIntsEqualityChecker<3>>;
template class HashIndex<CompactIntsKey<4>, ItemPointer *, CompactIntsHasher<4>,
                         CompactIntsEqualityChecker<4>>;

// Generic key
template class HashIndex<GenericKey<4>, ItemPointer *, GenericHasher<4>,
                         GenericEqualityChecker<4>>;
template class HashIndex<GenericKey<8>, ItemPointer *, GenericHasher<8>,
                         GenericEqualityChecker<8>>;
template class HashIndex<GenericKey<16>, ItemPointer *, GenericHasher<16>,
                         GenericEqualityChecker<16>>;
template class HashIndex<GenericKey<64>, ItemPointer *, GenericHasher<64>,
                         GenericEqualityChecker<64>>;
template class HashIndex<GenericKey<256>, ItemPointer *, GenericHasher<256>,
                         GenericEqualityChecker<256>>;

// Tuple key
template class HashIndex<TupleKey, ItemPointer *, TupleKeyHasher,
                         TupleKeyEqualityChecker>;

}  // namespace index
}  // namespace peloton
