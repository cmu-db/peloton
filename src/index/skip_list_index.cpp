//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// skip_list_index.cpp
//
// Identification: src/index/skip_list_index.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "index/skip_list_index.h"
#include "index/index_key.h"
#include "index/index_util.h"
#include "common/logger.h"
#include "storage/tuple.h"

namespace peloton {
namespace index {

template <typename KeyType, typename ValueType, class KeyComparator,
class KeyEqualityChecker>
SkipListIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::SkipListIndex(
    IndexMetadata *metadata)
    : Index(metadata),
      container(),
      equals(),
      comparator(){}

template <typename KeyType, typename ValueType, class KeyComparator,
class KeyEqualityChecker>
SkipListIndex<KeyType, ValueType, KeyComparator,
KeyEqualityChecker>::~SkipListIndex() {

  // we should not rely on shared_ptr to reclaim memory.
  // this is because the underlying index can split or merge leaf nodes,
  // which invokes data data copy and deletes.
  // as the underlying index is unaware of shared_ptr,
  // memory allocated should be managed carefully by programmers.
  auto iterator = container.begin();
  for (; iterator != container.end(); ++iterator) {
    delete iterator->second;
    iterator->second = nullptr;
  }

}

template <typename KeyType, typename ValueType, class KeyComparator,
class KeyEqualityChecker>
bool SkipListIndex<KeyType, ValueType, KeyComparator,
KeyEqualityChecker>::InsertEntry(const storage::Tuple *key,
                                 ItemPointer *location) {
  KeyType index_key;
  index_key.SetFromKey(key);

  // Insert the key, val pair
  auto status = container.Insert(index_key, location);

  return status;
}

template <typename KeyType, typename ValueType, class KeyComparator,
class KeyEqualityChecker>
bool SkipListIndex<KeyType, ValueType, KeyComparator,
KeyEqualityChecker>::DeleteEntry(const storage::Tuple *,
                                 const ItemPointer&) {
  // Dummy erase
  return true;
}

template <typename KeyType, typename ValueType, class KeyComparator,
class KeyEqualityChecker>
bool SkipListIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
CondInsertEntry(const storage::Tuple *key, ItemPointer *location,
                std::function<bool(const ItemPointer &)>) {
  KeyType index_key;
  index_key.SetFromKey(key);

  // Insert the key if it does not exist
  const bool bInsert = false;
  auto status = container.Update(index_key, location, bInsert);

  return status;
}

template <typename KeyType, typename ValueType, class KeyComparator,
class KeyEqualityChecker>
void SkipListIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Scan(
    const std::vector<Value> &values, const std::vector<oid_t> &key_column_ids,
    const std::vector<ExpressionType> &expr_types,
    const ScanDirectionType &scan_direction,
    std::vector<ItemPointer *> &result,
    const ConjunctionScanPredicate *csp_p) {
  // Check if we have leading (leftmost) column equality
  // refer : http://www.postgresql.org/docs/8.2/static/indexes-multicolumn.html
  //  oid_t leading_column_id = 0;
  //  auto key_column_ids_itr = std::find(key_column_ids.begin(),
  //                                      key_column_ids.end(),
  //                                      leading_column_id);

  // PARTIAL SCAN CASE : leading column id is one of the key column ids
  // and is involved in a equality constraint
  // Currently special case only includes EXPRESSION_TYPE_COMPARE_EQUAL
  // There are two more types, one is aligned, another is not aligned.
  // Aligned example: A > 0, B >= 15, c > 4
  // Not Aligned example: A >= 15, B < 30
  
  (void)csp_p;

  // Check if suitable for point query
  bool point_query = true;
  for (auto key_column_ids_itr = key_column_ids.begin();
      key_column_ids_itr != key_column_ids.end(); key_column_ids_itr++) {
    auto offset = std::distance(key_column_ids.begin(), key_column_ids_itr);

    if (expr_types[offset] != EXPRESSION_TYPE_COMPARE_EQUAL){
      point_query = false;
    }
  }

  // Check if suitable for partial scan
  bool partial_scan_candidate = true;
  if(point_query == false) {

    for (auto key_column_ids_itr = key_column_ids.begin();
        key_column_ids_itr != key_column_ids.end();
        key_column_ids_itr++) {
      auto offset = std::distance(key_column_ids.begin(), key_column_ids_itr);

      if (expr_types[offset] == EXPRESSION_TYPE_COMPARE_NOTEQUAL ||
          expr_types[offset] == EXPRESSION_TYPE_COMPARE_IN ||
          expr_types[offset] == EXPRESSION_TYPE_COMPARE_LIKE ||
          expr_types[offset] == EXPRESSION_TYPE_COMPARE_NOTLIKE) {
        partial_scan_candidate = false;
      }
    }

  }

  LOG_TRACE("Point query : %d ", point_query);
  LOG_TRACE("Partial scan : %d ", partial_scan_candidate);

  // POINT QUERY
  if (point_query == true) {

    std::unique_ptr<storage::Tuple> find_key;

    find_key.reset(new storage::Tuple(metadata->GetKeySchema(), true));

    LOG_TRACE("%s", "Constructing key");

    for (auto key_column_ids_itr = key_column_ids.begin();
        key_column_ids_itr != key_column_ids.end();
        key_column_ids_itr++) {
      auto column_offset = std::distance(key_column_ids.begin(), key_column_ids_itr);
      find_key->SetValue(column_offset, values[column_offset], GetPool());
    }

    LOG_TRACE("Find key : %s", find_key->GetInfo().c_str());

    KeyType find_index_key;
    find_index_key.SetFromKey(find_key.get());

    // Check if key exists in index
    auto find_itr = container.Contains(find_index_key);

    if(find_itr != container.end()){
      ItemPointer *location_header = find_itr->second;
      result.push_back(location_header);
    }

  }
  // PARTIAL SCAN
  else if (partial_scan_candidate == true) {
    PL_ASSERT(key_column_ids.size() > 0);
    LOG_TRACE("key_column_ids size : %lu ", key_column_ids.size());

    oid_t leading_column_offset = 0;
    oid_t leading_column_id = key_column_ids[leading_column_offset];
    std::vector<std::pair<Value, Value>> intervals;

    ConstructIntervals(leading_column_id,
                       values,
                       key_column_ids,
                       expr_types,
                       intervals);

    // For non-leading columns, find the max and min
    std::map<oid_t, std::pair<Value, Value>> non_leading_columns;

    FindMaxMinInColumns(leading_column_id,
                        values,
                        key_column_ids,
                        expr_types,
                        non_leading_columns);

    auto indexed_columns = metadata->GetKeySchema()->GetIndexedColumns();
    oid_t non_leading_column_offset = 0;

    for (auto key_column_id : indexed_columns) {
      if (key_column_id == leading_column_id) {
        LOG_TRACE("Leading column : %u", key_column_id);
        continue;
      }

      if (non_leading_columns.find(key_column_id) == non_leading_columns.end()) {
        auto key_schema = metadata->GetKeySchema();
        auto type = key_schema->GetColumn(non_leading_column_offset).column_type;
        std::pair<Value, Value> range(Value::GetMinValue(type),
                                      Value::GetMaxValue(type));
        std::pair<oid_t, std::pair<Value, Value>> key_value(non_leading_column_offset,
                                                            range);

        non_leading_columns.insert(key_value);
      }

      non_leading_column_offset++;
    }

    LOG_TRACE("Non leading columns size : %lu", non_leading_columns.size());
    PL_ASSERT(intervals.empty() != true);

    // Search each interval of leading_column.
    for (const auto &interval : intervals) {
      auto scan_begin_itr = container.begin();
      auto scan_end_itr = container.end();

      std::unique_ptr<storage::Tuple> start_key;
      std::unique_ptr<storage::Tuple> end_key;

      start_key.reset(new storage::Tuple(metadata->GetKeySchema(), true));
      end_key.reset(new storage::Tuple(metadata->GetKeySchema(), true));

      LOG_TRACE("%s", "Constructing start/end keys");

      LOG_TRACE("Leading Column: column id : %u left bound %s\t\t right bound %s",
                leading_column_id,
                interval.first.GetInfo().c_str(),
                interval.second.GetInfo().c_str());

      start_key->SetValue(leading_column_offset, interval.first, GetPool());
      end_key->SetValue(leading_column_offset, interval.second, GetPool());

      for (const auto &k_v : non_leading_columns) {
        start_key->SetValue(k_v.first, k_v.second.first, GetPool());
        end_key->SetValue(k_v.first, k_v.second.second, GetPool());
        LOG_TRACE("Non Leading Column: column id : %u left bound %s\t\t right bound %s",
                  k_v.first,
                  k_v.second.first.GetInfo().c_str(),
                  k_v.second.second.GetInfo().c_str());
      }

      KeyType start_index_key;
      KeyType end_index_key;
      start_index_key.SetFromKey(start_key.get());
      end_index_key.SetFromKey(end_key.get());

      LOG_TRACE("Start key : %s", start_key->GetInfo().c_str());
      LOG_TRACE("End key : %s", end_key->GetInfo().c_str());

      scan_begin_itr = container.Contains(start_index_key);
      scan_end_itr = container.Contains(end_index_key);

      if(scan_begin_itr == container.end()){
        LOG_TRACE("Did not find start key -- so setting to lower bound");
        scan_begin_itr = container.begin();
      }
      else {
        LOG_TRACE("Found start key");
      }

      if(scan_end_itr == container.end()){
        LOG_TRACE("Did not find end key");
      }
      else {
        LOG_TRACE("Found end key");
      }

      switch (scan_direction) {
        case SCAN_DIRECTION_TYPE_FORWARD:
        case SCAN_DIRECTION_TYPE_BACKWARD: {

          // Scan the index entries in forward direction
          for (auto scan_itr = scan_begin_itr;
              scan_itr != scan_end_itr;
              ++scan_itr) {
            auto scan_current_key = scan_itr->first;
            auto tuple = scan_current_key.GetTupleForComparison(
                metadata->GetKeySchema());

            // Compare the current key in the scan with "values" based on
            // "expression types"
            // For instance, "5" EXPR_GREATER_THAN "2" is true
            if (Compare(tuple, key_column_ids, expr_types, values) == true) {
              ItemPointer *location_header = scan_itr->second;
              result.push_back(location_header);
            }

          }

        }
        break;


        case SCAN_DIRECTION_TYPE_INVALID:
        default:
          throw Exception("Invalid scan direction ");
          break;
      }
    }

  }
  // FULL SCAN
  else {
    auto scan_begin_itr = container.begin();
    auto scan_end_itr = container.end();

    switch (scan_direction) {
      case SCAN_DIRECTION_TYPE_FORWARD:
      case SCAN_DIRECTION_TYPE_BACKWARD: {
        // Scan the index entries in forward direction
        for (auto scan_itr = scan_begin_itr;
            scan_itr != scan_end_itr;
            ++scan_itr) {
          auto scan_current_key = scan_itr->first;
          auto tuple = scan_current_key.GetTupleForComparison(
              metadata->GetKeySchema());

          // Compare the current key in the scan with "values" based on
          // "expression types"
          // For instance, "5" EXPR_GREATER_THAN "2" is true
          if (Compare(tuple, key_column_ids, expr_types, values) == true) {
            ItemPointer *location_header = scan_itr->second;
            result.push_back(location_header);
          }
        }
      } break;

      case SCAN_DIRECTION_TYPE_INVALID:
      default:
        throw Exception("Invalid scan direction ");
        break;
    }
  }

  LOG_TRACE("Scan matched tuple count : %lu", result.size());

}

template <typename KeyType, typename ValueType, class KeyComparator,
class KeyEqualityChecker>
void SkipListIndex<KeyType, ValueType, KeyComparator,
KeyEqualityChecker>::ScanAllKeys(std::vector<ItemPointer *> &
                                 result) {

  // scan all entries
  auto iterator = container.begin();
  for (; iterator != container.end(); ++iterator) {
    ItemPointer *location = iterator->second;
    result.push_back(location);
  }

}

/**
 * @brief Return all locations related to this key.
 */
template <typename KeyType, typename ValueType, class KeyComparator,
class KeyEqualityChecker>
void SkipListIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ScanKey(
    const storage::Tuple *key, std::vector<ItemPointer *> &result) {
  KeyType index_key;
  index_key.SetFromKey(key);

  // find the <key, location> pair
  auto iterator = container.Contains(index_key);
  if(iterator != container.end()) {
    result.push_back(iterator->second);
  }

}

///////////////////////////////////////////////////////////////////////////////////////////

template <typename KeyType, typename ValueType, class KeyComparator,
class KeyEqualityChecker>
std::string SkipListIndex<KeyType, ValueType, KeyComparator,
KeyEqualityChecker>::GetTypeName() const {
  return "Btree";
}

// Explicit template instantiation

template class SkipListIndex<GenericKey<4>, ItemPointer *, GenericComparatorRaw<4>,
GenericEqualityChecker<4>>;
template class SkipListIndex<GenericKey<8>, ItemPointer *, GenericComparatorRaw<8>,
GenericEqualityChecker<8>>;
template class SkipListIndex<GenericKey<16>, ItemPointer *, GenericComparatorRaw<16>,
GenericEqualityChecker<16>>;
template class SkipListIndex<GenericKey<64>, ItemPointer *, GenericComparatorRaw<64>,
GenericEqualityChecker<64>>;
template class SkipListIndex<GenericKey<256>, ItemPointer *,
GenericComparatorRaw<256>, GenericEqualityChecker<256>>;

template class SkipListIndex<TupleKey, ItemPointer *, TupleKeyComparatorRaw,
TupleKeyEqualityChecker>;


}  // End index namespace
}  // End peloton namespace
