//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// btree_index.cpp
//
// Identification: src/backend/index/btree_index.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/index/btree_index.h"
#include "backend/index/index_key.h"
#include "backend/common/logger.h"
#include "backend/storage/tuple.h"

namespace peloton {
namespace index {

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
BTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::BTreeIndex(
    IndexMetadata *metadata)
    : Index(metadata),
      container(KeyComparator(metadata)),
      equals(metadata),
      comparator(metadata) {}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
BTreeIndex<KeyType, ValueType, KeyComparator,
           KeyEqualityChecker>::~BTreeIndex() {
  // we should not rely on shared_ptr to reclaim memory.
  // this is because the underlying index can split or merge leaf nodes,
  // which invokes data data copy and deletes.
  // as the underlying index is unaware of shared_ptr,
  // memory allocated should be managed carefully by programmers.
  for (auto entry = container.begin(); entry != container.end(); ++entry) {
    delete entry->second;
    entry->second = nullptr;
  }
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
bool BTreeIndex<KeyType, ValueType, KeyComparator,
                KeyEqualityChecker>::InsertEntry(const storage::Tuple *key,
                                                 const ItemPointer &location) {
  KeyType index_key;

  index_key.SetFromKey(key);
  std::pair<KeyType, ValueType> entry(index_key, new ItemPointer(location));

  {
    index_lock.WriteLock();

    // Insert the key, val pair
    container.insert(entry);

    index_lock.Unlock();
  }

  return true;
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
bool BTreeIndex<KeyType, ValueType, KeyComparator,
                KeyEqualityChecker>::DeleteEntry(const storage::Tuple *key,
                                                 const ItemPointer &location) {
  KeyType index_key;
  index_key.SetFromKey(key);

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
        ItemPointer value = *(iterator->second);

        if ((value.block == location.block) &&
            (value.offset == location.offset)) {
          delete iterator->second;
          iterator->second = nullptr;
          container.erase(iterator);
          // Set try again
          try_again = true;
          break;
        }
      }
    }

    index_lock.Unlock();
  }

  return true;
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
bool BTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
    CondInsertEntry(const storage::Tuple *key, const ItemPointer &location,
                    std::function<bool(const ItemPointer &)> predicate) {
  KeyType index_key;
  index_key.SetFromKey(key);

  {
    index_lock.WriteLock();

    // find the <key, location> pair
    auto entries = container.equal_range(index_key);
    for (auto entry = entries.first; entry != entries.second; ++entry) {
      ItemPointer item_pointer = *(entry->second);

      if (predicate(item_pointer)) {
        // this key is already visible or dirty in the index
        index_lock.Unlock();
        return false;
      }
    }

    // Insert the key, val pair
    container.insert(
        std::pair<KeyType, ValueType>(index_key, new ItemPointer(location)));

    index_lock.Unlock();
  }

  return true;
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
void BTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Scan(
    const std::vector<Value> &values, const std::vector<oid_t> &key_column_ids,
    const std::vector<ExpressionType> &expr_types,
    const ScanDirectionType &scan_direction, std::vector<ItemPointer> &result) {
  bool special_case = true;
  for (auto key_column_ids_itr = key_column_ids.begin();
       key_column_ids_itr != key_column_ids.end(); key_column_ids_itr++) {
    auto offset = std::distance(key_column_ids.begin(), key_column_ids_itr);
    if (expr_types[offset] == EXPRESSION_TYPE_COMPARE_NOTEQUAL ||
        expr_types[offset] == EXPRESSION_TYPE_COMPARE_IN ||
        expr_types[offset] == EXPRESSION_TYPE_COMPARE_LIKE ||
        expr_types[offset] == EXPRESSION_TYPE_COMPARE_NOTLIKE) {
      special_case = false;
      break;
    }
  }

  LOG_TRACE("Special case : %d ", special_case);

  {
    index_lock.ReadLock();

    // If it is a special case, we can figure out the range to scan in the index
    if (special_case == true) {
      // Assumption: must have leading column, assume it's first one in
      // key_column_ids.
      assert(key_column_ids.size() > 0);
      oid_t leading_column_id = key_column_ids[0];
      std::vector<std::pair<Value, Value>> intervals;

      ConstructIntervals(leading_column_id, values, key_column_ids, expr_types,
                         intervals);

      // For non-leading columns, find the max and min
      std::map<oid_t, std::pair<Value, Value>> non_leading_columns;
      FindMaxMinInColumns(leading_column_id, values, key_column_ids, expr_types,
                          non_leading_columns);

      for (auto key_column_id : metadata->GetKeySchema()->GetIndexedColumns()) {
        if (non_leading_columns.find(key_column_id) ==
            non_leading_columns.end()) {
          auto type =
              metadata->GetKeySchema()->GetColumn(key_column_id).column_type;
          std::pair<Value, Value> range(Value::GetMinValue(type),
                                        Value::GetMaxValue(type));
          std::pair<oid_t, std::pair<Value, Value>> key_value(key_column_id,
                                                              range);
          non_leading_columns.insert(key_value);
        }
      }

      // Search each interval of leading_column.
      for (const auto &interval : intervals) {
        auto scan_begin_itr = container.begin();
        auto scan_end_itr = container.end();
        std::unique_ptr<storage::Tuple> start_key;
        std::unique_ptr<storage::Tuple> end_key;
        start_key.reset(new storage::Tuple(metadata->GetKeySchema(), true));
        end_key.reset(new storage::Tuple(metadata->GetKeySchema(), true));

        LOG_TRACE("%s", "Constructing start/end keys");

        LOG_TRACE("left bound %s\t\t right bound %s", interval.first.GetInfo(),
                  interval.second.GetInfo());

        start_key->SetValue(leading_column_id, interval.first, GetPool());
        end_key->SetValue(leading_column_id, interval.second, GetPool());

        for (const auto &k_v : non_leading_columns) {
          start_key->SetValue(k_v.first, k_v.second.first, GetPool());
          end_key->SetValue(k_v.first, k_v.second.second, GetPool());
          LOG_TRACE("left bound %s\t\t right bound %s",
                    k_v.second.first.GetInfo(), k_v.second.second.GetInfo());
        }

        KeyType start_index_key;
        KeyType end_index_key;
        start_index_key.SetFromKey(start_key.get());
        end_index_key.SetFromKey(end_key.get());

        scan_begin_itr = container.equal_range(start_index_key).first;
        scan_end_itr = container.equal_range(end_index_key).second;

        switch (scan_direction) {
          case SCAN_DIRECTION_TYPE_FORWARD:
          case SCAN_DIRECTION_TYPE_BACKWARD: {
            // Scan the index entries in forward direction
            for (auto scan_itr = scan_begin_itr; scan_itr != scan_end_itr;
                 scan_itr++) {
              auto scan_current_key = scan_itr->first;
              auto tuple = scan_current_key.GetTupleForComparison(
                  metadata->GetKeySchema());

              // Compare the current key in the scan with "values" based on
              // "expression types"
              // For instance, "5" EXPR_GREATER_THAN "2" is true
              if (Compare(tuple, key_column_ids, expr_types, values) == true) {
                ItemPointer location_header = *(scan_itr->second);
                result.push_back(location_header);
              }
            }
          } break;

          case SCAN_DIRECTION_TYPE_INVALID:
          default:
            throw Exception("Invalid scan direction \n");
            break;
        }
      }

    } else {
      auto scan_begin_itr = container.begin();

      switch (scan_direction) {
        case SCAN_DIRECTION_TYPE_FORWARD:
        case SCAN_DIRECTION_TYPE_BACKWARD: {
          // Scan the index entries in forward direction
          for (auto scan_itr = scan_begin_itr; scan_itr != container.end();
               scan_itr++) {
            auto scan_current_key = scan_itr->first;
            auto tuple = scan_current_key.GetTupleForComparison(
                metadata->GetKeySchema());

            // Compare the current key in the scan with "values" based on
            // "expression types"
            // For instance, "5" EXPR_GREATER_THAN "2" is true
            if (Compare(tuple, key_column_ids, expr_types, values) == true) {
              ItemPointer location_header = *(scan_itr->second);
              result.push_back(location_header);
            }
          }
        } break;

        case SCAN_DIRECTION_TYPE_INVALID:
        default:
          throw Exception("Invalid scan direction \n");
          break;
      }
    }

    index_lock.Unlock();
  }
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
void BTreeIndex<KeyType, ValueType, KeyComparator,
                KeyEqualityChecker>::ScanAllKeys(std::vector<ItemPointer> &
                                                     result) {
  {
    index_lock.ReadLock();

    auto itr = container.begin();

    // scan all entries
    while (itr != container.end()) {
      ItemPointer item_pointer = *(itr->second);

      result.push_back(std::move(item_pointer));
      itr++;
    }

    index_lock.Unlock();
  }
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
void BTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ScanKey(
    const storage::Tuple *key, std::vector<ItemPointer> &result) {
  KeyType index_key;
  index_key.SetFromKey(key);

  {
    index_lock.ReadLock();

    // find the <key, location> pair
    auto entries = container.equal_range(index_key);
    for (auto entry = entries.first; entry != entries.second; ++entry) {
      ItemPointer item_pointer = *(entry->second);

      result.push_back(item_pointer);
    }

    index_lock.Unlock();
  }
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
void BTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
    ConstructIntervals(oid_t leading_column_id,
                       const std::vector<Value> &values,
                       const std::vector<oid_t> &key_column_ids,
                       const std::vector<ExpressionType> &expr_types,
                       std::vector<std::pair<Value, Value>> &intervals) {
  // Find all contrains of leading column.
  // Equal --> > < num
  // > >= --->  > num
  // < <= ----> < num
  std::vector<std::pair<peloton::Value, int>> nums;
  for (size_t i = 0; i < key_column_ids.size(); i++) {
    if (key_column_ids[i] != leading_column_id) {
      continue;
    }

    // If leading column
    if (IfForwardExpression(expr_types[i])) {
      nums.push_back(std::pair<Value, int>(values[i], -1));
    } else if (IfBackwardExpression(expr_types[i])) {
      nums.push_back(std::pair<Value, int>(values[i], 1));
    } else {
      assert(expr_types[i] == EXPRESSION_TYPE_COMPARE_EQUAL);
      nums.push_back(std::pair<Value, int>(values[i], -1));
      nums.push_back(std::pair<Value, int>(values[i], 1));
    }
  }

  // Have merged all constraints in a single line, sort this line.
  std::sort(nums.begin(), nums.end(), Index::ValuePairComparator);
  assert(nums.size() == 0);

  // Build intervals.
  Value cur;
  size_t i = 0;
  if (nums[0].second < 0) {
    cur = nums[0].first;
    i++;
  } else {
    cur = Value::GetMinValue(nums[0].first.GetValueType());
  }

  while (i < nums.size()) {
    if (nums[i].second > 0) {
      if (i + 1 < nums.size() && nums[i + 1].second < 0) {
        // right value
        intervals.push_back(std::pair<Value, Value>(cur, nums[i].first));
        cur = nums[i + 1].first;
      } else if (i + 1 == nums.size()) {
        // Last value while right value
        intervals.push_back(std::pair<Value, Value>(cur, nums[i].first));
        cur = Value::GetNullValue(nums[0].first.GetValueType());
      }
    }
    i++;
  }

  if (cur.IsNull() == false) {
    intervals.push_back(std::pair<Value, Value>(
        cur, Value::GetMaxValue(nums[0].first.GetValueType())));
  }

  // Finish invtervals building.
};

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
void BTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
    FindMaxMinInColumns(
        oid_t leading_column_id, const std::vector<Value> &values,
        const std::vector<oid_t> &key_column_ids,
        const std::vector<ExpressionType> &expr_types,
        std::map<oid_t, std::pair<Value, Value>> &non_leading_columns) {
  // find extreme nums on each column.
  LOG_TRACE("FindMinMax leading column %d", leading_column_id);
  for (size_t i = 0; i < key_column_ids.size(); i++) {
    oid_t column_id = key_column_ids[i];
    if (column_id == leading_column_id) {
      continue;
    }

    if (non_leading_columns.find(column_id) == non_leading_columns.end()) {
      auto type = values[i].GetValueType();
      //std::pair<Value, Value> *range = new std::pair<Value, Value>(Value::GetMaxValue(type),
      //                                            Value::GetMinValue(type));
      // std::pair<oid_t, std::pair<Value, Value>> key_value(column_id, range);
       non_leading_columns.insert(std::pair<oid_t, std::pair<Value, Value>>(
                                         column_id, std::pair<Value, Value>(
                                          Value::GetMaxValue(type),
                                          Value::GetMinValue(type))));
      //  non_leading_columns[column_id] = *range;
      // delete range;
      LOG_TRACE("Insert a init bounds\tleft size %lu\t right description %s",
                non_leading_columns[column_id].first.GetInfo().size(),
                non_leading_columns[column_id].second.GetInfo());
    }

    if (IfForwardExpression(expr_types[i]) ||
        expr_types[i] == EXPRESSION_TYPE_COMPARE_EQUAL) {
      LOG_TRACE("min cur %lu compare with %s",
                non_leading_columns[column_id].first.GetInfo().size(),
                values[i].GetInfo());
      if (non_leading_columns[column_id].first.Compare(values[i]) ==
          VALUE_COMPARE_GREATERTHAN) {
        LOG_TRACE("Update min");
        non_leading_columns[column_id].first = ValueFactory::Clone(values[i], nullptr);
      }
    }

    if (IfBackwardExpression(expr_types[i]) ||
        expr_types[i] == EXPRESSION_TYPE_COMPARE_EQUAL) {
      LOG_TRACE("max cur %s compare with %s",
                non_leading_columns[column_id].second.GetInfo(),
                values[i].GetInfo());
      if (non_leading_columns[column_id].second.Compare(values[i]) ==
          VALUE_COMPARE_LESSTHAN) {
        LOG_TRACE("Update max");
        non_leading_columns[column_id].second = ValueFactory::Clone(values[i], nullptr);
      }
    }
  }

  // check if min value is right bound or max value is left bound, if so, update
  for (const auto &k_v : non_leading_columns) {
    if (k_v.second.first ==
        Value::GetMaxValue(k_v.second.first.GetValueType())) {
      non_leading_columns[k_v.first].first =
          Value::GetMinValue(k_v.second.first.GetValueType());
    }
    if (k_v.second.second ==
        Value::GetMinValue(k_v.second.second.GetValueType())) {
      non_leading_columns[k_v.first].second =
          Value::GetMaxValue(k_v.second.second.GetValueType());
    }
  }
};

///////////////////////////////////////////////////////////////////////

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
void BTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Scan(
    const std::vector<Value> &values, const std::vector<oid_t> &key_column_ids,
    const std::vector<ExpressionType> &expr_types,
    const ScanDirectionType &scan_direction,
    std::vector<ItemPointer *> &result) {
  // Check if we have leading (leftmost) column equality
  // refer : http://www.postgresql.org/docs/8.2/static/indexes-multicolumn.html
  //  oid_t leading_column_id = 0;
  //  auto key_column_ids_itr = std::find(key_column_ids.begin(),
  //                                      key_column_ids.end(),
  //                                      leading_column_id);

  // SPECIAL CASE : leading column id is one of the key column ids
  // and is involved in a equality constraint
  // Currently special case only includes EXPRESSION_TYPE_COMPARE_EQUAL
  // There are two more types, one is aligned, another is not aligned.
  // Aligned example: A > 0, B >= 15, c > 4
  // Not Aligned example: A >= 15, B < 30

  bool special_case = true;
  for (auto key_column_ids_itr = key_column_ids.begin();
       key_column_ids_itr != key_column_ids.end(); key_column_ids_itr++) {
    auto offset = std::distance(key_column_ids.begin(), key_column_ids_itr);
    if (expr_types[offset] == EXPRESSION_TYPE_COMPARE_NOTEQUAL ||
        expr_types[offset] == EXPRESSION_TYPE_COMPARE_IN ||
        expr_types[offset] == EXPRESSION_TYPE_COMPARE_LIKE ||
        expr_types[offset] == EXPRESSION_TYPE_COMPARE_NOTLIKE) {
      special_case = false;
      break;
    }
  }

  LOG_TRACE("Special case : %d ", special_case);

  {
    index_lock.ReadLock();

    // If it is a special case, we can figure out the range to scan in the index
    if (special_case == true) {
      // Assumption: must have leading column, assume it's first one in
      // key_column_ids.
      assert(key_column_ids.size() > 0);
      oid_t leading_column_id = key_column_ids[0];
      std::vector<std::pair<Value, Value>> intervals;

      ConstructIntervals(leading_column_id, values, key_column_ids, expr_types,
                         intervals);

      // For non-leading columns, find the max and min
      std::map<oid_t, std::pair<Value, Value>> non_leading_columns;
      FindMaxMinInColumns(leading_column_id, values, key_column_ids, expr_types,
                          non_leading_columns);

      for (auto key_column_id : metadata->GetKeySchema()->GetIndexedColumns()) {
        if (non_leading_columns.find(key_column_id) ==
            non_leading_columns.end()) {
          auto type =
              metadata->GetKeySchema()->GetColumn(key_column_id).column_type;
          std::pair<Value, Value> range(Value::GetMinValue(type),
                                        Value::GetMaxValue(type));
          std::pair<oid_t, std::pair<Value, Value>> key_value(key_column_id,
                                                              range);
          non_leading_columns.insert(key_value);
        }
      }

      assert(intervals.size() != 0);
      // Search each interval of leading_column.
      for (const auto &interval : intervals) {
        auto scan_begin_itr = container.begin();
        auto scan_end_itr = container.end();
        std::unique_ptr<storage::Tuple> start_key;
        std::unique_ptr<storage::Tuple> end_key;
        start_key.reset(new storage::Tuple(metadata->GetKeySchema(), true));
        end_key.reset(new storage::Tuple(metadata->GetKeySchema(), true));

        LOG_TRACE("%s", "Constructing start/end keys");

        LOG_TRACE("left bound %s\t\t right bound %s", interval.first.GetInfo(),
                  interval.second.GetInfo());

        start_key->SetValue(leading_column_id, interval.first, GetPool());
        end_key->SetValue(leading_column_id, interval.second, GetPool());

        for (const auto &k_v : non_leading_columns) {
          start_key->SetValue(k_v.first, k_v.second.first, GetPool());
          end_key->SetValue(k_v.first, k_v.second.second, GetPool());
          LOG_TRACE("left bound %s\t\t right bound %s",
                    k_v.second.first.GetInfo(), k_v.second.second.GetInfo());
        }

        KeyType start_index_key;
        KeyType end_index_key;
        start_index_key.SetFromKey(start_key.get());
        end_index_key.SetFromKey(end_key.get());

        scan_begin_itr = container.equal_range(start_index_key).first;
        scan_end_itr = container.equal_range(end_index_key).second;

        switch (scan_direction) {
          case SCAN_DIRECTION_TYPE_FORWARD:
          case SCAN_DIRECTION_TYPE_BACKWARD: {
            // Scan the index entries in forward direction
            for (auto scan_itr = scan_begin_itr; scan_itr != scan_end_itr;
                 scan_itr++) {
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
            throw Exception("Invalid scan direction \n");
            break;
        }
      }

    } else {
      auto scan_begin_itr = container.begin();

      switch (scan_direction) {
        case SCAN_DIRECTION_TYPE_FORWARD:
        case SCAN_DIRECTION_TYPE_BACKWARD: {
          // Scan the index entries in forward direction
          for (auto scan_itr = scan_begin_itr; scan_itr != container.end();
               scan_itr++) {
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
          throw Exception("Invalid scan direction \n");
          break;
      }
    }

    index_lock.Unlock();
  }
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
void BTreeIndex<KeyType, ValueType, KeyComparator,
                KeyEqualityChecker>::ScanAllKeys(std::vector<ItemPointer *> &
                                                     result) {
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
}

/**
 * @brief Return all locations related to this key.
 */
template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
void BTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ScanKey(
    const storage::Tuple *key, std::vector<ItemPointer *> &result) {
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
}

///////////////////////////////////////////////////////////////////////////////////////////

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
std::string BTreeIndex<KeyType, ValueType, KeyComparator,
                       KeyEqualityChecker>::GetTypeName() const {
  return "Btree";
}

// Explicit template instantiation
template class BTreeIndex<IntsKey<1>, ItemPointer *, IntsComparator<1>,
                          IntsEqualityChecker<1>>;
template class BTreeIndex<IntsKey<2>, ItemPointer *, IntsComparator<2>,
                          IntsEqualityChecker<2>>;
template class BTreeIndex<IntsKey<3>, ItemPointer *, IntsComparator<3>,
                          IntsEqualityChecker<3>>;
template class BTreeIndex<IntsKey<4>, ItemPointer *, IntsComparator<4>,
                          IntsEqualityChecker<4>>;

template class BTreeIndex<GenericKey<4>, ItemPointer *, GenericComparator<4>,
                          GenericEqualityChecker<4>>;
template class BTreeIndex<GenericKey<8>, ItemPointer *, GenericComparator<8>,
                          GenericEqualityChecker<8>>;
template class BTreeIndex<GenericKey<12>, ItemPointer *, GenericComparator<12>,
                          GenericEqualityChecker<12>>;
template class BTreeIndex<GenericKey<16>, ItemPointer *, GenericComparator<16>,
                          GenericEqualityChecker<16>>;
template class BTreeIndex<GenericKey<24>, ItemPointer *, GenericComparator<24>,
                          GenericEqualityChecker<24>>;
template class BTreeIndex<GenericKey<32>, ItemPointer *, GenericComparator<32>,
                          GenericEqualityChecker<32>>;
template class BTreeIndex<GenericKey<48>, ItemPointer *, GenericComparator<48>,
                          GenericEqualityChecker<48>>;
template class BTreeIndex<GenericKey<64>, ItemPointer *, GenericComparator<64>,
                          GenericEqualityChecker<64>>;
template class BTreeIndex<GenericKey<96>, ItemPointer *, GenericComparator<96>,
                          GenericEqualityChecker<96>>;
template class BTreeIndex<GenericKey<128>, ItemPointer *,
                          GenericComparator<128>, GenericEqualityChecker<128>>;
template class BTreeIndex<GenericKey<256>, ItemPointer *,
                          GenericComparator<256>, GenericEqualityChecker<256>>;
template class BTreeIndex<GenericKey<512>, ItemPointer *,
                          GenericComparator<512>, GenericEqualityChecker<512>>;

template class BTreeIndex<TupleKey, ItemPointer *, TupleKeyComparator,
                          TupleKeyEqualityChecker>;

}  // End index namespace
}  // End peloton namespace
