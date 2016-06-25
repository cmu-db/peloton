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
      comparator(),
      indexed_tile_group_offset_(-1) {}

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
                                 const ItemPointer &location) {
  KeyType index_key;
  index_key.SetFromKey(key);

  // Insert the key, val pair
  auto status = container.Insert(index_key, new ItemPointer(location));

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
CondInsertEntry(const storage::Tuple *key, const ItemPointer &location,
                std::function<bool(const ItemPointer &)>) {
  KeyType index_key;
  index_key.SetFromKey(key);

  // Insert the key if it does not exist
  const bool bInsert = false;
  auto status = container.Update(index_key, new ItemPointer(location), bInsert);

  return status;
}

template <typename KeyType, typename ValueType, class KeyComparator,
class KeyEqualityChecker>
void SkipListIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Scan(
    const std::vector<Value> &values, const std::vector<oid_t> &key_column_ids,
    const std::vector<ExpressionType> &expr_types,
    const ScanDirectionType &scan_direction, std::vector<ItemPointer> &result) {
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

      LOG_TRACE("left bound %s\t\t right bound %s",
               interval.first.GetInfo().c_str(),
               interval.second.GetInfo().c_str());

      start_key->SetValue(leading_column_id, interval.first, GetPool());
      end_key->SetValue(leading_column_id, interval.second, GetPool());

      for (const auto &k_v : non_leading_columns) {
        start_key->SetValue(k_v.first, k_v.second.first, GetPool());
        end_key->SetValue(k_v.first, k_v.second.second, GetPool());
        LOG_TRACE("left bound %s\t\t right bound %s",
                 k_v.second.first.GetInfo().c_str(),
                 k_v.second.second.GetInfo().c_str());
      }

      KeyType start_index_key;
      KeyType end_index_key;
      start_index_key.SetFromKey(start_key.get());
      end_index_key.SetFromKey(end_key.get());

      scan_begin_itr = container.Contains(start_index_key);
      scan_end_itr = container.Contains(end_index_key);

      switch (scan_direction) {
        case SCAN_DIRECTION_TYPE_FORWARD:
        case SCAN_DIRECTION_TYPE_BACKWARD: {
          // Scan the index entries in forward direction
          for (auto scan_itr = scan_begin_itr; scan_itr != scan_end_itr;
              ++scan_itr) {
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
          throw Exception("Invalid scan direction ");
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
            ++scan_itr) {
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
        throw Exception("Invalid scan direction ");
        break;
    }
  }

}

template <typename KeyType, typename ValueType, class KeyComparator,
class KeyEqualityChecker>
void SkipListIndex<KeyType, ValueType, KeyComparator,
KeyEqualityChecker>::ScanAllKeys(std::vector<ItemPointer> &
                                 result) {

  // scan all entries
  auto iterator = container.begin();
  for (; iterator != container.end(); ++iterator) {
    ItemPointer item_pointer = *(iterator->second);
    result.push_back(std::move(item_pointer));
  }

}

template <typename KeyType, typename ValueType, class KeyComparator,
class KeyEqualityChecker>
void SkipListIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ScanKey(
    const storage::Tuple *key, std::vector<ItemPointer> &result) {
  KeyType index_key;
  index_key.SetFromKey(key);

  // find the <key, location> pair
  auto iterator = container.Contains(index_key);
  if(iterator != container.end()) {
    ItemPointer item_pointer = *(iterator->second);
    result.push_back(item_pointer);
  }

}

template <typename KeyType, typename ValueType, class KeyComparator,
class KeyEqualityChecker>
void SkipListIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
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
    LOG_TRACE("Column id : %u", key_column_ids[i]);

    if (key_column_ids[i] != leading_column_id) {
      continue;
    }

    // If leading column
    if (IfForwardExpression(expr_types[i])) {
      LOG_TRACE("Forward expression");
      nums.push_back(std::pair<Value, int>(values[i], -1));
    } else if (IfBackwardExpression(expr_types[i])) {
      LOG_TRACE("Backward expression");
      nums.push_back(std::pair<Value, int>(values[i], 1));
    } else {
      assert(expr_types[i] == EXPRESSION_TYPE_COMPARE_EQUAL);
      nums.push_back(std::pair<Value, int>(values[i], -1));
      nums.push_back(std::pair<Value, int>(values[i], 1));
    }
  }

  // Have merged all constraints in a single line, sort this line.
  std::sort(nums.begin(), nums.end(), Index::ValuePairComparator);
  assert(nums.size() > 0);

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
void SkipListIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
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
      LOG_TRACE("Leading column : %u", column_id);
      continue;
    }

    LOG_TRACE("Non leading column : %u", column_id);

    if (non_leading_columns.find(column_id) == non_leading_columns.end()) {
      auto type = values[i].GetValueType();
      // std::pair<Value, Value> *range = new std::pair<Value,
      // Value>(Value::GetMaxValue(type),
      //                                            Value::GetMinValue(type));
      // std::pair<oid_t, std::pair<Value, Value>> key_value(column_id, range);
      non_leading_columns.insert(std::pair<oid_t, std::pair<Value, Value>>(
          column_id, std::pair<Value, Value>(Value::GetNullValue(type),
                                             Value::GetNullValue(type))));
      //  non_leading_columns[column_id] = *range;
      // delete range;
      LOG_TRACE("Insert a init bounds\tleft size %lu\t right description %s",
               non_leading_columns[column_id].first.GetInfo().size(),
               non_leading_columns[column_id].second.GetInfo().c_str());
    }

    if (IfForwardExpression(expr_types[i]) ||
        expr_types[i] == EXPRESSION_TYPE_COMPARE_EQUAL) {
      LOG_TRACE("min cur %lu compare with %s",
               non_leading_columns[column_id].first.GetInfo().size(),
               values[i].GetInfo().c_str());
      if (non_leading_columns[column_id].first.IsNull() ||
          non_leading_columns[column_id].first.Compare(values[i]) ==
              VALUE_COMPARE_GREATERTHAN) {
        LOG_TRACE("Update min");
        non_leading_columns[column_id].first =
            ValueFactory::Clone(values[i], nullptr);
      }
    }

    if (IfBackwardExpression(expr_types[i]) ||
        expr_types[i] == EXPRESSION_TYPE_COMPARE_EQUAL) {
      LOG_TRACE("max cur %s compare with %s",
               non_leading_columns[column_id].second.GetInfo().c_str(),
               values[i].GetInfo().c_str());
      if (non_leading_columns[column_id].first.IsNull() ||
          non_leading_columns[column_id].second.Compare(values[i]) ==
              VALUE_COMPARE_LESSTHAN) {
        LOG_TRACE("Update max");
        non_leading_columns[column_id].second =
            ValueFactory::Clone(values[i], nullptr);
      }
    }
  }

  // check if min value is right bound or max value is left bound, if so, update
  for (const auto &k_v : non_leading_columns) {
    if (k_v.second.first.IsNull()) {
      non_leading_columns[k_v.first].first =
          Value::GetMinValue(k_v.second.first.GetValueType());
    }
    if (k_v.second.second.IsNull()) {
      non_leading_columns[k_v.first].second =
          Value::GetMaxValue(k_v.second.second.GetValueType());
    }
  }
};

///////////////////////////////////////////////////////////////////////

template <typename KeyType, typename ValueType, class KeyComparator,
class KeyEqualityChecker>
void SkipListIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Scan(
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

  // If it is a special case, we can figure out the range to scan in the index
  if (special_case == true) {
    // Assumption: must have leading column, assume it's first one in
    // key_column_ids.
    assert(key_column_ids.size() > 0);
    LOG_TRACE("key_column_ids size : %lu ", key_column_ids.size());

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

      LOG_TRACE("Leading Column: column id : %u left bound %s\t\t right bound %s",
               leading_column_id,
               interval.first.GetInfo().c_str(),
               interval.second.GetInfo().c_str());

      start_key->SetValue(leading_column_id, interval.first, GetPool());
      end_key->SetValue(leading_column_id, interval.second, GetPool());

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
          for (auto scan_itr = scan_begin_itr; scan_itr != scan_end_itr;
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

  } else {
    auto scan_begin_itr = container.begin();

    switch (scan_direction) {
      case SCAN_DIRECTION_TYPE_FORWARD:
      case SCAN_DIRECTION_TYPE_BACKWARD: {
        // Scan the index entries in forward direction
        for (auto scan_itr = scan_begin_itr; scan_itr != container.end();
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
