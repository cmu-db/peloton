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
#include "storage/tuple.h"

namespace peloton {
namespace index {

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
BTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::BTreeIndex(
    IndexMetadata *metadata)
    : Index(metadata),
      container(KeyComparator()),
      equals(),
      comparator() {}

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
      oid_t leading_column_offset = 0;
      oid_t leading_column_id = key_column_ids[leading_column_offset];
      std::vector<std::pair<Value, Value>> intervals;

      ConstructIntervals(leading_column_id, values, key_column_ids, expr_types,
                         intervals);

      // For non-leading columns, find the max and min
      std::map<oid_t, std::pair<Value, Value>> non_leading_columns;
      FindMaxMinInColumns(leading_column_id, values, key_column_ids, expr_types,
                          non_leading_columns);

      auto indexed_columns = metadata->GetKeySchema()->GetIndexedColumns();
      for (auto key_column_id : indexed_columns) {
        if (key_column_id == leading_column_id) {
          LOG_TRACE("Leading column : %u", key_column_id);
          continue;
        }

        if (non_leading_columns.find(key_column_id) == non_leading_columns.end()) {
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

        LOG_TRACE("%s", "Constructing start/end keys\n");

        LOG_TRACE("left bound %s\t\t right bound %s\n",
                  interval.first.GetInfo().c_str(),
                  interval.second.GetInfo().c_str());

        start_key->SetValue(leading_column_offset, interval.first, GetPool());
        end_key->SetValue(leading_column_offset, interval.second, GetPool());

        for (const auto &k_v : non_leading_columns) {
          start_key->SetValue(k_v.first, k_v.second.first, GetPool());
          end_key->SetValue(k_v.first, k_v.second.second, GetPool());
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
