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
           KeyEqualityChecker>::~BTreeIndex() {}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
bool BTreeIndex<KeyType, ValueType, KeyComparator,
                KeyEqualityChecker>::InsertEntry(const storage::Tuple *key,
                                                 ItemPointer &location) {
  KeyType index_key;

  index_key.SetFromKey(key);
  std::pair<KeyType, ValueType> entry = std::make_pair(index_key, location);

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
        ItemPointer value = iterator->second;

        if ((value.block == location.block) &&
            (value.offset == location.offset)) {
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
bool BTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>
::ConditionalInsertEntry(const storage::Tuple *key,
    const ItemPointer &location,
    std::function<bool(const storage::Tuple *, const ItemPointer &)> predicate) {

  std::vector<ItemPointer> result;
  KeyType index_key;
  index_key.SetFromKey(key);

  {
    index_lock.WriteLock();

    // find the <key, location> pair
    auto entries = container.equal_range(index_key);
    for (auto entry = entries.first; entry != entries.second; ++entry) {
      if (predicate(key, entry->second)) {
        // this key is already visible or dirty in the index
        return false;
      }
    }

    // Insert the key, val pair
    container.insert(std::pair<KeyType, ValueType>(index_key, location));

    index_lock.Unlock();
  }

  return true;
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
std::vector<ItemPointer>
BTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Scan(
    const std::vector<Value> &values, const std::vector<oid_t> &key_column_ids,
    const std::vector<ExpressionType> &expr_types,
    const ScanDirectionType &scan_direction) {
  std::vector<ItemPointer> result;
  KeyType index_key;

  // Check if we have leading (leftmost) column equality
  // refer : http://www.postgresql.org/docs/8.2/static/indexes-multicolumn.html
  oid_t leading_column_id = 0;
  auto key_column_ids_itr = std::find(
      key_column_ids.begin(), key_column_ids.end(), leading_column_id);

  // SPECIAL CASE : leading column id is one of the key column ids
  // and is involved in a equality constraint
  bool special_case = false;
  if (key_column_ids_itr != key_column_ids.end()) {
    auto offset = std::distance(key_column_ids.begin(), key_column_ids_itr);
    if (expr_types[offset] == EXPRESSION_TYPE_COMPARE_EQUAL) {
      special_case = true;
    }
  }

  LOG_TRACE("Special case : %d ", special_case);

  {
    index_lock.ReadLock();

    auto scan_begin_itr = container.begin();
    std::unique_ptr<storage::Tuple> start_key;
    bool all_constraints_are_equal = false;

    // If it is a special case, we can figure out the range to scan in the index
    if (special_case == true) {
      start_key.reset(new storage::Tuple(metadata->GetKeySchema(), true));

      // Construct the lower bound key tuple
      all_constraints_are_equal = ConstructLowerBoundTuple(
          start_key.get(), values, key_column_ids, expr_types);
      LOG_TRACE("All constraints are equal : %d ", all_constraints_are_equal);
      index_key.SetFromKey(start_key.get());

      index_key.SetFromKey(start_key.get());

      // Set scan begin iterator
      scan_begin_itr = container.equal_range(index_key).first;
    }

    switch (scan_direction) {
      case SCAN_DIRECTION_TYPE_FORWARD:
      case SCAN_DIRECTION_TYPE_BACKWARD: {
        // Scan the index entries in forward direction
        for (auto scan_itr = scan_begin_itr; scan_itr != container.end();
             scan_itr++) {
          auto scan_current_key = scan_itr->first;
          auto tuple =
              scan_current_key.GetTupleForComparison(metadata->GetKeySchema());

          // Compare the current key in the scan with "values" based on
          // "expression types"
          // For instance, "5" EXPR_GREATER_THAN "2" is true
          if (Compare(tuple, key_column_ids, expr_types, values) == true) {
            ItemPointer location = scan_itr->second;
            result.push_back(location);
          } else {
            // We can stop scanning if we know that all constraints are equal
            if (all_constraints_are_equal == true) {
              break;
            }
          }
        }

      } break;

      case SCAN_DIRECTION_TYPE_INVALID:
      default:
        throw Exception("Invalid scan direction \n");
        break;
    }

    index_lock.Unlock();
  }

  return result;
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
std::vector<ItemPointer> BTreeIndex<KeyType, ValueType, KeyComparator,
                                    KeyEqualityChecker>::ScanAllKeys() {
  std::vector<ItemPointer> result;

  {
    index_lock.ReadLock();

    auto itr = container.begin();

    // scan all entries
    while (itr != container.end()) {
      ItemPointer location = itr->second;
      result.push_back(location);
      itr++;
    }

    index_lock.Unlock();
  }

  return result;
}

/**
 * @brief Return all locations related to this key.
 */
template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
std::vector<ItemPointer>
BTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ScanKey(
    const storage::Tuple *key) {
  std::vector<ItemPointer> result;
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

  return result;
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
std::string BTreeIndex<KeyType, ValueType, KeyComparator,
                       KeyEqualityChecker>::GetTypeName() const {
  return "Btree";
}

// Explicit template instantiation
template class BTreeIndex<IntsKey<1>, ItemPointer, IntsComparator<1>,
                          IntsEqualityChecker<1>>;
template class BTreeIndex<IntsKey<2>, ItemPointer, IntsComparator<2>,
                          IntsEqualityChecker<2>>;
template class BTreeIndex<IntsKey<3>, ItemPointer, IntsComparator<3>,
                          IntsEqualityChecker<3>>;
template class BTreeIndex<IntsKey<4>, ItemPointer, IntsComparator<4>,
                          IntsEqualityChecker<4>>;

template class BTreeIndex<GenericKey<4>, ItemPointer, GenericComparator<4>,
                          GenericEqualityChecker<4>>;
template class BTreeIndex<GenericKey<8>, ItemPointer, GenericComparator<8>,
                          GenericEqualityChecker<8>>;
template class BTreeIndex<GenericKey<12>, ItemPointer, GenericComparator<12>,
                          GenericEqualityChecker<12>>;
template class BTreeIndex<GenericKey<16>, ItemPointer, GenericComparator<16>,
                          GenericEqualityChecker<16>>;
template class BTreeIndex<GenericKey<24>, ItemPointer, GenericComparator<24>,
                          GenericEqualityChecker<24>>;
template class BTreeIndex<GenericKey<32>, ItemPointer, GenericComparator<32>,
                          GenericEqualityChecker<32>>;
template class BTreeIndex<GenericKey<48>, ItemPointer, GenericComparator<48>,
                          GenericEqualityChecker<48>>;
template class BTreeIndex<GenericKey<64>, ItemPointer, GenericComparator<64>,
                          GenericEqualityChecker<64>>;
template class BTreeIndex<GenericKey<96>, ItemPointer, GenericComparator<96>,
                          GenericEqualityChecker<96>>;
template class BTreeIndex<GenericKey<128>, ItemPointer, GenericComparator<128>,
                          GenericEqualityChecker<128>>;
template class BTreeIndex<GenericKey<256>, ItemPointer, GenericComparator<256>,
                          GenericEqualityChecker<256>>;
template class BTreeIndex<GenericKey<512>, ItemPointer, GenericComparator<512>,
                          GenericEqualityChecker<512>>;

template class BTreeIndex<TupleKey, ItemPointer, TupleKeyComparator,
                          TupleKeyEqualityChecker>;

}  // End index namespace
}  // End peloton namespace
