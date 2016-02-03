//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// btree_index.cpp
//
// Identification: src/backend/index/btree_index.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/index/btree_index.h"
#include "backend/index/index_key.h"
#include "backend/common/logger.h"
#include "backend/storage/tuple.h"

namespace peloton {
namespace index {

template <typename KeyType, class KeyComparator, class KeyEqualityChecker>
BtreeIndex<KeyType, KeyComparator, KeyEqualityChecker>::BtreeIndex(
    IndexMetadata *metadata)
    : Index(metadata),
      container(KeyComparator(metadata)),
      equals(metadata),
      comparator(metadata) {}

template <typename KeyType, class KeyComparator, class KeyEqualityChecker>
BtreeIndex<KeyType, KeyComparator, KeyEqualityChecker>::~BtreeIndex() {}

template <typename KeyType, class KeyComparator, class KeyEqualityChecker>
bool BtreeIndex<KeyType, KeyComparator, KeyEqualityChecker>::InsertEntry(
    const storage::Tuple *key, const ItemPointer location) {
  KeyType index_key;
  index_key.SetFromKey(key);

  {
    index_lock.WriteLock();

    // Insert the key, val pair
    container.insert(std::pair<KeyType, ValueType>(index_key, location));

    index_lock.Unlock();
  }

  return true;
}

template <typename KeyType, class KeyComparator, class KeyEqualityChecker>
bool BtreeIndex<KeyType, KeyComparator, KeyEqualityChecker>::DeleteEntry(
    const storage::Tuple *key, const ItemPointer location) {
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

template <typename KeyType, class KeyComparator, class KeyEqualityChecker>
std::vector<ItemPointer>
BtreeIndex<KeyType, KeyComparator, KeyEqualityChecker>::Scan(
    const std::vector<Value> &values, const std::vector<oid_t> &key_column_ids,
    const std::vector<ExpressionType> &expr_types) {
  std::vector<ItemPointer> result;
  KeyType index_key;

  {
    index_lock.ReadLock();

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

    auto scan_begin_itr = container.begin();
    std::unique_ptr<storage::Tuple> start_key;
    bool all_constraints_are_equal = false;

    // If it is a special case, we can figure out the range to scan in the index
    if (special_case == true) {

      start_key.reset(new storage::Tuple(metadata->GetKeySchema(), true));
      index_key.SetFromKey(start_key.get());

      // Construct the lower bound key tuple
      all_constraints_are_equal =
          ConstructLowerBoundTuple(start_key.get(), values, key_column_ids, expr_types);
      LOG_TRACE("All constraints are equal : %d ", all_constraints_are_equal);

      // Set scan begin iterator
      scan_begin_itr = container.equal_range(index_key).first;
    }

    // Scan the index entries and compare them
    for (auto scan_itr = scan_begin_itr; scan_itr != container.end(); scan_itr++) {
      auto scan_current_key = scan_itr->first;
      auto tuple = scan_current_key.GetTupleForComparison(metadata->GetKeySchema());

      // Compare the current key in the scan with "values" based on "expression types"
      // For instance, "5" EXPR_GREATER_THAN "2" is true
      if (Compare(tuple, key_column_ids, expr_types, values) == true) {
        ItemPointer location = scan_itr->second;
        result.push_back(location);
      }
      else {
        // We can stop scanning if we know that all constraints are equal
        if(all_constraints_are_equal == true) {
          break;
        }
      }
    }

    index_lock.Unlock();
  }

  return result;
}

template <typename KeyType, class KeyComparator, class KeyEqualityChecker>
std::vector<ItemPointer>
BtreeIndex<KeyType, KeyComparator, KeyEqualityChecker>::ScanAllKeys() {
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
template <typename KeyType, class KeyComparator, class KeyEqualityChecker>
std::vector<ItemPointer>
BtreeIndex<KeyType, KeyComparator, KeyEqualityChecker>::ScanKey(
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

template <typename KeyType, class KeyComparator, class KeyEqualityChecker>
std::string
BtreeIndex<KeyType, KeyComparator, KeyEqualityChecker>::GetTypeName() const {
  return "Btree";
}

// Explicit template instantiation
template class BtreeIndex<GenericKey<4>, GenericComparator<4>,
                          GenericEqualityChecker<4>>;
template class BtreeIndex<GenericKey<8>, GenericComparator<8>,
                          GenericEqualityChecker<8>>;
template class BtreeIndex<GenericKey<12>, GenericComparator<12>,
                          GenericEqualityChecker<12>>;
template class BtreeIndex<GenericKey<16>, GenericComparator<16>,
                          GenericEqualityChecker<16>>;
template class BtreeIndex<GenericKey<24>, GenericComparator<24>,
                          GenericEqualityChecker<24>>;
template class BtreeIndex<GenericKey<32>, GenericComparator<32>,
                          GenericEqualityChecker<32>>;
template class BtreeIndex<GenericKey<48>, GenericComparator<48>,
                          GenericEqualityChecker<48>>;
template class BtreeIndex<GenericKey<64>, GenericComparator<64>,
                          GenericEqualityChecker<64>>;
template class BtreeIndex<GenericKey<96>, GenericComparator<96>,
                          GenericEqualityChecker<96>>;
template class BtreeIndex<GenericKey<128>, GenericComparator<128>,
                          GenericEqualityChecker<128>>;
template class BtreeIndex<GenericKey<256>, GenericComparator<256>,
                          GenericEqualityChecker<256>>;
template class BtreeIndex<GenericKey<512>, GenericComparator<512>,
                          GenericEqualityChecker<512>>;

template class BtreeIndex<TupleKey, TupleKeyComparator,
                          TupleKeyEqualityChecker>;

}  // End index namespace
}  // End peloton namespace
