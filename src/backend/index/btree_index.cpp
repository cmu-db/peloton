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
bool BtreeIndex<KeyType, KeyComparator, KeyEqualityChecker>::UpdateEntry(
    const storage::Tuple *key, const ItemPointer location) {
  KeyType index_key;
  index_key.SetFromKey(key);

  {
    index_lock.WriteLock();

    // insert the key, val pair
    container.insert(std::pair<KeyType, ValueType>(index_key, location));

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

    // check if we have leading column equality
    oid_t leading_column_id = 0;
    auto key_column_ids_itr = std::find(
        key_column_ids.begin(), key_column_ids.end(), leading_column_id);
    bool special_case = false;
    if (key_column_ids_itr != key_column_ids.end()) {
      auto offset = std::distance(key_column_ids.begin(), key_column_ids_itr);
      if (expr_types[offset] == EXPRESSION_TYPE_COMPARE_EQUAL) {
        special_case = true;
        LOG_INFO("Special case");
      }
    }

    auto itr_begin = container.begin();
    auto itr_end = container.end();
    storage::Tuple *start_key = nullptr;
    // start scanning from upper bound if possible
    if (special_case == true) {
      start_key = new storage::Tuple(metadata->GetKeySchema(), true);
      // set the lower bound tuple
      auto all_equal =
          SetLowerBoundTuple(start_key, values, key_column_ids, expr_types);
      index_key.SetFromKey(start_key);

      // all equal case
      if (all_equal) {
        auto ret = container.equal_range(index_key);
        itr_begin = ret.first;
        itr_end = ret.second;
        LOG_INFO("equal range return %ld", std::distance(itr_begin, itr_end));
      } else {
        itr_begin = container.upper_bound(index_key);
      }
    }

    // scan all entries comparing against arbitrary key
    for (auto itr = itr_begin; itr != itr_end; itr++) {
      auto index_key = itr->first;
      auto tuple = index_key.GetTupleForComparison(metadata->GetKeySchema());

      if (Compare(tuple, key_column_ids, expr_types, values) == true) {
        ItemPointer location = itr->second;
        result.push_back(location);
      }
    }

    delete start_key;

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
  std::vector<ItemPointer> retval;
  KeyType index_key;
  index_key.SetFromKey(key);

  {
    index_lock.ReadLock();

    // find the <key, location> pair
    auto entries = container.equal_range(index_key);
    for (auto entry = entries.first; entry != entries.second; ++entry) {
      retval.push_back(entry->second);
    }

    index_lock.Unlock();
  }

  return std::move(retval);
}

template <typename KeyType, class KeyComparator, class KeyEqualityChecker>
std::string
BtreeIndex<KeyType, KeyComparator, KeyEqualityChecker>::GetTypeName() const {
  return "Btree";
}

// Explicit template instantiation
template class BtreeIndex<GenericKey<4>, GenericComparator<4>, GenericEqualityChecker<4>>;
template class BtreeIndex<GenericKey<8>, GenericComparator<8>, GenericEqualityChecker<8>>;
template class BtreeIndex<GenericKey<12>, GenericComparator<12>, GenericEqualityChecker<12>>;
template class BtreeIndex<GenericKey<16>, GenericComparator<16>, GenericEqualityChecker<16>>;
template class BtreeIndex<GenericKey<24>, GenericComparator<24>, GenericEqualityChecker<24>>;
template class BtreeIndex<GenericKey<32>, GenericComparator<32>, GenericEqualityChecker<32>>;
template class BtreeIndex<GenericKey<48>, GenericComparator<48>, GenericEqualityChecker<48>>;
template class BtreeIndex<GenericKey<64>, GenericComparator<64>, GenericEqualityChecker<64>>;
template class BtreeIndex<GenericKey<96>, GenericComparator<96>, GenericEqualityChecker<96>>;
template class BtreeIndex<GenericKey<128>, GenericComparator<128>, GenericEqualityChecker<128>>;
template class BtreeIndex<GenericKey<256>, GenericComparator<256>, GenericEqualityChecker<256>>;
template class BtreeIndex<GenericKey<512>, GenericComparator<512>, GenericEqualityChecker<512>>;

template class BtreeIndex<TupleKey, TupleKeyComparator, TupleKeyEqualityChecker>;


}  // End index namespace
}  // End peloton namespace
