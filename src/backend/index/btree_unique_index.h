/*-------------------------------------------------------------------------
 *
 * btree_multimap_index.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <vector>
#include <string>

#include "backend/catalog/manager.h"
#include "backend/common/types.h"
#include "backend/storage/tuple.h"
#include "backend/index/index.h"

#include "stx/btree_map.h"

namespace peloton {
namespace index {

/**
 * B+tree-based index implementation.
 *
 * @see Index
 */
template<typename KeyType, class KeyComparator, class KeyEqualityChecker>
class BtreeUniqueIndex : public Index {
  friend class IndexFactory;

  typedef ItemPointer ValueType;
  typedef stx::btree_map<KeyType, ValueType, KeyComparator> MapType;

 public:
  BtreeUniqueIndex(IndexMetadata *metadata)
 : Index(metadata),
   container(KeyComparator(metadata)) {
  }

  ~BtreeUniqueIndex() {
  }

  bool InsertEntry(const storage::Tuple *key,
                   const ItemPointer location) {
    index_key1.setFromKey(key);
    auto status = container.insert(std::pair<KeyType, ValueType>(index_key1, location));

    std::cout << " Key : " << *key;
    std::cout << " Value : " << location.block << " " << location.offset << "\n";
    std::cout << " Status : " << status.second << "\n";
    std::cout << " Container : " << container.size() << "\n";
    return status.second;
  }

  bool DeleteEntry(const storage::Tuple *key) {
    index_key1.setFromKey(key);
    auto status = container.erase(index_key1);
    return status;
  }

  bool Exists(const storage::Tuple *key) {
    index_key1.setFromKey(key);
    auto found = (container.find(index_key1) != container.end());
    return found;
  }

  std::vector<ItemPointer> Scan() {
    std::vector<ItemPointer> result;

    auto itr = container.begin();
    while (itr != container.end()) {
      ItemPointer location = itr->second;
      result.push_back(location);
      itr++;
    }

    std::cout << "Total keys read " << result.size() << "\n";
    return result;
  }

  std::vector<ItemPointer> GetLocationsForKey(
      const storage::Tuple *key) {
    std::vector<ItemPointer> result;
    index_key1.setFromKey(key);

    auto itr = container.find(index_key1);
    while (itr != container.end()) {
      result.push_back(itr->second);
      itr++;
    }

    return result;
  }

  std::vector<ItemPointer> GetLocationsForKeyBetween(
      const storage::Tuple *start, const storage::Tuple *end) {
    std::vector<ItemPointer> result;

    index_key1.setFromKey(start);
    index_key2.setFromKey(end);

    auto start_itr = container.upper_bound(index_key1);
    auto end_itr = container.lower_bound(index_key2);
    while (start_itr != end_itr) {
      result.push_back(start_itr->second);
      start_itr++;
    }

    return result;
  }

  std::vector<ItemPointer> GetLocationsForKeyLT(
      const storage::Tuple *key) {
    std::vector<ItemPointer> result;

    index_key1.setFromKey(key);

    auto itr = container.begin();
    auto end_itr = container.upper_bound(index_key1);
    while (itr != end_itr) {
      result.push_back(itr->second);
      itr++;
    }

    return result;
  }

  std::vector<ItemPointer> GetLocationsForKeyLTE(
      const storage::Tuple *key) {
    std::vector<ItemPointer> result;

    index_key1.setFromKey(key);

    auto itr = container.begin();
    auto end_itr = container.find(index_key1);
    while (itr != end_itr) {
      result.push_back(itr->second);
      itr++;
    }

    return result;
  }

  std::vector<ItemPointer> GetLocationsForKeyGT(
      const storage::Tuple *key) {
    std::vector<ItemPointer> result;

    index_key1.setFromKey(key);

    auto itr = container.upper_bound(index_key1);
    auto end_itr = container.end();
    while (itr != end_itr) {
      result.push_back(itr->second);
      itr++;
    }

    return result;
  }

  std::vector<ItemPointer> GetLocationsForKeyGTE(
      const storage::Tuple *key) {
    std::vector<ItemPointer> result;

    index_key1.setFromKey(key);

    auto itr = container.find(index_key1);
    auto end_itr = container.end();
    while (itr != end_itr) {
      result.push_back(itr->second);
      itr++;
    }

    return result;
  }

  std::string GetTypeName() const { return "BtreeMap"; }

 protected:

  MapType container;
  KeyType index_key1;
  KeyType index_key2;

};

}  // End index namespace
}  // End peloton namespace
