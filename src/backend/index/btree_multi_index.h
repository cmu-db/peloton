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
#include <map>

#include "backend/catalog/manager.h"
#include "backend/common/types.h"
#include "backend/storage/tuple.h"
#include "backend/index/index.h"

#include "stx/btree_multimap.h"

namespace peloton {
namespace index {

/**
 * B+tree-based index implementation.
 *
 * @see Index
 */
template<typename KeyType, class KeyComparator, class KeyEqualityChecker>
class BtreeMultiIndex : public Index {
  friend class IndexFactory;

  typedef ItemPointer ValueType;
  typedef stx::btree_multimap<KeyType, ValueType, KeyComparator> MapType;

 public:

  BtreeMultiIndex(IndexMetadata *metadata)
 : Index(metadata),
   container(KeyComparator(metadata)),
   equals(metadata),
   comparator(metadata){
  }

  ~BtreeMultiIndex() {
  }

  bool InsertEntry(const storage::Tuple *key,
                   const ItemPointer location) {
    {
      std::lock_guard<std::mutex> lock(index_mutex);

      index_key1.SetFromKey(key);

      // Insert the key, val pair
      container.insert(std::pair<KeyType, ValueType>(index_key1, location));
      return true;
    }
  }

  bool DeleteEntry(const storage::Tuple *key,
                   const ItemPointer location) {
    {
      std::lock_guard<std::mutex> lock(index_mutex);

      index_key1.SetFromKey(key);

      // Delete the < key, location > pair
      auto entries = container.equal_range(index_key1);
      for (auto entry = entries.first ; entry != entries.second; ++entry) {
        ItemPointer value = entry->second;
        if((value.block == location.block) &&
            (value.offset == location.offset)) {
          // remove matching (key, value) entry
          container.erase(entry);
        }
      }

      return true;
    }
  }

  bool UpdateEntry(const storage::Tuple *key,
                   const ItemPointer location,
                   const ItemPointer old_location) {

    {
      std::lock_guard<std::mutex> lock(index_mutex);

      index_key1.SetFromKey(key);

      // Check for <key, old location> first
      auto entries = container.equal_range(index_key1);
      for (auto entry = entries.first ; entry != entries.second; ++entry) {
        ItemPointer value = entry->second;
        if((value.block == old_location.block) &&
            (value.offset == old_location.offset)) {
          // remove matching (key, value) entry
          container.erase(entry);
        }
      }

      // insert the key, val pair
      container.insert(std::pair<KeyType, ValueType>(index_key1, location));

      return false;
    }

  }

  ItemPointer Exists(const storage::Tuple *key,
                     const ItemPointer location) {
    {
      std::lock_guard<std::mutex> lock(index_mutex);

      index_key1.SetFromKey(key);

      // find the <key, location> pair
      auto entries = container.equal_range(index_key1);
      for (auto entry = entries.first ; entry != entries.second; ++entry) {
        ItemPointer value = entry->second;
        if((value.block == location.block) &&
            (value.offset == location.offset)) {
          return value;
        }
      }

      return INVALID_ITEMPOINTER;
    }
  }

  std::vector<ItemPointer> Scan() {
    std::vector<ItemPointer> result;

    {
      std::lock_guard<std::mutex> lock(index_mutex);

      // scan all entries
      auto itr = container.begin();
      while (itr != container.end()) {
        ItemPointer location = itr->second;
        result.push_back(location);
        itr++;
      }

    }

    return result;
  }

  std::vector<ItemPointer> GetLocationsForKey(
      const storage::Tuple *key) {
    std::vector<ItemPointer> result;

    {
      std::lock_guard<std::mutex> lock(index_mutex);

      index_key1.SetFromKey(key);

      // scan all
      auto itr = container.find(index_key1);
      while (itr != container.end()) {
        result.push_back(itr->second);
        itr++;
        if(equals(itr->first, index_key1) == false)
          break;
      }

    }

    return result;
  }

  std::vector<ItemPointer> GetLocationsForKeyBetween(
      const storage::Tuple *start, const storage::Tuple *end) {
    std::vector<ItemPointer> result;

    {
      std::lock_guard<std::mutex> lock(index_mutex);

      index_key1.SetFromKey(start);
      index_key2.SetFromKey(end);

      // scan all between start and end
      auto start_itr = container.upper_bound(index_key1);
      auto end_itr = container.end();
      while (start_itr != end_itr) {
        result.push_back(start_itr->second);
        start_itr++;
        if(comparator(start_itr->first, index_key2) == false)
          break;
      }
    }

    return result;
  }

  std::vector<ItemPointer> GetLocationsForKeyLT(
      const storage::Tuple *key) {
    std::vector<ItemPointer> result;

    {
      std::lock_guard<std::mutex> lock(index_mutex);

      index_key1.SetFromKey(key);

      // scan all lt
      auto itr = container.begin();
      auto end_itr = container.lower_bound(index_key1);
      while (itr != end_itr) {
        result.push_back(itr->second);
        itr++;
      }

    }

    return result;
  }

  std::vector<ItemPointer> GetLocationsForKeyLTE(
      const storage::Tuple *key) {
    std::vector<ItemPointer> result;

    {
      std::lock_guard<std::mutex> lock(index_mutex);

      index_key1.SetFromKey(key);

      // scan all lt
      auto itr = container.begin();
      auto end_itr = container.lower_bound(index_key1);
      while (itr != end_itr) {
        result.push_back(itr->second);
        itr++;
      }

      // scan all equal
      while (itr != container.end()) {
        result.push_back(itr->second);
        itr++;
        if(equals(itr->first, index_key1) == false)
          break;
      }

    }

    return result;
  }

  std::vector<ItemPointer> GetLocationsForKeyGT(
      const storage::Tuple *key) {
    std::vector<ItemPointer> result;

    {
      std::lock_guard<std::mutex> lock(index_mutex);

      index_key1.SetFromKey(key);

      // scan all gt
      auto itr = container.upper_bound(index_key1);
      auto end_itr = container.end();
      while (itr != end_itr) {
        result.push_back(itr->second);
        itr++;
      }

    }

    return result;
  }

  std::vector<ItemPointer> GetLocationsForKeyGTE(
      const storage::Tuple *key) {
    std::vector<ItemPointer> result;

    {
      std::lock_guard<std::mutex> lock(index_mutex);

      index_key1.SetFromKey(key);

      // scan all gte
      auto itr = container.find(index_key1);
      auto end_itr = container.end();
      while (itr != end_itr) {
        result.push_back(itr->second);
        itr++;
      }

    }

    return result;
  }

  std::vector<ItemPointer> Scan(
      const storage::Tuple *key,
      const std::vector<oid_t>& index_key_columns,
      const ExpressionType expr_type) {
    std::vector<ItemPointer> result;

    {
      std::lock_guard<std::mutex> lock(index_mutex);

      // scan all entries comparing against arbitrary key
      auto itr = container.begin();
      while (itr != container.end()) {
        auto index_key = itr->first;
        auto index_key_tuple = index_key.GetTupleForComparison(metadata->GetKeySchema());
        if(IndexKeyComparator(&index_key_tuple,
                              key,
                              index_key_columns,
                              expr_type) == true) {
          ItemPointer location = itr->second;
          result.push_back(location);
        }
        itr++;
      }

    }

    return result;
  }

  std::string GetTypeName() const { return "BtreeMulti"; }

 protected:

  MapType container;
  KeyType index_key1;
  KeyType index_key2;

  // comparison stuff
  KeyEqualityChecker equals;
  KeyComparator comparator;

  // synch helper
  std::mutex index_mutex;

};

}  // End index namespace
}  // End peloton namespace
