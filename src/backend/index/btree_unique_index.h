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
  typedef std::map<KeyType, ValueType, KeyComparator> MapType;

 public:
  BtreeUniqueIndex(IndexMetadata *metadata)
 : Index(metadata),
   container(KeyComparator(metadata)),
   equals(metadata),
   comparator(metadata){
  }

  ~BtreeUniqueIndex() {
  }

  bool InsertEntry(const storage::Tuple *key,
                   const ItemPointer location) {
    {
      std::lock_guard<std::mutex> lock(index_mutex);
      index_key1.SetFromKey(key);

      // Insert the key, val pair
      auto status = container.insert(std::pair<KeyType, ValueType>(index_key1, location));
      return status.second;
    }
  }

  bool DeleteEntry(const storage::Tuple *key,
                   const ItemPointer location) {
    {
      std::lock_guard<std::mutex> lock(index_mutex);
      index_key1.SetFromKey(key);

      // Delete the < key, location > pair
      auto status = container.erase(index_key1);
      return status;
    }
  }

  bool UpdateEntry(const storage::Tuple *key,
                   const ItemPointer location,
                   const ItemPointer old_location) {

    {
      std::lock_guard<std::mutex> lock(index_mutex);
      index_key1.SetFromKey(key);

      // Check for <key, old location> first
      if(container.count(index_key1) != 0) {
        ItemPointer old_loc = container[index_key1];
        if((old_loc.block == old_location.block) &&
            (old_loc.offset == old_location.offset))  {
          container[index_key1] = location;
          return true;
        }
      }

      return false;
    }

  }

  ItemPointer Exists(const storage::Tuple *key,
                     const ItemPointer location) {
    {
      std::lock_guard<std::mutex> lock(index_mutex);
      index_key1.SetFromKey(key);

      // find the key, location pair
      auto container_itr = container.find(index_key1);
      if(container_itr != container.end()) {
        return container_itr->second;
      }

      return INVALID_ITEMPOINTER;
    }
  }

  std::vector<ItemPointer> Scan(
      const std::vector<Value>& values,
      const std::vector<oid_t>& key_column_ids,
      const std::vector<ExpressionType>& expr_types) {
    std::vector<ItemPointer> result;

    {
      std::lock_guard<std::mutex> lock(index_mutex);

      // check if we have leading column equality
      oid_t leading_column_id = 0;
      auto key_column_ids_itr = std::find(key_column_ids.begin(), key_column_ids.end(),
                                          leading_column_id);
      bool special_case = false;
      if(key_column_ids_itr != key_column_ids.end()) {
        auto offset = std::distance(key_column_ids.begin(), key_column_ids_itr);
        if(expr_types[offset] == EXPRESSION_TYPE_COMPARE_EQ){
          special_case = true;
          LOG_INFO("Special case");
        }
      }

      auto itr = container.begin();
      storage::Tuple *start_key = nullptr;

      // start scanning from upper bound if possible
      if(special_case == true) {
        start_key = new storage::Tuple(metadata->GetKeySchema(), true);
        // set the lower bound tuple
        SetLowerBoundTuple(start_key, values, key_column_ids, expr_types);
        index_key1.SetFromKey(start_key);
        itr = container.upper_bound(index_key1);
      }

      // scan all entries comparing against arbitrary key
      while (itr != container.end()) {
        auto index_key = itr->first;
        auto tuple = index_key.GetTupleForComparison(metadata->GetKeySchema());

        if(Compare(tuple,
                   key_column_ids,
                   expr_types,
                   values) == true) {
          ItemPointer location = itr->second;
          result.push_back(location);
        }
        itr++;
      }

      delete start_key;
    }

    return result;
  }


  std::string GetTypeName() const { return "BtreeMap"; }

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
