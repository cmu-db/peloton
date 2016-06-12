//===----------------------------------------------------------------------===//
//
//       Peloton
//
// map.h
//
// Identification: src/include/common/map.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <type_traits>

#include "libcds/cds/init.h"
#include "libcds/cds/urcu/general_buffered.h"
#include "libcds/cds/sync/pool_monitor.h"
#include "libcds/cds/memory/vyukov_queue_pool.h"
#include "libcds/cds/container/bronson_avltree_map_rcu.h"

namespace peloton {

// MAP_TEMPLATE_ARGUMENTS
#define MAP_TEMPLATE_ARGUMENTS template <typename KeyType, \
    typename ValueType>

// MAP_TYPE
#define MAP_TYPE Map<KeyType, ValueType>

template<typename KeyType, typename ValueType>
class Map {
 public:

  // Stores pointers to values
  typedef ValueType* MappedType;

  typedef typename std::remove_pointer<ValueType*>::type FindMappedType;

  // rcu implementation
  typedef cds::urcu::general_buffered<> RCUImpl;

  // rcu type
  typedef cds::urcu::gc<RCUImpl> RCUType;

  Map();
  ~Map();

  // Inserts a item into priority map.
  bool Insert(const KeyType &key, MappedType pVal);

  // Extracts item with high priority
  std::pair<bool, bool> Update(const KeyType &key, MappedType pVal, bool bInsert=true);

  // Delete key from the map
  bool Erase(const KeyType &key);

  // Extracts the corresponding value
  bool Find(const KeyType &key);

  // Checks whether the map contains key
  bool Contains(const KeyType &key);

  // Clears the tree (thread safe, not atomic)
  void Clear();

  // Returns item count in the map
  size_t GetSize() const;

  // Checks if the map is empty
  bool IsEmpty() const;

  // Checks internal consistency (not atomic, not thread-safe)
  bool CheckConsistency() const;

 private:

  // concurrent AVL tree algorithm
  // http://libcds.sourceforge.net/doc/cds-api/
  typedef cds::container::BronsonAVLTreeMap<RCUType, KeyType, MappedType> avl_tree_t;

  avl_tree_t avl_tree;
};

}  // namespace peloton
