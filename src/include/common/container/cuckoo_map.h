//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cuckoo_map.h
//
// Identification: src/include/container/cuckoo_map.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <cstdlib>
#include <cstring>
#include <cstdio>

#include "libcuckoo/cuckoohash_map.hh"

namespace peloton {

// CUCKOO_MAP_TEMPLATE_ARGUMENTS
#define CUCKOO_MAP_TEMPLATE_ARGUMENTS template <typename KeyType, \
    typename ValueType>

// CUCKOO_MAP_TYPE
#define CUCKOO_MAP_TYPE CuckooMap<KeyType, ValueType>

// Iterator type
#define CUCKOO_MAP_ITERATOR_TYPE \
typename cuckoohash_map<KeyType, ValueType>::locked_table

CUCKOO_MAP_TEMPLATE_ARGUMENTS
class CuckooMap {
 public:

  CuckooMap();
  ~CuckooMap();

  // Inserts a item
  bool Insert(const KeyType &key, ValueType value);

  // Extracts item with high priority
  bool Update(const KeyType &key, ValueType value);

  // Extracts the corresponding value
  bool Find(const KeyType &key, ValueType &value) const;

  // Delete key from the cuckoo_map
  bool Erase(const KeyType &key);

  // Checks whether the cuckoo_map contains key
  bool Contains(const KeyType &key);

  // Clears the tree (thread safe, not atomic)
  void Clear();

  // Returns item count in the cuckoo_map
  size_t GetSize() const;

  // Checks if the cuckoo_map is empty
  bool IsEmpty() const;

  // Lock the table and get iterator
  // The table would be unlock when the iterator
  // is out of scope
  CUCKOO_MAP_ITERATOR_TYPE
  GetIterator();

 private:

  // cuckoo map
  typedef cuckoohash_map<KeyType, ValueType> cuckoo_map_t;

  cuckoo_map_t cuckoo_map;
};

}  // namespace peloton
