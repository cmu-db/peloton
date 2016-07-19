//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// skip_list_map.h
//
// Identification: src/include/container/skip_list_map.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <cstdlib>
#include <cstring>
#include <cstdio>

#include "libcds/cds/container/skip_list_map_nogc.h"

namespace peloton {

// SKIP_LIST_MAP_TEMPLATE_ARGUMENTS
#define SKIP_LIST_MAP_TEMPLATE_ARGUMENTS template <typename KeyType, \
    typename ValueType, typename KeyComparator>

// SKIP_LIST_MAP_TYPE
#define SKIP_LIST_MAP_TYPE SkipListMap<KeyType, ValueType, KeyComparator>

SKIP_LIST_MAP_TEMPLATE_ARGUMENTS
class SkipListMap {
 public:

  SkipListMap();
  ~SkipListMap();

  struct skip_list_map_traits: public cds::container::skip_list::traits {
    typedef KeyComparator compare;

  };

  // concurrent skip list algorithm
  // http://libcds.sourceforge.net/doc/cds-api/
  typedef cds::container::SkipListMap<cds::gc::nogc, KeyType, ValueType, skip_list_map_traits> skip_list_map_t;

  // map pair <key, value>
  typedef typename skip_list_map_t::value_type map_pair;

  typedef typename skip_list_map_t::iterator map_iterator;

  typedef typename skip_list_map_t::const_iterator map_const_iterator;

  // Inserts a item
  bool Insert(const KeyType &key, ValueType value);

  // Updates the key
  bool Update(const KeyType &key, ValueType value, const bool bInsert = true);

  // Extracts the corresponding value
  bool Find(const KeyType &key, ValueType& value);

  // Delete key from the skip_list_map
  bool Erase(const KeyType &key);

  // Checks whether the skip_list_map contains key
  map_iterator Contains(const KeyType &key) {
    return skip_list_map.contains(key);
  }

  // Clears the tree (thread safe, not atomic)
  void Clear();

  // Returns item count in the skip_list_map
  size_t GetSize() const;

  // Checks if the skip_list_map is empty
  bool IsEmpty() const;

 public:
  // Iterators

  map_iterator begin(){
    return skip_list_map.begin();
  }

  map_const_iterator begin() const{
    return skip_list_map.begin();
  }

  map_const_iterator cbegin() const{
    return skip_list_map.cbegin();
  }

  map_iterator end(){
    return skip_list_map.end();
  }

  map_const_iterator end() const{
    return skip_list_map.end();
  }

  map_const_iterator cend() const{
    return skip_list_map.cend();
  }

  private:

  skip_list_map_t skip_list_map;

};

}  // namespace peloton
