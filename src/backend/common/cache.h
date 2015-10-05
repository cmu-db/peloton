///===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// cache.h
//
// Identification: src/backend/common/cache.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <unordered_map>
#include <iterator>

#include "backend/common/synch.h"

namespace peloton {
template<class Key, class Value>
class Cache {
  /* A key value pair */
  typedef std::pair<Key, Value> Entry;

  /* A list of Key in lru order
   *
   * When iterate thru it, we get most recent entry first
   * */
  typedef std::list<Key> KeyList;

  /* pair of Value and interator into the list */
  typedef std::pair<Value, typename KeyList::iterator> IndexedValue;

  typedef std::unordered_map<Key, IndexedValue> Map;
  typedef size_t size_type;

 public:
  Cache(size_type capacity);

  class iterator : public std::iterator<std::input_iterator_tag, Entry> {
    friend class Cache;

   public:
    inline iterator& operator++();
    inline iterator operator++(int);
    inline bool operator==(const iterator &rhs);
    inline bool operator!=(const iterator &rhs);
    inline Value& operator*();


   private:
    inline iterator(typename Map::iterator map_it);

    typename Map::iterator map_it_;
  };

  iterator find(const Key& key);

  iterator insert(const Entry& val);

  size_type size() const;

  inline iterator begin();
  inline iterator end();

 private:
  Map map_;
  KeyList list_;
  size_type capacity_;
  RWLock cache_lock_;

};

template<class Key, class Value>
inline Cache<Key, Value>::iterator::iterator(
    typename Cache<Key, Value>::Map::iterator map_it)
    : map_it_(map_it) {
}

template<class Key, class Value>
inline typename Cache<Key, Value>::iterator& Cache<Key, Value>::iterator::operator++() {
  this->map_it_++;
  return *this;
}

template<class Key, class Value>
inline typename Cache<Key, Value>::iterator Cache<Key, Value>::iterator::operator++(int) {
  Cache<Key, Value>::iterator tmp(*this);
  operator++();
  return tmp;
}

template<class Key, class Value>
inline bool Cache<Key, Value>::iterator::operator==(const typename Cache<Key, Value>::iterator &rhs) {
  return this->map_it_ == rhs.map_it_;
}

template<class Key, class Value>
inline bool Cache<Key, Value>::iterator::operator!=(const typename Cache<Key, Value>::iterator &rhs) {
  return this->map_it_ != rhs.map_it_;
}

template<class Key, class Value>
inline Value& Cache<Key, Value>::iterator::operator*() {
  return map_it_->second.first;
}

template<class Key, class Value>
inline typename Cache<Key, Value>::iterator Cache<Key, Value>::begin() {
  return iterator(this->map_.begin());
}

template<class Key, class Value>
inline typename Cache<Key, Value>::iterator Cache<Key, Value>::end() {
  return iterator(this->map_.end());
}


}
