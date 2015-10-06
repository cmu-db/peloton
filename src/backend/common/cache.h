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
#include <mutex>


namespace peloton {
template<class Key, class Value>
class Cache {
  /* A key value pair */
  typedef std::pair<Key, Value> Entry;

  /* A list of Key in LRU order
   *
   * When iterate thru it, we get most recent entry first
   * i.e.
   * list.front() gives you the most recent entry
   * */
  typedef std::list<Key> KeyList;

  /* pair of Value and iterator into the list */
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
    inline iterator(typename Map::iterator map_itr);

    /* delegates to a map iterator */
    typename Map::iterator map_itr_;
  };

  iterator find(const Key& key);

  iterator insert(const Entry& kv);

  size_type size(void) const;

  bool empty(void) const;

  void clear(void);

  inline iterator begin();
  inline iterator end();

 private:
  Map map_;
  KeyList list_;
  size_type capacity_;
  mutable std::mutex cache_mut_;

};

/** @brief cache iterator constructor
 *  @param a Map iterator
 *
 * It mostly delegates to the underlying map iterator
 */
template<class Key, class Value>
inline Cache<Key, Value>::iterator::iterator(
    typename Cache<Key, Value>::Map::iterator map_itr)
    : map_itr_(map_itr) {
}

/** @brief increment operator
 *  @return iterator after increment
 */
template<class Key, class Value>
inline typename Cache<Key, Value>::iterator& Cache<Key, Value>::iterator::operator++() {
  this->map_itr_++;
  return *this;
}

/** @brief increment operator
 *  @return iterator before increment
 */
template<class Key, class Value>
inline typename Cache<Key, Value>::iterator Cache<Key, Value>::iterator::operator++(int) {
  Cache<Key, Value>::iterator tmp(*this);
  operator++();
  return tmp;
}
/**
 * @brief Equality operator.
 * @param rhs The iterator to compare to.
 *
 * @return True if equal, false otherwise.
 */
template<class Key, class Value>
inline bool Cache<Key, Value>::iterator::operator==(const typename Cache<Key, Value>::iterator &rhs) {
  return this->map_itr_ == rhs.map_itr_;
}

/**
 * @brief Inequality operator.
 * @param rhs The iterator to compare to.
 *
 * @return False if equal, true otherwise.
 */
template<class Key, class Value>
inline bool Cache<Key, Value>::iterator::operator!=(const typename Cache<Key, Value>::iterator &rhs) {
  return this->map_itr_ != rhs.map_itr_;
}

/**
 * @brief Dereference operator.
 *
 * @return A value type
 */
template<class Key, class Value>
inline Value& Cache<Key, Value>::iterator::operator*() {
  return map_itr_->second.first;
}

/** @brief Return the iterator points to the first kv pair of the cache
 *
 *  This is unordered
 *
 *  @return the first kv pair of the cache
 */
template<class Key, class Value>
inline typename Cache<Key, Value>::iterator Cache<Key, Value>::begin() {
  return iterator(this->map_.begin());
}

/* @brief Return the iterator points to the past-the-end element
 *
 * @return a iterator that indicating we are out of range
 * */
template<class Key, class Value>
inline typename Cache<Key, Value>::iterator Cache<Key, Value>::end() {
  return iterator(this->map_.end());
}


}
