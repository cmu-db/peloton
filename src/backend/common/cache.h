//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cache.h
//
// Identification: src/backend/common/cache.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <unordered_map>
#include <iterator>
#include <mutex>
#include <memory>

#define DEFAULT_CACHE_SIZE 100
#define DEFAULT_CACHE_INSERT_THRESHOLD 3

namespace peloton {
template <class Key, class Value>

/** @brief An implementation of LRU cache
 *
 *  To use this class, make a explicit instantiation at the end of cache.cpp
 *  The two template parameters are Key and Value, but the cache actually take
 *  a pair of (key, ValuePtr) on insert, as the responsibility of managing
 *  the allocated memory for Value would be taken by the Cache class.
 *
 * */
class Cache {
  /* Raw pointer of Value type */
  typedef Value *RawValuePtr;

  /* Shared pointer of Value type */
  typedef std::shared_ptr<Value> ValuePtr;

  /* A key value pair */
  typedef std::pair<Key, ValuePtr> Entry;

  /* A list of Key in LRU order
   *
   * When iterate thru it, we get most recent entry first
   * i.e.
   * list.front() gives you the most recent entry
   * */
  typedef std::list<Key> KeyList;

  /* pair of Value and iterator into the list */
  typedef std::pair<ValuePtr, typename KeyList::iterator> IndexedValue;

  typedef std::unordered_map<Key, IndexedValue> Map;
  typedef std::unordered_map<Key, size_t> CountMap;
  typedef size_t size_type;

 public:
  Cache(const Cache &) = delete;
  Cache &operator=(const Cache &) = delete;
  Cache(Cache &&) = delete;
  Cache &operator=(Cache &&) = delete;

  explicit Cache(size_type capacity = DEFAULT_CACHE_SIZE,
                 size_t insert_threshold = DEFAULT_CACHE_INSERT_THRESHOLD);

  class iterator : public std::iterator<std::input_iterator_tag, ValuePtr> {
    friend class Cache;

   public:
    inline iterator &operator++();
    inline iterator operator++(int);
    inline bool operator==(const iterator &rhs);
    inline bool operator!=(const iterator &rhs);
    inline ValuePtr operator*();

   private:
    inline iterator(typename Map::iterator map_itr);

    /* delegates to a map iterator */
    typename Map::iterator map_itr_;
  };

  iterator find(const Key &key);

  iterator insert(const Entry &kv);

  size_type size(void) const;

  bool empty(void) const;

  void clear(void);

  inline iterator begin();
  inline iterator end();

 private:
  Map map_;
  KeyList list_;
  CountMap counts_;
  size_type capacity_;
  size_t insert_threshold_;
};

/** @brief cache iterator constructor
 *  @param a Map iterator
 *
 * It mostly delegates to the underlying map iterator
 */
template <class Key, class Value>
inline Cache<Key, Value>::iterator::iterator(
    typename Cache<Key, Value>::Map::iterator map_itr)
    : map_itr_(map_itr) {}

/** @brief increment operator
 *  @return iterator after increment
 */
template <class Key, class Value>
inline typename Cache<Key, Value>::iterator &Cache<Key, Value>::iterator::
operator++() {
  this->map_itr_++;
  return *this;
}

/** @brief increment operator
 *  @return iterator before increment
 */
template <class Key, class Value>
inline typename Cache<Key, Value>::iterator Cache<Key, Value>::iterator::
operator++(int) {
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
template <class Key, class Value>
inline bool Cache<Key, Value>::iterator::operator==(
    const typename Cache<Key, Value>::iterator &rhs) {
  return this->map_itr_ == rhs.map_itr_;
}

/**
 * @brief Inequality operator.
 * @param rhs The iterator to compare to.
 *
 * @return False if equal, true otherwise.
 */
template <class Key, class Value>
inline bool Cache<Key, Value>::iterator::operator!=(
    const typename Cache<Key, Value>::iterator &rhs) {
  return this->map_itr_ != rhs.map_itr_;
}

/**
 * @brief Dereference operator.
 *
 * @return A shared pointer of the ValueType
 */
template <class Key, class Value>
inline typename Cache<Key, Value>::ValuePtr Cache<Key, Value>::iterator::
operator*() {
  return map_itr_->second.first;
}

/** @brief Return the iterator points to the first kv pair of the cache
 *
 *  This is unordered
 *
 *  @return the first kv pair of the cache
 */
template <class Key, class Value>
inline typename Cache<Key, Value>::iterator Cache<Key, Value>::begin() {
  return iterator(this->map_.begin());
}

/* @brief Return the iterator points to the past-the-end element
 *
 * @return a iterator that indicating we are out of range
 * */
template <class Key, class Value>
inline typename Cache<Key, Value>::iterator Cache<Key, Value>::end() {
  return iterator(this->map_.end());
}
}
