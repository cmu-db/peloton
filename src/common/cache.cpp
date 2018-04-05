//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cache.cpp
//
// Identification: src/common/cache.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "common/cache.h"

#include "common/statement.h"
#include "common/macros.h"
#include "planner/abstract_plan.h"

namespace peloton {

/** @brief the constructor, nothing fancy here
 */
template <class Key, class Value>
Cache<Key, Value>::Cache(size_type capacity, size_t insert_threshold)
    : capacity_(capacity), insert_threshold_(insert_threshold) {}

/* @brief find a value cached with key
 *
 * @param key the key associated with the value, if found, this effectively
 *            make it the most recently accessed
 *
 * @return a iterator of this cache, end() if no such entry
 * */
template <class Key, class Value>
typename Cache<Key, Value>::iterator Cache<Key, Value>::find(const Key &key) {
  auto map_itr = map_.find(key);
  auto cache_itr = end();
  if (map_itr != map_.end()) {
    /* put this at the front of the list */
    list_.splice(list_.begin(), list_, map_itr->second.second);
    *(map_itr->second.second) = list_.front();
    cache_itr = iterator(map_itr);
  } else {
  }
  return cache_itr;
}

/** @brief insert a key value pair
 *         if the key already exists, this updates its value, the reference
 *         count of the previous value would decrement by 1
 *
 *         if not, this effectively insert a new entry
 *         regardless, the related entry would become the most recent one
 *         If after insertion, the size of the cache exceeds its
 *         capacity, the cache automatically evict the least recent
 *         accessed entry
 *
 *  @param entry a key value pair to be inserted of type std::pair<Key,
 *ValuePtr>
 *  @return a iterator of the inserted entry
 **/
template <class Key, class Value>
typename Cache<Key, Value>::iterator Cache<Key, Value>::insert(
    const Entry &entry) {
  PELOTON_ASSERT(list_.size() == map_.size());
  PELOTON_ASSERT(list_.size() <= this->capacity_);
  auto map_itr = map_.find(entry.first);
  auto cache_itr = iterator(map_itr);

  if (map_itr == map_.end()) {
    /* new key */
    list_.push_front(entry.first);
    auto ret =
        map_.emplace(entry.first, std::make_pair(entry.second, list_.begin()));
    PELOTON_ASSERT(ret.second); /* should not fail */
    cache_itr = iterator(ret.first);
    while (map_.size() > this->capacity_) {
      auto deleted = list_.back();
      this->map_.erase(deleted);
      list_.erase(std::prev(list_.end()));
    }
  } else {
    list_.splice(list_.begin(), list_, map_itr->second.second);
    map_itr->second = std::make_pair(entry.second, list_.begin());
  }
  PELOTON_ASSERT(list_.size() == map_.size());
  PELOTON_ASSERT(list_.size() <= capacity_);
  return cache_itr;
}

/* @brief deletes a key from the cache
 *
 * @param key the key to be deleted
 *
 * */
template <class Key, class Value>
void Cache<Key, Value>::delete_key(const Key &key) {
  auto map_itr = map_.find(key);
  if (map_itr != map_.end()) {
    /* delete this from the list */
    list_.erase(map_itr->second.second);
    this->map_.erase(map_itr);
  }
}

/** @brief get the size of the cache
 *    it should always less than or equal to its capacity
 *
 *  @return the size of the cache
 */
template <class Key, class Value>
typename Cache<Key, Value>::size_type Cache<Key, Value>::size() const {
  PELOTON_ASSERT(map_.size() == list_.size());
  return map_.size();
}

/** @brief clear the cache
 *
 *  @return Void
 */
template <class Key, class Value>
void Cache<Key, Value>::clear(void) {
  list_.clear();
  map_.clear();
  counts_.clear();
  capacity_ = DEFAULT_CACHE_SIZE;
  insert_threshold_ = DEFAULT_CACHE_INSERT_THRESHOLD;
}

/** @brief is the cache empty
 *
 *  @return true if empty, false if not
 */
template <class Key, class Value>
bool Cache<Key, Value>::empty(void) const {
  return map_.empty();
}

/* Explicit instantiations */
template class Cache<uint32_t, const planner::AbstractPlan>; /* For testing */
template class Cache<std::string,
                     const planner::AbstractPlan>; /* Actual in use */

template class Cache<std::string, Statement >;
}
