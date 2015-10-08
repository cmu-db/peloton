///===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// cache.h
//
// Identification: src/backend/common/cache.cpp
//
// Copyright (c) 2015, Carnegie Mellon Universitry Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>

#include "backend/common/cache.h"
#include "backend/common/logger.h"
#include "backend/planner/abstract_plan.h"

namespace peloton {

/** @brief the constructor, nothing fancy here
 */
template<class Key, class Value>
Cache<Key, Value>::Cache(size_type capacitry)
    : capacity_(capacitry) {
}

/* @brief find a value cached with key
 *
 * @param key the key associated with the value, if found, this effectively
 *            make it the most recently accessed
 *
 * @return a iterator of this cache, end() if no such entry
 * */
template<class Key, class Value>
typename Cache<Key, Value>::iterator Cache<Key, Value>::find(const Key& key) {
  std::lock_guard<std::mutex> lock(cache_mut_);
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
 *         if the key already exists, this updates its value
 *         if not, this effectively insert a new entry
 *         regardless, the related entry would become the most recent one
 *         If after insertion, the size of the cache exceeds its
 *         capacity, the cache automatically evict the least recent
 *         accessed entry
 *
 *  @param entry a key value pair to be inserted
 *  @return a iterator of the inserted entry
 **/
template<class Key, class Value>
typename Cache<Key, Value>::iterator Cache<Key, Value>::insert(
    const Entry& entry) {
  std::lock_guard<std::mutex> lock(cache_mut_);
  assert(list_.size() == map_.size());
  assert(list_.size() <= this->capacity_);
  auto map_itr = map_.find(entry.first);
  auto cache_itr = iterator(map_itr);

  if (map_itr == map_.end()) {
    /* new key */
    list_.push_front(entry.first);
    auto ret = map_.emplace(entry.first,
                            std::make_pair(entry.second, list_.begin()));
    assert(ret.second); /* should not fail */
    cache_itr = iterator(ret.first);
    while (map_.size() > this->capacity_) {
      auto deleted = list_.back();
      auto count = this->map_.erase(deleted);
      list_.erase(std::prev(list_.end()));
      assert(count == 1);
    }
  } else {
    list_.splice(list_.begin(), list_, map_itr->second.second);
    map_itr->second = std::make_pair(entry.second, list_.begin());
  }
  assert(list_.size() == map_.size());
  assert(list_.size() <= capacity_);
  return cache_itr;
}

/** @brief get the size of the cache
 *    it should always less than or equal to its capacity
 *
 *  @return the size of the cache
 */
template<class Key, class Value>
typename Cache<Key, Value>::size_type Cache<Key, Value>::size() const {
  std::lock_guard<std::mutex> lock(cache_mut_);
  assert(map_.size() == list_.size());
  return map_.size();
}

/** @brief clear the cache
 *
 *  @return Void
 */
template<class Key, class Value>
void Cache<Key, Value>::clear(void) {
  std::lock_guard<std::mutex> lock(cache_mut_);
  list_.clear();
  map_.clear();
}

/** @brief is the cache empty
 *
 *  @return true if empty, false if not
 */
template<class Key, class Value>
bool Cache<Key, Value>::empty(void) const {
  std::lock_guard<std::mutex> lock(cache_mut_);
  return map_.empty();
}

/* Explicit instantiations */
template class Cache<uint32_t, planner::AbstractPlan*>; /* For testing */
template class Cache<std::string, const planner::AbstractPlan*>; /* Actual in use */

}
