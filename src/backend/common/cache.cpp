///===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// cache.h
//
// Identification: src/backend/common/cache.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>

#include "backend/common/cache.h"
#include "backend/planner/abstract_plan.h"

namespace peloton {

template<class Key, class Value>
Cache<Key, Value>::Cache(size_type capacity)
    : capacity_(capacity) {
}

template<class Key, class Value>
typename Cache<Key, Value>::iterator Cache<Key, Value>::find(const Key& key) {
  cache_lock_.ReadLock();
  auto map_it = map_.find(key);
  if (map_it != map_.end()) {
    /* put this at the front of the list */
    list_.splice(list_.begin(), list_, map_it->second.second);
    *(map_it->second.second) = list_.front();
    Cache<Key, Value>::iterator ret(map_it);
    cache_lock_.Unlock();
    return ret;
  } else {
    cache_lock_.Unlock();
    return this->end();
  }
}

template<class Key, class Value>
typename Cache<Key, Value>::iterator Cache<Key, Value>::insert(
    const Entry& entry) {
  cache_lock_.WriteLock();
  assert(list_.size() == map_.size());
  assert(list_.size() <= this->capacity_);
  auto cache_it = this->find(entry.first);

  if (cache_it == this->end()) {
    /* new key */
    list_.push_front(entry.first);
    auto map_it = map_.emplace(entry.first, std::make_pair(entry.second, list_.begin()));
    assert(map_it.second); /* should not fail */
    cache_it = iterator(map_it.first);
    while (map_.size() > this->capacity_) {
      auto deleted = list_.back();
      auto count = this->map_.erase(deleted);
      assert(count == 1);
    }
  } else {
    /* update value associated with the key
     * Order has been updated in Cache.find() */
    *cache_it = entry.second;
  }
  assert(list_.size() == map_.size());
  assert(list_.size() <= capacity_);
  cache_lock_.Unlock();
  return cache_it;
}

template<class Key, class Value>
typename Cache<Key, Value>::size_type Cache<Key, Value>::size() const {
  assert(map_.size() == list_.size());
  return map_.size();
}

template class Cache<uint32_t, planner::AbstractPlan*>;

}
