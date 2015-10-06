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

template<class Key, class Value>
Cache<Key, Value>::Cache(size_type capacitry)
    : capacity_(capacitry) {
}

template<class Key, class Value>
typename Cache<Key, Value>::iterator Cache<Key, Value>::find(const Key& key) {
  {
    cache_lock_.WriteLock();
    auto map_itr = map_.find(key);
    auto cache_itr = end();
    if (map_itr != map_.end()) {
        /* put this at the front of the list */
        list_.splice(list_.begin(), list_, map_itr->second.second);
        *(map_itr->second.second) = list_.front();
        cache_itr = iterator(map_itr);
      LOG_INFO("Found 1 record");
    } else {
      LOG_INFO("Found 0 record");
    }
    cache_lock_.Unlock();
    return cache_itr;
  }
}

template<class Key, class Value>
typename Cache<Key, Value>::iterator Cache<Key, Value>::insert(
    const Entry& entry) {
  {
    cache_lock_.WriteLock();
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
      LOG_INFO("Insert %d", entry.first);
      while (map_.size() > this->capacity_) {
        auto deleted = list_.back();
        auto count = this->map_.erase(deleted);
        list_.erase(std::prev(list_.end()));
        LOG_INFO("Evicted %d", deleted);
        assert(count == 1);
      }
    } else {
      list_.splice(list_.begin(), list_, map_itr->second.second);
      map_itr->second = std::make_pair(entry.second, list_.begin());
      LOG_INFO("Updated 1 record");
    }
    assert(list_.size() == map_.size());
    assert(list_.size() <= capacity_);
    cache_lock_.Unlock();
    return cache_itr;
  }
}

template<class Key, class Value>
typename Cache<Key, Value>::size_type Cache<Key, Value>::size() const {
  assert(map_.size() == list_.size());
  return map_.size();
}

template class Cache<uint32_t, planner::AbstractPlan*> ;

}
