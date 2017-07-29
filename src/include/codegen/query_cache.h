//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_cache.h
//
// Identification: src/include/codegen/query_cache.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include "codegen/query.h"
#include "common/singleton.h"
#include "planner/abstract_plan.h"

namespace peloton {
namespace codegen {

// Query cache implementation that maps an AbstractPlan with a CodeGen query
// using LRU eviction policy. The cache is implemented as a singleton.
// Potential enhancements (major):
//   1) Persistency may increase the performance when rebooted
//   2) Apply other eviction policies
//     e.g. Keep some heavy compilation workloads by mixing policies
//   3) Have a cache per table
// Potential enhancements (minor):
//   1) Manually keep some of the compiled results in the cache
//   2) Configure the cache size
class QueryCache : public Singleton<QueryCache> {
 public:
  Query* Find(const std::shared_ptr<planner::AbstractPlan> &key);

  void Add(const std::shared_ptr<planner::AbstractPlan> &key,
           std::unique_ptr<Query> &&val);

  size_t GetSize() { return size_; }

  void SetSize(size_t size) {
    Resize(size);
    size_ = size;
  }

  size_t GetCount() { return cache_map_.size(); }

  void Clear() {
    cache_map_.clear();
    query_list_.clear();
  }

  void Remove(const oid_t table_oid);

  void RemoveCache(const oid_t table_oid);

 private:
  friend class Singleton<QueryCache>;

  QueryCache() {}

  void Resize(size_t target_size) {
    while (cache_map_.size() > target_size) {
      auto last_it = query_list_.end();
      last_it--;
      cache_map_.erase(last_it->first);
      query_list_.pop_back();
    }
  }

  oid_t GetOidFromPlan(const planner::AbstractPlan &plan);

 private:
  std::list<std::pair<std::shared_ptr<planner::AbstractPlan>,
                      std::unique_ptr<Query>>> query_list_;

  std::unordered_map<std::shared_ptr<planner::AbstractPlan>,
           decltype(query_list_.begin()),
           planner::Hash, planner::Equal> cache_map_;

  size_t size_ = 0;
};

}  // namespace codegen
}  // namespace peloton 
