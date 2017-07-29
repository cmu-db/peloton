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
#include "planner/abstract_plan.h"

namespace peloton {
namespace codegen {

// Query cache implementation that maps an AbstractPlan with a CodeGen query
// using LRU eviction policy. The cache is implemented as a singleton.
// Potential future enhancements:
//   1) Apply other interesting eviction policies
//     e.g. Keep some heavy compilation workloads by mixing policies
//   2) Manually keep some of the compiled results in the cache
//   3) Configure the cache size
//   4) Persistency may increase the performance when rebooted
class QueryCache {
 public:
  static QueryCache& Instance() {
    static QueryCache query_cache;
    return query_cache;
  }

  Query* Find(const std::shared_ptr<planner::AbstractPlan>& key);

  void Add(const std::shared_ptr<planner::AbstractPlan>& key,
           std::unique_ptr<Query> val);

  size_t GetCacheSize() { return max_size_; }

  void SetCacheSize(size_t max_size) {
    ResizeCache(max_size);
    max_size_ = max_size;
  }

  size_t GetSize() { return cache_map_.size(); }

  void ClearCache() { cache_map_.clear(); }

  void RemoveCache(const oid_t table_oid);

 private:
  // Can't consturct
  QueryCache() {}

  // Compartor function of AbstractPlan
  struct Compare {
    bool operator()(const std::shared_ptr<planner::AbstractPlan>& a,
                    const std::shared_ptr<planner::AbstractPlan>& b) const {
      return !(*a.get() == *b.get());
    }
  };

  void ResizeCache(size_t target_size) {
    while (cache_map_.size() > target_size) {
      auto last_it = query_list_.end();
      last_it--;
      cache_map_.erase(last_it->first);
      query_list_.pop_back();
    }
  }

 private:
  std::list<std::pair<std::shared_ptr<planner::AbstractPlan>,
                      std::unique_ptr<Query>>> query_list_;

  std::map<std::shared_ptr<planner::AbstractPlan>,
           decltype(query_list_.begin()), Compare> cache_map_;

  size_t max_size_ = 0;
};

}  // namespace codegen
}  // namespace peloton 
