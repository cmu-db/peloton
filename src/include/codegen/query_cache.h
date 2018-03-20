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
#include "common/synchronization/readwrite_latch.h"
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
  // Find the cached query object with the given plan
  Query *Find(const std::shared_ptr<planner::AbstractPlan> &key);

  // Add a plan and a query object to the cache
  void Add(const std::shared_ptr<planner::AbstractPlan> &key,
           std::unique_ptr<Query> &&val);

  // Remove all the items in the cache
  void Clear();

  // Remove all the cached query items related to a table
  void Remove(const oid_t table_oid);

  // Get the number of queries currently cached
  size_t GetCount() const { return cache_map_.size(); }

  // Get the total capacity of the cache, i.e. max. no. of queries to be cached
  size_t GetCapacity() const { return capacity_; }

  // Set the total capacity of the cache
  void SetCapacity(size_t capacity) { Resize(capacity); }

 private:
  friend class Singleton<QueryCache>;

  QueryCache() {}

  // Resize the cache in the LRU manner
  void Resize(size_t target_size);

  // Get the table Oid from the plan given
  oid_t GetOidFromPlan(const planner::AbstractPlan &plan) const;

 private:
  std::list<std::pair<std::shared_ptr<planner::AbstractPlan>,
                      std::unique_ptr<Query>>> query_list_;

  std::unordered_map<std::shared_ptr<planner::AbstractPlan>,
                     decltype(query_list_.begin()), planner::Hash,
                     planner::Equal> cache_map_;

  common::synchronization::ReadWriteLatch cache_lock_;

  size_t capacity_ = 0;
};

}  // namespace codegen
}  // namespace peloton
