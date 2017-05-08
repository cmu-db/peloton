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
#include "codegen/plan_comparator.h"
#include <list>
namespace peloton {
namespace codegen {

/* This is the implementation of query cache that maps an AbstractPlan with a codegen
 * Query using LRU eviction policy. The cache is implemented as a singleton with a list
 * and a map.
*/
class QueryCache {
public:
  static QueryCache& Instance() {
    static  QueryCache instance;
    return instance;
  }

  size_t GetSize() {
    return cache_map.size();
  }

  void ClearCache() {
    cache_map.clear();
  }

  //Clean the cache until the cache size is within target_size with LRU policy
  void CleanCache(int target_size) {
    if (target_size < 0)
      return;
    while((int)cache_map.size() > target_size) {
      auto last_it = query_list.end();
      last_it--;
      cache_map.erase(last_it->first);
      query_list.pop_back();
    }
  }


  Query* FindPlan(const std::shared_ptr<planner::AbstractPlan>& key) {
    auto it = cache_map.find(key);
    if (it == cache_map.end())
      return nullptr;
    
    query_list.splice(query_list.begin(), query_list, it->second);
    return it->second->second.get();
  }

  void InsertPlan(const std::shared_ptr<planner::AbstractPlan>& key,
                  std::unique_ptr<Query> val) {

    query_list.push_front(make_pair(key,std::move(val)));
    cache_map.insert(make_pair(key,query_list.begin()));
    

  }

private:
  //compartor function of AbstractPlan
  struct ComparePlan {

    bool operator()(const  std::shared_ptr<planner::AbstractPlan> & a,
                    const std::shared_ptr<planner::AbstractPlan> & b) const {
      return (codegen::PlanComparator::Compare(* a.get(), * b.get()) < 0);
    }
  };
  

  std::list<std::pair<std::shared_ptr<planner::AbstractPlan>, std::unique_ptr<Query>>> query_list;

  std::map<std::shared_ptr<planner::AbstractPlan>,
            decltype(query_list.begin()), ComparePlan> cache_map;
 
  QueryCache() {}
};
}
}