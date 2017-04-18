//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// value_peeker_proxy.h
//
// Identification: src/include/codegen/query_cache.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once
#include "codegen/plan_comparator.h"
namespace peloton {
namespace codegen {
class QueryCache {
public:
  static QueryCache& Instance() {
    static  QueryCache instance;
    return instance;
  }

  //Test needed
  planner::AbstractPlan* GetFirst() {
    LOG_DEBUG("cache size %d", int(cache.size()));
    return cache.begin()->first.get();
  }

  void ClearCache() {
    cache.clear();
  }

  bool FindPlan(planner::AbstractPlan& key) {
    auto it = cache.find(key.Copy());
    if (it == cache.end())
      return false;
    return true;
  }

  Query* GetQuery(const planner::AbstractPlan& key) {
    return cache.find(key.Copy())->second.get();
  }

  void InsertPlan(planner::AbstractPlan &key,
                  std::unique_ptr<Query> val) {

    int compare = codegen::PlanComparator::Compare(key, * key.Copy().get());
    LOG_DEBUG("insert compare result: %d\n", compare);
    cache.insert(std::pair<std::unique_ptr<planner::AbstractPlan>,
    std::unique_ptr<Query>>(key.Copy(),std::move(val)));
  }



private:
  struct ComparePlan {

    bool operator()(const  std::unique_ptr<planner::AbstractPlan> & a,
                    const std::unique_ptr<planner::AbstractPlan> & b) const {
      return (codegen::PlanComparator::Compare(* a.get(), * b.get()) < 0);
    }
  };
  std::map<std::unique_ptr<planner::AbstractPlan>,
           std::unique_ptr<Query>, ComparePlan> cache;
  QueryCache() {}
};
}
}