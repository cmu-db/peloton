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
namespace peloton {
namespace codegen {
class QueryCache {
public:
  static QueryCache& Instance() {
    static  QueryCache instance;
    return instance;
  }

  size_t GetSize() {
    LOG_DEBUG("cache size %d", int(cache.size()));
    return cache.size();
  }

  void ClearCache() {
    cache.clear();
  }

  Query* FindPlan(std::unique_ptr<planner::AbstractPlan> && key) {
    auto it = cache.find(key);
    if (it == cache.end())
      return nullptr;
    return it->second.get();
  }

  void InsertPlan(std::unique_ptr<planner::AbstractPlan>&& key,
                  std::unique_ptr<Query> val) {


    cache.insert(std::pair<std::unique_ptr<planner::AbstractPlan>,
      std::unique_ptr<Query>>(std::move(key),std::move(val)));
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