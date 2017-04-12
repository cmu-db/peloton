//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// value_peeker_proxy.h
//
// Identification: src/include/codegen/cache.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once
namespace peloton {
namespace codegen {
class QueryCache {
public:
  static QueryCache& Instance() {
    static  QueryCache instance;
    return instance;
  }
  bool FindPlan(const planner::AbstractPlan & key) {
    auto it = cache.find(key);
    if (it != cache.end())
      return false;
    return true;
  }

  Query GetQuery(const planenr::AbstractPlan & key) {
    return cache.find(key)->second;
  }

  void InsertPlan(const planner::AbstractPlan & key, const Query & val) {
    cache.insert(key, val);
  }

private:
  struct ComparePlan {
    bool operator()(const  planner::AbstractPlan & a, const planner::AbstractPlan & b) const {
      return (codegen::PlanComparator::Compare(A, B) < 0);
    }
  };
  std::map<planner::AbstractPlan, Query, ComparePlan> cache;
  QueryCache() {}
};
}
}