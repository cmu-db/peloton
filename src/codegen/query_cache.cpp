//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_cache.cpp
//
// Identification: src/codegen/query_cache.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/query_cache.h"
#include "planner/delete_plan.h"
#include "planner/insert_plan.h"
#include "planner/seq_scan_plan.h"
#include "planner/update_plan.h"
#include "storage/data_table.h"

namespace peloton {
namespace codegen {

Query* QueryCache::Find(const std::shared_ptr<planner::AbstractPlan> &ey) {
  auto it = cache_map_.find(key);
  if (it == cache_map_.end()) {
    return nullptr;
  }
  query_list_.splice(query_list_.begin(), query_list_, it->second);
  return it->second->second.get();
}

void QueryCache::Add(const std::shared_ptr<planner::AbstractPlan>& key,
                     std::unique_ptr<Query> val) {
  query_list_.push_front(make_pair(key, std::move(val)));
  cache_map_.insert(make_pair(key, query_list_.begin()));
}

oid_t QueryCache::GetOidFromPlan(const planner::AbstractPlan &plan) {
 switch (plan.GetPlanNodeType()) {
    case PlanNodeType::SEQSCAN: {
      auto &dplan = reinterpret_cast<const planner::SeqScanPlan &>(plan);
      return dplan.GetTable()->GetOid();
    }
    case PlanNodeType::DELETE: {
      auto &dplan = reinterpret_cast<const planner::DeletePlan &>(plan);
      return dplan.GetTable()->GetOid();
    }
    case PlanNodeType::INSERT: {
      auto &dplan = reinterpret_cast<const planner::InsertPlan &>(plan);
      return dplan.GetTable()->GetOid();
    }
    case PlanNodeType::UPDATE: {
      auto &dplan = reinterpret_cast<const planner::UpdatePlan &>(plan);
      return dplan.GetTable()->GetOid();
    }
    default: { break; }
  }
  if (plan.GetChildren().size() == 0)
    return INVALID_OID;
  else
    return GetOidFromPlan(*plan.GetChild(0));
}

void QueryCache::RemoveCache(const oid_t table_oid) {
  for (auto it = cache_map_.begin(); it != cache_map_.end(); ) {
    oid_t oid = GetOidFromPlan(*it->first.get());
    if (oid == table_oid) {
      query_list_.erase(it->second);
      it = cache_map_.erase(it); 
    }
    else {
       ++it;
    }
  }
}

}  // namespace codegen
}  // namespace peloton 
