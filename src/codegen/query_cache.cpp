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
#include "planner/update_plan.h"
#include "planner/seq_scan_plan.h"
#include "storage/data_table.h"

namespace peloton {
namespace codegen {

Query* QueryCache::Find(const std::shared_ptr<planner::AbstractPlan>& key) {
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

void QueryCache::RemoveCache(const oid_t table_oid) {
  for (auto it = cache_map_.begin(); it != cache_map_.end(); ) {
    oid_t oid = INVALID_OID;
    switch (it->first->GetPlanNodeType()) {
      case PlanNodeType::SEQSCAN: {
        auto *plan =
            reinterpret_cast<const planner::SeqScanPlan *>(it->first.get());
        oid = plan->GetTable()->GetOid();
        break;
      }
      case PlanNodeType::DELETE: {
        auto *plan =
            reinterpret_cast<const planner::DeletePlan *>(it->first.get());
        oid = plan->GetTable()->GetOid();
        break;
      }
      case PlanNodeType::INSERT: {
        auto *plan =
            reinterpret_cast<const planner::InsertPlan *>(it->first.get());
        oid = plan->GetTable()->GetOid();
        break;
      }
      case PlanNodeType::UPDATE: {
        auto *plan =
            reinterpret_cast<const planner::UpdatePlan *>(it->first.get());
        oid = plan->GetTable()->GetOid();
        break;
      }
      default: { break; }
    }
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
