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

Query* QueryCache::Find(const std::shared_ptr<planner::AbstractPlan> &key) {
  cache_lock_.ReadLock();
  auto it = cache_map_.find(key);
  if (it == cache_map_.end()) {
    cache_lock_.Unlock();
    return nullptr;
  }
  query_list_.splice(query_list_.begin(), query_list_, it->second);
  auto *query = it->second->second.get();
  cache_lock_.Unlock();
  return query;
}

void QueryCache::Add(const std::shared_ptr<planner::AbstractPlan> &key,
                     std::unique_ptr<Query> &&val) {
  cache_lock_.WriteLock();
  query_list_.push_front(make_pair(key, std::move(val)));
  cache_map_.insert(make_pair(key, query_list_.begin()));
  cache_lock_.Unlock();
}

void QueryCache::Clear() {
  cache_lock_.WriteLock();
  cache_map_.clear();
  query_list_.clear();
  cache_lock_.Unlock();
}

void QueryCache::Remove(const oid_t table_oid) {
  cache_lock_.WriteLock();

  for (auto it = cache_map_.begin(); it != cache_map_.end(); ) {
    oid_t oid = GetOidFromPlan(*it->first.get());
    if (oid == table_oid) {
      query_list_.erase(it->second);
      it = cache_map_.erase(it);
    } else {
      ++it;
    }
  }
  cache_lock_.Unlock();
}

void QueryCache::Resize(size_t target_size) {
  cache_lock_.WriteLock();
  while (cache_map_.size() > target_size) {
    auto last_it = query_list_.end();
    last_it--;
    cache_map_.erase(last_it->first);
    query_list_.pop_back();
  }
  capacity_ = target_size;
  cache_lock_.Unlock();
}

oid_t QueryCache::GetOidFromPlan(const planner::AbstractPlan &plan) const {
 switch (plan.GetPlanNodeType()) {
    case PlanNodeType::SEQSCAN: {
      auto &dplan = static_cast<const planner::SeqScanPlan &>(plan);
      return dplan.GetTable()->GetOid();
    }
    case PlanNodeType::DELETE: {
      auto &dplan = static_cast<const planner::DeletePlan &>(plan);
      return dplan.GetTable()->GetOid();
    }
    case PlanNodeType::INSERT: {
      auto &dplan = static_cast<const planner::InsertPlan &>(plan);
      return dplan.GetTable()->GetOid();
    }
    case PlanNodeType::UPDATE: {
      auto &dplan = static_cast<const planner::UpdatePlan &>(plan);
      return dplan.GetTable()->GetOid();
    }
    default: { break; }
  }
  if (plan.GetChildren().size() == 0) {
    return INVALID_OID;
  } else {
    return GetOidFromPlan(*plan.GetChild(0));
  }
}

}  // namespace codegen
}  // namespace peloton 
