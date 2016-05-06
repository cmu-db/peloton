//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// backend_stats_context.cpp
//
// Identification: src/backend/statistics/backend_stats_context.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <mutex>
#include <map>
#include <vector>

#include "backend/common/types.h"
#include "backend/index/index.h"
#include "backend/statistics/backend_stats_context.h"
#include "backend/statistics/stats_aggregator.h"
#include "backend/storage/database.h"

//===--------------------------------------------------------------------===//
// GUC Variables
//===--------------------------------------------------------------------===//


namespace peloton {
namespace stats {


BackendStatsContext::BackendStatsContext() {
  std::thread::id this_id = std::this_thread::get_id();
  thread_id = this_id;
}

BackendStatsContext::~BackendStatsContext() {
  //peloton::stats::StatsAggregator::GetInstance().UnregisterContext(thread_id);
}

void BackendStatsContext::Aggregate(BackendStatsContext &source_) {
  for (auto& database_item : source_.database_metrics_) {
    GetDatabaseMetric(database_item.first)->Aggregate(*database_item.second);
  }

  for (auto access_item : source_.table_metrics_) {
    GetTableMetric(access_item.second->GetDatabaseId(), access_item.second->GetTableId())
        ->Aggregate(*access_item.second);
  }

  for (auto access_item : source_.index_metrics_) {
    GetIndexMetric(access_item.second->GetDatabaseId(), access_item.second->GetTableId(),
        access_item.second->GetIndexId())->Aggregate(*access_item.second);
  }
}


}  // namespace stats
}  // namespace peloton
