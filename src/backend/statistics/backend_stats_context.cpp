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

//  oid_t num_databases = catalog::Manager::GetInstance().GetDatabaseCount();
//  for (oid_t i = 0; i < num_databases; ++i) {
//    auto *database = catalog::Manager::GetInstance().GetDatabase(i);
//    oid_t num_tables = database->GetTableCount();
//
//    for (oid_t j = 0; j < num_tables; ++j) {
//      // Initialize all per-table stats
//      auto *table = database->GetTable(j);
//      oid_t table_id = table->GetOid();
//      table_accesses_[table_id] = new AccessMetric{MetricType::ACCESS_METRIC};
//      oid_t num_indexes = table->GetIndexCount();
//
//      for (oid_t k = 0; k < num_indexes; ++k) {
//        auto *index = table->GetIndex(k);
//        oid_t index_id = index->GetOid();
//        index_accesses_[index_id] = new AccessMetric{MetricType::ACCESS_METRIC};
//      }
//    }
//  }
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
}


}  // namespace stats
}  // namespace peloton
