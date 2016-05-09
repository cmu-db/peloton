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

#include <map>

#include "backend/common/types.h"
#include "backend/index/index.h"
#include "backend/statistics/backend_stats_context.h"
#include "backend/storage/database.h"

namespace peloton {
namespace stats {

BackendStatsContext::BackendStatsContext(size_t max_latency_history)
    : txn_latencies_(LATENCY_METRIC, max_latency_history) {
  std::thread::id this_id = std::this_thread::get_id();
  thread_id = this_id;
}

BackendStatsContext::~BackendStatsContext() {
  // peloton::stats::StatsAggregator::GetInstance().UnregisterContext(thread_id);
  for (auto& database_item : database_metrics_) {
    delete database_item.second;
  }

  for (auto& access_item : table_metrics_) {
    delete access_item.second;
  }

  for (auto& access_item : index_metrics_) {
    delete access_item.second;
  }
}

void BackendStatsContext::Aggregate(BackendStatsContext& source_) {
  for (auto& database_item : source_.database_metrics_) {
    GetDatabaseMetric(database_item.first)->Aggregate(*database_item.second);
  }

  for (auto& access_item : source_.table_metrics_) {
    GetTableMetric(access_item.second->GetDatabaseId(),
                   access_item.second->GetTableId())
        ->Aggregate(*access_item.second);
  }

  for (auto& access_item : source_.index_metrics_) {
    GetIndexMetric(
        access_item.second->GetDatabaseId(), access_item.second->GetTableId(),
        access_item.second->GetIndexId())->Aggregate(*access_item.second);
  }
  txn_latencies_.Aggregate(source_.txn_latencies_);
}

void BackendStatsContext::Reset() {
  txn_latencies_.Reset();

  for (auto& database_item : database_metrics_) {
    database_item.second->Reset();
  }
  for (auto table_item : table_metrics_) {
    table_item.second->Reset();
  }
  for (auto index_item : index_metrics_) {
    index_item.second->Reset();
  }

  oid_t num_databases = catalog::Manager::GetInstance().GetDatabaseCount();
  for (oid_t i = 0; i < num_databases; ++i) {
    auto database = catalog::Manager::GetInstance().GetDatabase(i);
    oid_t database_id = database->GetOid();

    // Reset database metrics
    if (database_metrics_.find(database_id) == database_metrics_.end()) {
      database_metrics_[database_id] =
          new DatabaseMetric(DATABASE_METRIC, database_id);
    }

    // Reset table metrics
    oid_t num_tables = database->GetTableCount();
    for (oid_t j = 0; j < num_tables; ++j) {
      auto table = database->GetTable(j);
      oid_t table_id = table->GetOid();
      TableMetric::TableKey table_key =
          TableMetric::GetKey(database_id, table_id);

      if (table_metrics_.find(table_key) == table_metrics_.end()) {
        table_metrics_[table_key] =
            new TableMetric(TABLE_METRIC, database_id, table_id);
      }

      // Reset indexes metrics
      oid_t num_indexes = table->GetIndexCount();
      for (oid_t k = 0; k < num_indexes; ++k) {
        auto index = table->GetIndex(k);
        oid_t index_id = index->GetOid();
        IndexMetric::IndexKey index_key =
            IndexMetric::GetKey(database_id, table_id, index_id);

        if (index_metrics_.find(index_key) == index_metrics_.end()) {
          index_metrics_[index_key] =
              new IndexMetric(INDEX_METRIC, database_id, table_id, index_id);
        }
      }
    }
  }
}

std::string BackendStatsContext::ToString() {
  std::stringstream ss;

  ss << txn_latencies_.ToString() << std::endl;

  for (auto database_item : database_metrics_) {
    oid_t database_id = database_item.second->GetDatabaseId();
    ss << database_item.second->ToString();

    for (auto table_item : table_metrics_) {
      if (table_item.second->GetDatabaseId() == database_id) {
        ss << table_item.second->ToString();

        oid_t table_id = table_item.second->GetTableId();
        for (auto index_item : index_metrics_) {
          if (index_item.second->GetDatabaseId() == database_id &&
              index_item.second->GetTableId() == table_id) {
            ss << index_item.second->ToString();
          }
        }
        if (!index_metrics_.empty()) {
          ss << std::endl;
        }
      }
      if (!table_metrics_.empty()) {
        ss << std::endl;
      }
    }
    if (!database_metrics_.empty()) {
      ss << std::endl;
    }
  }

  return ss.str();
}

}  // namespace stats
}  // namespace peloton
