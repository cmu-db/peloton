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
  thread_id_ = this_id;
}

BackendStatsContext::~BackendStatsContext() {
  // peloton::stats::StatsAggregator::GetInstance().UnregisterContext(thread_id);
}

void BackendStatsContext::Aggregate(BackendStatsContext& source) {
  // Aggregate all global metrics
  txn_latencies_.Aggregate(source.txn_latencies_);
  txn_latencies_.ComputeLatencies();

  // Aggregate all per-database metrics
  for (auto& database_item : database_metrics_) {
    auto worker_db_metric = source.GetDatabaseMetric(database_item.first);
    if (worker_db_metric != nullptr) {
      database_item.second->Aggregate(*worker_db_metric);
    }
  }

  // Aggregate all per-table metrics
  for (auto& table_item : table_metrics_) {
    auto worker_table_metric = source.GetTableMetric(
        table_item.second->GetDatabaseId(), table_item.second->GetTableId());
    if (worker_table_metric != nullptr) {
      table_item.second->Aggregate(*worker_table_metric);
    }
  }

  // Aggregate all per-index metrics
  for (auto& index_item : source.index_metrics_) {
    auto worker_index_metric = GetIndexMetric(
        index_item.second->GetDatabaseId(), index_item.second->GetTableId(),
        index_item.second->GetIndexId());
    if (worker_index_metric != nullptr) {
      index_item.second->Aggregate(*worker_index_metric);
    }
  }
}

void BackendStatsContext::Reset() {
  txn_latencies_.Reset();

  for (auto& database_item : database_metrics_) {
    database_item.second->Reset();
  }
  for (auto& table_item : table_metrics_) {
    table_item.second->Reset();
  }
  for (auto& index_item : index_metrics_) {
    index_item.second->Reset();
  }

  oid_t num_databases = catalog::Manager::GetInstance().GetDatabaseCount();
  for (oid_t i = 0; i < num_databases; ++i) {
    auto database = catalog::Manager::GetInstance().GetDatabase(i);
    oid_t database_id = database->GetOid();

    // Reset database metrics
    if (database_metrics_.find(database_id) == database_metrics_.end()) {
      database_metrics_[database_id] = std::unique_ptr<DatabaseMetric>(
          new DatabaseMetric{DATABASE_METRIC, database_id});
    }

    // Reset table metrics
    oid_t num_tables = database->GetTableCount();
    for (oid_t j = 0; j < num_tables; ++j) {
      auto table = database->GetTable(j);
      oid_t table_id = table->GetOid();
      TableMetric::TableKey table_key =
          TableMetric::GetKey(database_id, table_id);

      if (table_metrics_.find(table_key) == table_metrics_.end()) {
        table_metrics_[table_key] = std::unique_ptr<TableMetric>(
            new TableMetric{TABLE_METRIC, database_id, table_id});
      }

      // Reset indexes metrics
      oid_t num_indexes = table->GetIndexCount();
      for (oid_t k = 0; k < num_indexes; ++k) {
        auto index = table->GetIndex(k);
        oid_t index_id = index->GetOid();
        IndexMetric::IndexKey index_key =
            IndexMetric::GetKey(database_id, table_id, index_id);

        if (index_metrics_.find(index_key) == index_metrics_.end()) {
          index_metrics_[index_key] = std::unique_ptr<IndexMetric>(
              new IndexMetric{INDEX_METRIC, database_id, table_id, index_id});
        }
      }
    }
  }
}

std::string BackendStatsContext::ToString() const {
  std::stringstream ss;

  ss << txn_latencies_.ToString() << std::endl;

  for (auto& database_item : database_metrics_) {
    oid_t database_id = database_item.second->GetDatabaseId();
    ss << database_item.second->ToString();

    for (auto& table_item : table_metrics_) {
      if (table_item.second->GetDatabaseId() == database_id) {
        ss << table_item.second->ToString();

        oid_t table_id = table_item.second->GetTableId();
        for (auto& index_item : index_metrics_) {
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
