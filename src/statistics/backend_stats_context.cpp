//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// backend_stats_context.cpp
//
// Identification: src/statistics/backend_stats_context.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "statistics/backend_stats_context.h"

#include <map>

#include "common/internal_types.h"
#include "common/statement.h"
#include "catalog/catalog.h"
#include "catalog/manager.h"
#include "index/index.h"
#include "statistics/stats_aggregator.h"
#include "storage/storage_manager.h"
#include "storage/tile_group.h"

namespace peloton {
namespace stats {

CuckooMap<std::thread::id, std::shared_ptr<BackendStatsContext>>&
BackendStatsContext::GetBackendContextMap() {
  static CuckooMap<std::thread::id, std::shared_ptr<BackendStatsContext>>
      stats_context_map;
  return stats_context_map;
}

BackendStatsContext* BackendStatsContext::GetInstance() {
  // Each thread gets a backend stats context
  std::thread::id this_id = std::this_thread::get_id();
  std::shared_ptr<BackendStatsContext> result(nullptr);
  auto& stats_context_map = GetBackendContextMap();
  if (stats_context_map.Find(this_id, result) == false) {
    result.reset(new BackendStatsContext(LATENCY_MAX_HISTORY_THREAD, true));
    stats_context_map.Insert(this_id, result);
  }
  return result.get();
}

BackendStatsContext::BackendStatsContext(size_t max_latency_history,
                                         bool regiser_to_aggregator)
    : txn_latencies_(MetricType::LATENCY, max_latency_history) {
  std::thread::id this_id = std::this_thread::get_id();
  thread_id_ = this_id;

  is_registered_to_aggregator_ = regiser_to_aggregator;

  // Register to the global aggregator
  if (regiser_to_aggregator == true)
    StatsAggregator::GetInstance().RegisterContext(thread_id_, this);
}

BackendStatsContext::~BackendStatsContext() {}

//===--------------------------------------------------------------------===//
// ACCESSORS
//===--------------------------------------------------------------------===//

// Returns the table metric with the given database ID and table ID
TableMetric* BackendStatsContext::GetTableMetric(oid_t database_id,
                                                 oid_t table_id) {
  if (table_metrics_.find(table_id) == table_metrics_.end()) {
    table_metrics_[table_id] = std::unique_ptr<TableMetric>(
        new TableMetric{MetricType::TABLE, database_id, table_id});
  }
  return table_metrics_[table_id].get();
}

// Returns the database metric with the given database ID
DatabaseMetric* BackendStatsContext::GetDatabaseMetric(oid_t database_id) {
  if (database_metrics_.find(database_id) == database_metrics_.end()) {
    database_metrics_[database_id] = std::unique_ptr<DatabaseMetric>(
        new DatabaseMetric{MetricType::DATABASE, database_id});
  }
  return database_metrics_[database_id].get();
}

// Returns the index metric with the given database ID, table ID, and
// index ID
IndexMetric* BackendStatsContext::GetIndexMetric(oid_t database_id,
                                                 oid_t table_id,
                                                 oid_t index_id) {
  std::shared_ptr<IndexMetric> index_metric;
  // Index metric doesn't exist yet
  if (index_metrics_.Contains(index_id) == false) {
    index_metric.reset(
        new IndexMetric{MetricType::INDEX, database_id, table_id, index_id});
    index_metrics_.Insert(index_id, index_metric);
    index_id_lock.Lock();
    index_ids_.insert(index_id);
    index_id_lock.Unlock();
  }
  // Get index metric from map
  index_metrics_.Find(index_id, index_metric);
  return index_metric.get();
}

LatencyMetric& BackendStatsContext::GetTxnLatencyMetric() {
  return txn_latencies_;
}

void BackendStatsContext::IncrementTableReads(oid_t tile_group_id) {
  oid_t table_id =
      catalog::Manager::GetInstance().GetTileGroup(tile_group_id)->GetTableId();
  oid_t database_id = catalog::Manager::GetInstance()
                          .GetTileGroup(tile_group_id)
                          ->GetDatabaseId();
  auto table_metric = GetTableMetric(database_id, table_id);
  PL_ASSERT(table_metric != nullptr);
  table_metric->GetTableAccess().IncrementReads();
  if (ongoing_query_metric_ != nullptr) {
    ongoing_query_metric_->GetQueryAccess().IncrementReads();
  }
}

void BackendStatsContext::IncrementTableInserts(oid_t tile_group_id) {
  oid_t table_id =
      catalog::Manager::GetInstance().GetTileGroup(tile_group_id)->GetTableId();
  oid_t database_id = catalog::Manager::GetInstance()
                          .GetTileGroup(tile_group_id)
                          ->GetDatabaseId();
  auto table_metric = GetTableMetric(database_id, table_id);
  PL_ASSERT(table_metric != nullptr);
  table_metric->GetTableAccess().IncrementInserts();
  if (ongoing_query_metric_ != nullptr) {
    ongoing_query_metric_->GetQueryAccess().IncrementInserts();
  }
}

void BackendStatsContext::IncrementTableUpdates(oid_t tile_group_id) {
  oid_t table_id =
      catalog::Manager::GetInstance().GetTileGroup(tile_group_id)->GetTableId();
  oid_t database_id = catalog::Manager::GetInstance()
                          .GetTileGroup(tile_group_id)
                          ->GetDatabaseId();
  auto table_metric = GetTableMetric(database_id, table_id);
  PL_ASSERT(table_metric != nullptr);
  table_metric->GetTableAccess().IncrementUpdates();
  if (ongoing_query_metric_ != nullptr) {
    ongoing_query_metric_->GetQueryAccess().IncrementUpdates();
  }
}

void BackendStatsContext::IncrementTableDeletes(oid_t tile_group_id) {
  oid_t table_id =
      catalog::Manager::GetInstance().GetTileGroup(tile_group_id)->GetTableId();
  oid_t database_id = catalog::Manager::GetInstance()
                          .GetTileGroup(tile_group_id)
                          ->GetDatabaseId();
  auto table_metric = GetTableMetric(database_id, table_id);
  PL_ASSERT(table_metric != nullptr);
  table_metric->GetTableAccess().IncrementDeletes();
  if (ongoing_query_metric_ != nullptr) {
    ongoing_query_metric_->GetQueryAccess().IncrementDeletes();
  }
}

void BackendStatsContext::IncrementIndexReads(size_t read_count,
                                              index::IndexMetadata* metadata) {
  oid_t index_id = metadata->GetOid();
  oid_t table_id = metadata->GetTableOid();
  oid_t database_id = metadata->GetDatabaseOid();
  auto index_metric = GetIndexMetric(database_id, table_id, index_id);
  PL_ASSERT(index_metric != nullptr);
  index_metric->GetIndexAccess().IncrementReads(read_count);
}

void BackendStatsContext::IncrementIndexInserts(
    index::IndexMetadata* metadata) {
  oid_t index_id = metadata->GetOid();
  oid_t table_id = metadata->GetTableOid();
  oid_t database_id = metadata->GetDatabaseOid();
  auto index_metric = GetIndexMetric(database_id, table_id, index_id);
  PL_ASSERT(index_metric != nullptr);
  index_metric->GetIndexAccess().IncrementInserts();
}

void BackendStatsContext::IncrementIndexUpdates(
    index::IndexMetadata* metadata) {
  oid_t index_id = metadata->GetOid();
  oid_t table_id = metadata->GetTableOid();
  oid_t database_id = metadata->GetDatabaseOid();
  auto index_metric = GetIndexMetric(database_id, table_id, index_id);
  PL_ASSERT(index_metric != nullptr);
  index_metric->GetIndexAccess().IncrementUpdates();
}

void BackendStatsContext::IncrementIndexDeletes(
    size_t delete_count, index::IndexMetadata* metadata) {
  oid_t index_id = metadata->GetOid();
  oid_t table_id = metadata->GetTableOid();
  oid_t database_id = metadata->GetDatabaseOid();
  auto index_metric = GetIndexMetric(database_id, table_id, index_id);
  PL_ASSERT(index_metric != nullptr);
  index_metric->GetIndexAccess().IncrementDeletes(delete_count);
}

void BackendStatsContext::IncrementTxnCommitted(oid_t database_id) {
  auto database_metric = GetDatabaseMetric(database_id);
  PL_ASSERT(database_metric != nullptr);
  database_metric->IncrementTxnCommitted();
  CompleteQueryMetric();
}

void BackendStatsContext::IncrementTxnAborted(oid_t database_id) {
  auto database_metric = GetDatabaseMetric(database_id);
  PL_ASSERT(database_metric != nullptr);
  database_metric->IncrementTxnAborted();
  CompleteQueryMetric();
}

void BackendStatsContext::InitQueryMetric(
    const std::shared_ptr<Statement> statement,
    const std::shared_ptr<QueryMetric::QueryParams> params) {
  // TODO currently all queries belong to DEFAULT_DB
  ongoing_query_metric_.reset(new QueryMetric(
      MetricType::QUERY, statement->GetQueryString(), params, DEFAULT_DB_ID));
}

//===--------------------------------------------------------------------===//
// HELPER FUNCTIONS
//===--------------------------------------------------------------------===//

void BackendStatsContext::Aggregate(BackendStatsContext& source) {
  // Aggregate all global metrics
  txn_latencies_.Aggregate(source.txn_latencies_);
  txn_latencies_.ComputeLatencies();

  // Aggregate all per-database metrics
  for (auto& database_item : source.database_metrics_) {
    GetDatabaseMetric(database_item.first)->Aggregate(*database_item.second);
  }

  // Aggregate all per-table metrics
  for (auto& table_item : source.table_metrics_) {
    GetTableMetric(table_item.second->GetDatabaseId(),
                   table_item.second->GetTableId())
        ->Aggregate(*table_item.second);
  }

  // Aggregate all per-index metrics
  for (auto id : index_ids_) {
    std::shared_ptr<IndexMetric> index_metric;
    index_metrics_.Find(id, index_metric);
    auto database_oid = index_metric->GetDatabaseId();
    auto table_oid = index_metric->GetTableId();
    index_metric->Aggregate(
        *(source.GetIndexMetric(database_oid, table_oid, id)));
  }

  // Aggregate all per-query metrics
  std::shared_ptr<QueryMetric> query_metric;
  while (source.completed_query_metrics_.Dequeue(query_metric)) {
    completed_query_metrics_.Enqueue(query_metric);
    LOG_TRACE("Found a query metric to aggregate");
    aggregated_query_count_++;
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
  for (auto id : index_ids_) {
    std::shared_ptr<IndexMetric> index_metric;
    index_metrics_.Find(id, index_metric);
    index_metric->Reset();
  }

  oid_t num_databases =
      storage::StorageManager::GetInstance()->GetDatabaseCount();
  for (oid_t i = 0; i < num_databases; ++i) {
    auto database =
        storage::StorageManager::GetInstance()->GetDatabaseWithOffset(i);
    oid_t database_id = database->GetOid();

    // Reset database metrics
    if (database_metrics_.find(database_id) == database_metrics_.end()) {
      database_metrics_[database_id] = std::unique_ptr<DatabaseMetric>(
          new DatabaseMetric{MetricType::DATABASE, database_id});
    }

    // Reset table metrics
    oid_t num_tables = database->GetTableCount();
    for (oid_t j = 0; j < num_tables; ++j) {
      auto table = database->GetTable(j);
      oid_t table_id = table->GetOid();

      if (table_metrics_.find(table_id) == table_metrics_.end()) {
        table_metrics_[table_id] = std::unique_ptr<TableMetric>(
            new TableMetric{MetricType::TABLE, database_id, table_id});
      }

      // Reset indexes metrics
      oid_t num_indexes = table->GetIndexCount();
      for (oid_t k = 0; k < num_indexes; ++k) {
        auto index = table->GetIndex(k);
        if (index == nullptr) continue;
        oid_t index_id = index->GetOid();
        if (index_metrics_.Contains(index_id) == false) {
          std::shared_ptr<IndexMetric> index_metric(
              new IndexMetric{MetricType::INDEX, database_id, table_id, index_id});
          index_metrics_.Insert(index_id, index_metric);
          index_ids_.insert(index_id);
        }
      }
    }
  }
}

std::string BackendStatsContext::ToString() const {
  std::stringstream ss;

  ss << txn_latencies_.GetInfo() << std::endl;

  for (auto& database_item : database_metrics_) {
    oid_t database_id = database_item.second->GetDatabaseId();
    ss << database_item.second->GetInfo();

    for (auto& table_item : table_metrics_) {
      if (table_item.second->GetDatabaseId() == database_id) {
        ss << table_item.second->GetInfo();

        oid_t table_id = table_item.second->GetTableId();
        for (auto id : index_ids_) {
          std::shared_ptr<IndexMetric> index_metric;
          index_metrics_.Find(id, index_metric);
          if (index_metric->GetDatabaseId() == database_id &&
              index_metric->GetTableId() == table_id) {
            ss << index_metric->GetInfo();
          }
        }
        if (!index_metrics_.IsEmpty()) {
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
  std::string info = ss.str();
  StringUtil::RTrim(info);
  return info;
}

void BackendStatsContext::CompleteQueryMetric() {
  if (ongoing_query_metric_ != nullptr) {
    ongoing_query_metric_->GetProcessorMetric().RecordTime();
    ongoing_query_metric_->GetQueryLatency().RecordLatency();
    completed_query_metrics_.Enqueue(ongoing_query_metric_);
    ongoing_query_metric_.reset();
    LOG_TRACE("Ongoing query completed");
  }
}

}  // namespace stats
}  // namespace peloton
