//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// backend_stats_context.h
//
// Identification: src/backend/statistics/backend_stats_context.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <map>
#include <thread>
#include <sstream>
#include <unordered_map>

#include "libcuckoo/cuckoohash_map.hh"

#include "backend/catalog/manager.h"
#include "backend/common/types.h"
#include "backend/index/index.h"
#include "backend/statistics/counter_metric.h"
#include "backend/statistics/database_metric.h"
#include "backend/statistics/index_metric.h"
#include "backend/statistics/latency_metric.h"
#include "backend/statistics/table_metric.h"
#include "backend/storage/database.h"
#include "backend/storage/tile_group.h"

namespace peloton {
namespace stats {

/**
 * Context of backend stats as a singleton per thread
 */
class BackendStatsContext {
 public:
  BackendStatsContext(size_t max_latency_history);
  ~BackendStatsContext();

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  inline std::thread::id GetThreadId() { return thread_id_; }

  // Returns the table metric with the given database ID and table ID
  inline TableMetric* GetTableMetric(oid_t database_id, oid_t table_id) {
    TableMetric::TableKey table_key = TableMetric::GetKey(database_id, table_id);
    if (table_metrics_.find(table_key) == table_metrics_.end()) {
      table_metrics_[table_key] = std::unique_ptr<TableMetric>(
          new TableMetric{TABLE_METRIC, database_id, table_id});
    }
    return table_metrics_[table_key].get();
  }

  // Returns the database metric with the given database ID
  inline DatabaseMetric* GetDatabaseMetric(oid_t database_id) {
    if (database_metrics_.find(database_id) == database_metrics_.end()) {
      database_metrics_[database_id] = std::unique_ptr<DatabaseMetric>(
          new DatabaseMetric{DATABASE_METRIC, database_id});
    }
    return database_metrics_[database_id].get();
  }

  // Returns the index metric with the given database ID, table ID, and
  // index ID
  inline IndexMetric* GetIndexMetric(oid_t database_id, oid_t table_id,
                                     oid_t index_id) {
    IndexMetric::IndexKey index_key =
        IndexMetric::GetKey(database_id, table_id, index_id);
    if (index_metrics_.find(index_key) == index_metrics_.end()) {
      index_metrics_[index_key] = std::unique_ptr<IndexMetric>(
          new IndexMetric{INDEX_METRIC, database_id, table_id,
          index_id});
    }
    return index_metrics_[index_key].get();
  }

  inline LatencyMetric& GetTxnLatencyMetric() { return txn_latencies_; }

  inline void IncrementTableReads(oid_t tile_group_id) {
    oid_t table_id = catalog::Manager::GetInstance()
                         .GetTileGroup(tile_group_id)
                         ->GetTableId();
    oid_t database_id = catalog::Manager::GetInstance()
                            .GetTileGroup(tile_group_id)
                            ->GetDatabaseId();
    auto table_metric = GetTableMetric(database_id, table_id);
    assert(table_metric != nullptr);
    table_metric->GetTableAccess().IncrementReads();
  }

  inline void IncrementTableInserts(oid_t tile_group_id) {
    oid_t table_id = catalog::Manager::GetInstance()
                         .GetTileGroup(tile_group_id)
                         ->GetTableId();
    oid_t database_id = catalog::Manager::GetInstance()
                            .GetTileGroup(tile_group_id)
                            ->GetDatabaseId();
    auto table_metric = GetTableMetric(database_id, table_id);
    assert(table_metric != nullptr);
    table_metric->GetTableAccess().IncrementInserts();
  }

  inline void IncrementTableUpdates(oid_t tile_group_id) {
    oid_t table_id = catalog::Manager::GetInstance()
                         .GetTileGroup(tile_group_id)
                         ->GetTableId();
    oid_t database_id = catalog::Manager::GetInstance()
                            .GetTileGroup(tile_group_id)
                            ->GetDatabaseId();
    auto table_metric = GetTableMetric(database_id, table_id);
    assert(table_metric != nullptr);
    table_metric->GetTableAccess().IncrementUpdates();
  }

  inline void IncrementTableDeletes(oid_t tile_group_id) {
    oid_t table_id = catalog::Manager::GetInstance()
                         .GetTileGroup(tile_group_id)
                         ->GetTableId();
    oid_t database_id = catalog::Manager::GetInstance()
                            .GetTileGroup(tile_group_id)
                            ->GetDatabaseId();
    auto table_metric = GetTableMetric(database_id, table_id);
    assert(table_metric != nullptr);
    table_metric->GetTableAccess().IncrementDeletes();
  }

  inline void IncrementIndexReads(size_t read_count,
                                  index::IndexMetadata* metadata) {
    oid_t index_id = metadata->GetOid();
    oid_t table_id = metadata->GetTableOid();
    oid_t database_id = metadata->GetDatabaseOid();
    auto index_metric = GetIndexMetric(database_id, table_id, index_id);
    assert(index_metric != nullptr);
    index_metric->GetIndexAccess().IncrementReads(read_count);
  }

  inline void IncrementIndexInserts(index::IndexMetadata* metadata) {
    oid_t index_id = metadata->GetOid();
    oid_t table_id = metadata->GetTableOid();
    oid_t database_id = metadata->GetDatabaseOid();
    auto index_metric = GetIndexMetric(database_id, table_id, index_id);
    assert(index_metric != nullptr);
    index_metric->GetIndexAccess().IncrementInserts();
  }

  inline void IncrementTableUpdates(index::IndexMetadata* metadata) {
    oid_t index_id = metadata->GetOid();
    oid_t table_id = metadata->GetTableOid();
    oid_t database_id = metadata->GetDatabaseOid();
    auto index_metric = GetIndexMetric(database_id, table_id, index_id);
    assert(index_metric != nullptr);
    index_metric->GetIndexAccess().IncrementUpdates();
  }

  inline void IncrementIndexDeletes(size_t delete_count,
                                    index::IndexMetadata* metadata) {
    oid_t index_id = metadata->GetOid();
    oid_t table_id = metadata->GetTableOid();
    oid_t database_id = metadata->GetDatabaseOid();
    auto index_metric = GetIndexMetric(database_id, table_id, index_id);
    assert(index_metric != nullptr);
    index_metric->GetIndexAccess().IncrementDeletes(delete_count);
  }

  inline void IncrementTxnCommitted(oid_t database_id) {
    auto database_metric = GetDatabaseMetric(database_id);
    assert(database_metric != nullptr);
    database_metric->IncrementTxnCommitted();
  }

  inline void IncrementTxnAborted(oid_t database_id) {
    auto database_metric = GetDatabaseMetric(database_id);
    assert(database_metric != nullptr);
    database_metric->IncrementTxnAborted();
  }

  //===--------------------------------------------------------------------===//
  // HELPER FUNCTIONS
  //===--------------------------------------------------------------------===//

  /**
   * Aggregate another BackendStatsContext to myself
   */
  void Aggregate(BackendStatsContext& source);

  inline bool operator==(const BackendStatsContext& other) {
    return database_metrics_ == other.database_metrics_ &&
           table_metrics_ == other.table_metrics_ &&
           index_metrics_ == other.index_metrics_;
  }

  inline bool operator!=(const BackendStatsContext& other) {
    return !(*this == other);
  }

  // Resets all metrics (and sub-metrics) to their starting state
  // (e.g., sets all counters to zero)
  void Reset();

  std::string ToString() const;

  // Database metrics
  std::unordered_map<oid_t, std::unique_ptr<DatabaseMetric>> database_metrics_;

  // Table metrics
  std::unordered_map<TableMetric::TableKey, std::unique_ptr<TableMetric>>
      table_metrics_;

  // Index metrics
  std::unordered_map<IndexMetric::IndexKey, std::unique_ptr<IndexMetric>>
      index_metrics_;

 private:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // The thread ID of this worker
  std::thread::id thread_id_;

  // Latencies recorded by this worker
  LatencyMetric txn_latencies_;
};

}  // namespace stats
}  // namespace peloton
