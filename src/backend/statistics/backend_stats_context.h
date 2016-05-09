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

#include "backend/catalog/manager.h"
#include "backend/common/types.h"
#include "backend/index/index.h"
#include "backend/statistics/counter_metric.h"
#include "backend/statistics/database_metric.h"
#include "backend/statistics/index_metric.h"
#include "backend/statistics/latency_metric.h"
#include "backend/statistics/table_metric.h"
#include "backend/storage/database.h"

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

  inline std::thread::id GetThreadId() { return thread_id; }

  // Returns the table metric with the given database ID and table ID
  inline TableMetric* GetTableMetric(oid_t database_id, oid_t table_id) {
    TableMetric::TableKey table_key =
        TableMetric::GetKey(database_id, table_id);
    if (table_metrics_.find(table_key) == table_metrics_.end()) {
      table_metrics_[table_key] =
          new TableMetric{TABLE_METRIC, database_id, table_id};
    }
    return table_metrics_[table_key];
  }

  // Returns the database metric with the given database ID
  inline DatabaseMetric* GetDatabaseMetric(oid_t database_id) {
    if (database_metrics_.find(database_id) == database_metrics_.end()) {
      database_metrics_[database_id] =
          new DatabaseMetric(DATABASE_METRIC, database_id);
    }
    return database_metrics_[database_id];
  }

  // Returns the index metric with the given database ID, table ID, and
  // index ID
  inline IndexMetric* GetIndexMetric(oid_t database_id, oid_t table_id,
                                     oid_t index_id) {
    IndexMetric::IndexKey index_key =
        IndexMetric::GetKey(database_id, table_id, index_id);
    if (index_metrics_.find(index_key) == index_metrics_.end()) {
      index_metrics_[index_key] =
          new IndexMetric{INDEX_METRIC, database_id, table_id, index_id};
    }
    return index_metrics_[index_key];
  }

  inline LatencyMetric& GetTxnLatencyMetric() { return txn_latencies_; }

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

  /**
   * Resets all metrics
   * Including create new submetrics according to current database
   * status.
   */
  void Reset();

  std::string ToString();

  // Database metrics
  std::unordered_map<oid_t, DatabaseMetric*> database_metrics_;

  // Table metrics
  std::unordered_map<TableMetric::TableKey, TableMetric*> table_metrics_;

  // Index metrics
  std::unordered_map<IndexMetric::IndexKey, IndexMetric*> index_metrics_;

 private:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // The thread ID of this worker
  std::thread::id thread_id;

  // Latencies recorded by this worker
  LatencyMetric txn_latencies_;
};

}  // namespace stats
}  // namespace peloton
