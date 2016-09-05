//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// backend_stats_context.h
//
// Identification: src/statistics/backend_stats_context.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <map>
#include <thread>
#include <sstream>
#include <unordered_map>

#include "statistics/table_metric.h"
#include "statistics/index_metric.h"
#include "statistics/latency_metric.h"
#include "statistics/database_metric.h"
#include "container/cuckoo_map.h"

namespace peloton {

namespace index {
class IndexMetadata;
}

namespace stats {

class CounterMetric;

/**
 * Context of backend stats as a singleton per thread
 */
class BackendStatsContext {
 public:
  static BackendStatsContext* GetInstance();

  BackendStatsContext(size_t max_latency_history, bool regiser_to_aggregator);
  ~BackendStatsContext();

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  inline std::thread::id GetThreadId() { return thread_id_; }

  // Returns the table metric with the given database ID and table ID
  TableMetric* GetTableMetric(oid_t database_id, oid_t table_id);

  // Returns the database metric with the given database ID
  DatabaseMetric* GetDatabaseMetric(oid_t database_id);

  // Returns the index metric with the given database ID, table ID, and
  // index ID
  IndexMetric* GetIndexMetric(oid_t database_id, oid_t table_id,
                              oid_t index_id);

  LatencyMetric& GetTxnLatencyMetric();

  void IncrementTableReads(oid_t tile_group_id);

  void IncrementTableInserts(oid_t tile_group_id);

  void IncrementTableUpdates(oid_t tile_group_id);

  void IncrementTableDeletes(oid_t tile_group_id);

  void IncrementIndexReads(size_t read_count, index::IndexMetadata* metadata);

  void IncrementIndexInserts(index::IndexMetadata* metadata);

  void IncrementIndexUpdates(index::IndexMetadata* metadata);

  void IncrementIndexDeletes(size_t delete_count,
                             index::IndexMetadata* metadata);

  void IncrementTxnCommitted(oid_t database_id);

  void IncrementTxnAborted(oid_t database_id);

  //===--------------------------------------------------------------------===//
  // HELPER FUNCTIONS
  //===--------------------------------------------------------------------===//

  /**
   * Aggregate another BackendStatsContext to myself
   */
  void Aggregate(BackendStatsContext& source);

  bool operator==(const BackendStatsContext& other);

  bool operator!=(const BackendStatsContext& other);

  // Resets all metrics (and sub-metrics) to their starting state
  // (e.g., sets all counters to zero)
  void Reset();

  std::string ToString() const;

  // Database metrics
  std::unordered_map<oid_t, std::unique_ptr<DatabaseMetric>>
      database_metrics_{};

  // Table metrics
  std::unordered_map<TableMetric::TableKey, std::unique_ptr<TableMetric>>
      table_metrics_{};

  // Index metrics
  std::unordered_map<IndexMetric::IndexKey, std::unique_ptr<IndexMetric>>
      index_metrics_{};

 private:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // The thread ID of this worker
  std::thread::id thread_id_;

  // Latencies recorded by this worker
  LatencyMetric txn_latencies_;

  // Whether this context is registered to the global aggregator
  bool is_registered_to_aggregator_;

  static CuckooMap<std::thread::id, std::shared_ptr<BackendStatsContext>>
      stats_context_map_;
};

}  // namespace stats
}  // namespace peloton
