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

#include "common/platform.h"
#include "common/container/cuckoo_map.h"
#include "common/container/lock_free_queue.h"
#include "common/synchronization/spin_latch.h"
#include "statistics/table_metric.h"
#include "statistics/index_metric.h"
#include "statistics/latency_metric.h"
#include "statistics/database_metric.h"
#include "statistics/query_metric.h"

#define QUERY_METRIC_QUEUE_SIZE 100000

namespace peloton {

class Statement;

namespace index {
class IndexMetadata;
}  // namespace index

namespace stats {

class CounterMetric;

/**
 * Context of backend stats as a singleton per thread
 */
class BackendStatsContext {
 public:
  static BackendStatsContext* GetInstance();

  BackendStatsContext(bool register_to_aggregator, std::vector<MetricType> metrics_to_collect);
  ~BackendStatsContext();

  //===--------------------------------------------------------------------===//
  // MUTATORS
  //===--------------------------------------------------------------------===//

  // calls Log for metric_type
  // invariant: check type is in GetLoggedMetricTypes() before calling Log()
  // alternative: walk through all of metric_types_ each time
  // possibly more overhead, but cleaner exterior
  void Log(MetricType metric_type);

  // calls Init for metric_type
  void Init();

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  inline std::thread::id GetThreadId() { return thread_id_; }

  std::vector<std::pair<MetricType, AbstractMetric>> GetLoggedMetrics();

  std::vector<MetricType> GetLoggedMetricTypes();


  //===--------------------------------------------------------------------===//
  // HELPER FUNCTIONS
  //===--------------------------------------------------------------------===//

  /**
   * Aggregate another BackendStatsContext to myself
   */
  void Aggregate(BackendStatsContext& source);

  // Resets all metrics (and sub-metrics) to their starting state
  // (e.g., sets all counters to zero)
  void Reset();

  std::string ToString() const;

 private:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // The thread ID of this worker
  std::thread::id thread_id_;

  std::vector<MetricType> metric_types_;
  std::vector<AbstractMetric> metrics_;

  //===--------------------------------------------------------------------===//
  // HELPER FUNCTIONS
  //===--------------------------------------------------------------------===//

  // Get the mapping table of backend stat context for each thread
  static CuckooMap<std::thread::id, std::shared_ptr<BackendStatsContext>> &
    GetBackendContextMap(void);

};

}  // namespace stats
}  // namespace peloton
