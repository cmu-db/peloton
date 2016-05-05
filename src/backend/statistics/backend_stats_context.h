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

#include <mutex>
#include <map>
#include <vector>
#include <thread>

#include "backend/common/types.h"
#include "backend/statistics/counter_metric.h"

//===--------------------------------------------------------------------===//
// GUC Variables
//===--------------------------------------------------------------------===//


namespace peloton {
namespace stats {


/**
 * Context of backend stats as a singleton
 */
class BackendStatsContext {
 public:

  BackendStatsContext();
  ~BackendStatsContext();

  inline std::thread::id GetThreadId() {
    return thread_id;
  }

  void Aggregtate(BackendStatsContext &source);

  // Global metrics
  CounterMetric txn_committed{MetricType::COUNTER_METRIC};
  CounterMetric txn_aborted{MetricType::COUNTER_METRIC};

  // Table metrics
  //std::unordered_map<oid_t, >

 private:
  std::thread::id thread_id;

};

}  // namespace stats
}  // namespace peloton
