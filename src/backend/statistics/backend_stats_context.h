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
#include <iostream>
#include <sstream>

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

  inline void Reset() {
    txn_committed.Reset();
    txn_aborted.Reset();
  }

  inline std::string ToString() {
    std::stringstream ss;
    ss <<  "txn_committed: " << txn_committed.ToString() << std::endl;
    ss <<  "txn_aborted: " << txn_aborted.ToString() << std::endl;
    return ss.str();
  }


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
