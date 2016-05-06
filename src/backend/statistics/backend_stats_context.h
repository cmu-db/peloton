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
#include <thread>
#include <unordered_map>
#include <iostream>
#include <sstream>

#include "backend/catalog/manager.h"
#include "backend/common/types.h"
#include "backend/statistics/access_metric.h"
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

  inline AccessMetric* GetTableAccessMetric(oid_t table_id) {
    return table_accesses_.find(table_id)->second;
  }

  void Aggregtate(BackendStatsContext &source);

  inline void Reset() {
    txn_committed.Reset();
    txn_aborted.Reset();
  }

  inline std::string ToString() {
    std::stringstream ss;
    ss <<  "txn_committed: " << txn_committed.ToString();
    ss <<  "txn_aborted: " << txn_aborted.ToString();

    for (auto table_item : table_accesses_) {
      ss << "Table " << table_item.first << ": " << table_item.second->ToString() << std::endl;
    }
    return ss.str();
  }


  // Global metrics
  CounterMetric txn_committed{MetricType::COUNTER_METRIC};
  CounterMetric txn_aborted{MetricType::COUNTER_METRIC};

  // Table metrics
  std::unordered_map<oid_t, AccessMetric*> table_accesses_;

  // Index metrics
  std::unordered_map<oid_t, AccessMetric*> index_accesses_;

 private:
  std::thread::id thread_id;

};

}  // namespace stats
}  // namespace peloton
