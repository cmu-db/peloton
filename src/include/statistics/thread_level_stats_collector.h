//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// stats_collector.h
//
// Identification: src/statistics/backend_stats_context.h
//
// Copyright (c) 2017-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <unordered_map>
#include <thread>
#include "common/internal_types.h"
#include "statistics/stat_insertion_point.h"
#include "statistics/abstract_metric.h"

namespace peloton {
namespace stats {
class ThreadLevelStatsCollector {
 public:
  static ThreadLevelStatsCollector &GetCollectorForThread(std::thread::id tid = std::this_thread::get_id()) {
    static std::unordered_map<std::thread::id, ThreadLevelStatsCollector> collector_map;
    return collector_map[tid];
  }

  // TODO(tianyu): fill arguments
  inline void CollectTransactionBegin() {
    for (auto &metric : metric_dispatch_[StatInsertionPoint::TXN_BEGIN])
      metric->OnTransactionBegin();
  };
  inline void CollectTransactionCommit(oid_t database_id) {
    for (auto &metric : metric_dispatch_[StatInsertionPoint::TXN_COMMIT])
      metric->OnTransactionCommit(database_id);
  };

  inline void CollectTransactionAbort(oid_t) {};
  inline void CollectTupleRead() {};
  inline void CollectTupleUpdate() {};
  inline void CollectTupleInsert() {};
  inline void CollectTupleDelete() {};
  inline void CollectIndexRead() {};
  inline void CollectIndexUpdate() {};
  inline void CollectIndexInsert() {};
  inline void CollectIndexDelete() {};
  inline void CollectQueryBegin() {};
  inline void CollectQueryEnd() {};

 private:
  static void RegisterMetrics() {
    // TODO(tianyu): Write stats to register here
  }

  template<typename metric>
  void RegisterMetric(std::vector<StatInsertionPoint> points) {
    auto m = std::make_shared<metric>();
    metrics_.push_back(m);
    for (StatInsertionPoint point : points)
      metric_dispatch_[point].push_back(m);
  }

  using MetricList = std::vector<std::shared_ptr<Metric>>;
  MetricList metrics_;
  std::unordered_map<StatInsertionPoint,
                     MetricList,
                     EnumHash<StatInsertionPoint>> metric_dispatch_;
};

}  // namespace stats
}  // namespace peloton
