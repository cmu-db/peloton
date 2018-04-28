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
  static ThreadLevelStatsCollector &GetCollectorForThread() {
    static std::unordered_map<std::thread::id, ThreadLevelStatsCollector> collector_map;
    std::thread::id tid = std::this_thread::get_id();
    return collector_map[tid];
  }

  static std::unordered_map<std::thread::id, ThreadLevelStatsCollector> &GetAllCollectprs() {
    static std::unordered_map<std::thread::id, ThreadLevelStatsCollector> collector_map;
    return collector_map;
  };

  ThreadLevelStatsCollector() {
    // TODO(tianyu): Write stats to register here
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
  inline void CollectTransactionAbort(oid_t database_id) {
    for (auto &metric : metric_dispatch_[StatInsertionPoint::TXN_ABORT])
      metric->OnTransactionAbort(database_id);
  };
  inline void CollectTupleRead() {
    for (auto &metric : metric_dispatch_[StatInsertionPoint::TUPLE_READ])
      metric->OnTupleRead();
  };
  inline void CollectTupleUpdate() {
    for (auto &metric : metric_dispatch_[StatInsertionPoint::TUPLE_UPDATE])
      metric->OnTupleUpdate();
  };
  inline void CollectTupleInsert() {
    for (auto &metric : metric_dispatch_[StatInsertionPoint::TUPLE_INSERT])
      metric->OnTupleInsert();
  };
  inline void CollectTupleDelete() {
    for (auto &metric : metric_dispatch_[StatInsertionPoint::TUPLE_DELETE])
      metric->OnTupleDelete();
  };
  inline void CollectIndexRead() {
    for (auto &metric : metric_dispatch_[StatInsertionPoint::INDEX_READ])
      metric->OnIndexRead();
  };
  inline void CollectIndexUpdate() {
    for (auto &metric : metric_dispatch_[StatInsertionPoint::INDEX_UPDATE])
      metric->OnIndexUpdate();
  };
  inline void CollectIndexInsert() {
    for (auto &metric : metric_dispatch_[StatInsertionPoint::INDEX_INSERT])
      metric->OnIndexInsert();
  };
  inline void CollectIndexDelete() {
    for (auto &metric : metric_dispatch_[StatInsertionPoint::INDEX_DELETE])
      metric->OnIndexDelete();
  };
  inline void CollectQueryBegin() {
    for (auto &metric : metric_dispatch_[StatInsertionPoint::QUERY_BEGIN])
      metric->OnQueryBegin();
  };
  inline void CollectQueryEnd() {
    for (auto &metric : metric_dispatch_[StatInsertionPoint::QUERY_END])
      metric->OnQueryEnd();
  };

 private:
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
