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
#include "statistics/StatsEventType.h"
#include "statistics/abstract_metric.h"

namespace peloton {
namespace stats {
/**
 * @brief Class responsible for collecting raw data on a single thread.
 *
 * Each thread will be assigned one collector that is globally unique. This is
 * to ensure that we can collect raw data in an non-blocking way as the collection
 * code runs on critical query path. Periodically a dedicated aggregator thread
 * will put the data from all collectors together into a meaningful form.
 */
class ThreadLevelStatsCollector {
 public:
  /**
   * @return the Collector for the calling thread
   */
  static ThreadLevelStatsCollector &GetCollectorForThread() {
    static std::unordered_map<std::thread::id, ThreadLevelStatsCollector> collector_map;
    std::thread::id tid = std::this_thread::get_id();
    return collector_map[tid];
  }

  /**
   * @return A mapping from each thread to their assigned Collector
   */
  static std::unordered_map<std::thread::id, ThreadLevelStatsCollector> &GetAllCollectors() {
    static std::unordered_map<std::thread::id, ThreadLevelStatsCollector> collector_map;
    return collector_map;
  };

  ThreadLevelStatsCollector() {
    // TODO(tianyu): Write stats to register here
  }

  // TODO(tianyu): fill arguments
  inline void CollectTransactionBegin() {
    for (auto &metric : metric_dispatch_[StatsEventType::TXN_BEGIN])
      metric->OnTransactionBegin();
  };
  inline void CollectTransactionCommit(oid_t database_id) {
    for (auto &metric : metric_dispatch_[StatsEventType::TXN_COMMIT])
      metric->OnTransactionCommit(database_id);
  };
  inline void CollectTransactionAbort(oid_t database_id) {
    for (auto &metric : metric_dispatch_[StatsEventType::TXN_ABORT])
      metric->OnTransactionAbort(database_id);
  };
  inline void CollectTupleRead() {
    for (auto &metric : metric_dispatch_[StatsEventType::TUPLE_READ])
      metric->OnTupleRead();
  };
  inline void CollectTupleUpdate() {
    for (auto &metric : metric_dispatch_[StatsEventType::TUPLE_UPDATE])
      metric->OnTupleUpdate();
  };
  inline void CollectTupleInsert() {
    for (auto &metric : metric_dispatch_[StatsEventType::TUPLE_INSERT])
      metric->OnTupleInsert();
  };
  inline void CollectTupleDelete() {
    for (auto &metric : metric_dispatch_[StatsEventType::TUPLE_DELETE])
      metric->OnTupleDelete();
  };
  inline void CollectIndexRead() {
    for (auto &metric : metric_dispatch_[StatsEventType::INDEX_READ])
      metric->OnIndexRead();
  };
  inline void CollectIndexUpdate() {
    for (auto &metric : metric_dispatch_[StatsEventType::INDEX_UPDATE])
      metric->OnIndexUpdate();
  };
  inline void CollectIndexInsert() {
    for (auto &metric : metric_dispatch_[StatsEventType::INDEX_INSERT])
      metric->OnIndexInsert();
  };
  inline void CollectIndexDelete() {
    for (auto &metric : metric_dispatch_[StatsEventType::INDEX_DELETE])
      metric->OnIndexDelete();
  };
  inline void CollectQueryBegin() {
    for (auto &metric : metric_dispatch_[StatsEventType::QUERY_BEGIN])
      metric->OnQueryBegin();
  };
  inline void CollectQueryEnd() {
    for (auto &metric : metric_dispatch_[StatsEventType::QUERY_END])
      metric->OnQueryEnd();
  };

  /**
   * @return A vector of raw data, for each registered metric. Each piece of
   * data is guaranteed to be safe to read and remove, and the same type of
   * metric is guaranteed to be in the same positopn in the returned vector
   * for different instances of Collector.
   */
  std::vector<std::shared_ptr<AbstractRawData>> GetDataToAggregate() {
    std::vector<std::shared_ptr<AbstractRawData>> result;
    for (auto &metric : metrics_)
      result.push_back(metric->Swap());
    return result;
  }

 private:
  /**
   * Registers a Metric so that its callbacks are invoked.
   * Use this only in the constructor.
   * @tparam metric type of Metric to register
   * @param types A list of event types to receive updates about.
   */
  template<typename metric>
  void RegisterMetric(std::vector<StatsEventType> types) {
    auto m = std::make_shared<metric>();
    metrics_.push_back(m);
    for (StatsEventType type : types)
      metric_dispatch_[type].push_back(m);
  }

  using MetricList = std::vector<std::shared_ptr<Metric>>;
  /**
   * List of all registered metrics
   */
  MetricList metrics_;
  /**
   * Mapping from each type of event to a list of metrics registered to receive
   * updates from that type of event.
   */
  std::unordered_map<StatsEventType,
                     MetricList,
                     EnumHash<StatsEventType>> metric_dispatch_;
};

}  // namespace stats
}  // namespace peloton
