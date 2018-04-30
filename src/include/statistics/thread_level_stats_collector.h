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
#include "statistics/stats_event_type.h"
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
    for (auto &metric : metric_dispatch_[stats_event_type::TXN_BEGIN])
      metric->OnTransactionBegin();
  };
  inline void CollectTransactionCommit(oid_t database_id) {
    for (auto &metric : metric_dispatch_[stats_event_type::TXN_COMMIT])
      metric->OnTransactionCommit(database_id);
  };
  inline void CollectTransactionAbort(oid_t database_id) {
    for (auto &metric : metric_dispatch_[stats_event_type::TXN_ABORT])
      metric->OnTransactionAbort(database_id);
  };
  inline void CollectTupleRead(oid_t table_id, size_t num_read) {
    for (auto &metric : metric_dispatch_[stats_event_type::TUPLE_READ])
      metric->OnTupleRead(table_id, num_read);
  };
  inline void CollectTupleUpdate(oid_t table_id) {
    for (auto &metric : metric_dispatch_[stats_event_type::TUPLE_UPDATE])
      metric->OnTupleUpdate(table_id);
  };
  inline void CollectTupleInsert(oid_t table_id) {
    for (auto &metric : metric_dispatch_[stats_event_type::TUPLE_INSERT])
      metric->OnTupleInsert(table_id);
  };
  inline void CollectTupleDelete(oid_t table_id) {
    for (auto &metric : metric_dispatch_[stats_event_type::TUPLE_DELETE])
      metric->OnTupleDelete(table_id);
  };
  inline void CollectIndexRead(oid_t index_id, size_t num_read) {
    for (auto &metric : metric_dispatch_[stats_event_type::INDEX_READ])
      metric->OnIndexRead(index_id, num_read);
  };
  inline void CollectIndexUpdate(oid_t index_id) {
    for (auto &metric : metric_dispatch_[stats_event_type::INDEX_UPDATE])
      metric->OnIndexUpdate(index_id);
  };
  inline void CollectIndexInsert(oid_t index_id) {
    for (auto &metric : metric_dispatch_[stats_event_type::INDEX_INSERT])
      metric->OnIndexInsert(index_id);
  };
  inline void CollectIndexDelete(oid_t index_id) {
    for (auto &metric : metric_dispatch_[stats_event_type::INDEX_DELETE])
      metric->OnIndexDelete(index_id);
  };
  inline void CollectQueryBegin() {
    for (auto &metric : metric_dispatch_[stats_event_type::QUERY_BEGIN])
      metric->OnQueryBegin();
  };
  inline void CollectQueryEnd() {
    for (auto &metric : metric_dispatch_[stats_event_type::QUERY_END])
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
  void RegisterMetric(std::vector<stats_event_type> types) {
    auto m = std::make_shared<metric>();
    metrics_.push_back(m);
    for (stats_event_type type : types)
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
  std::unordered_map<stats_event_type,
                     MetricList,
                     EnumHash<stats_event_type>> metric_dispatch_;
};

}  // namespace stats
}  // namespace peloton
