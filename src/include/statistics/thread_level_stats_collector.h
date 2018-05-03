//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// stats_collector.h
//
// Identification: src/statistics/thread_level_stats_collector.h
//
// Copyright (c) 2017-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <thread>
#include <unordered_map>
#include "common/internal_types.h"
#include "settings/settings_manager.h"
#include "statistics/abstract_metric.h"
#include "tbb/concurrent_unordered_map.h"
#include "statistics/database_metric.h"
#include "statistics/index_metric.h"
#include "statistics/stats_event_type.h"
#include "statistics/table_metric.h"

namespace peloton {
namespace stats {
/**
 * @brief Class responsible for collecting raw data on a single thread.
 *
 * Each thread will be assigned one collector that is globally unique. This is
 * to ensure that we can collect raw data in an non-blocking way as the
 *collection
 * code runs on critical query path. Periodically a dedicated aggregator thread
 * will put the data from all collectors together into a meaningful form.
 */
class ThreadLevelStatsCollector {
 public:
  using CollectorsMap =
      tbb::concurrent_unordered_map<std::thread::id, ThreadLevelStatsCollector,
                                    std::hash<std::thread::id>>;
  /**
   * @return the Collector for the calling thread
   */
  static ThreadLevelStatsCollector &GetCollectorForThread() {
    static CollectorsMap collector_map;
    std::thread::id tid = std::this_thread::get_id();
    return collector_map[tid];
  }

  /**
   * @return A mapping from each thread to their assigned Collector
   */
  static CollectorsMap &GetAllCollectors() {
    static CollectorsMap collector_map;
    return collector_map;
  };

  ThreadLevelStatsCollector();

  // TODO(tianyu): fill arguments
  inline void CollectTransactionBegin() {
    for (auto &metric : metric_dispatch_[stats_event_type::TXN_BEGIN])
      metric->OnTransactionBegin();
  };

  inline void CollectTransactionCommit(oid_t tile_group_id) {
    for (auto &metric : metric_dispatch_[stats_event_type::TXN_COMMIT])
      metric->OnTransactionCommit(tile_group_id);
  };

  inline void CollectTransactionAbort(oid_t tile_group_id) {
    for (auto &metric : metric_dispatch_[stats_event_type::TXN_ABORT])
      metric->OnTransactionAbort(tile_group_id);
  };

  inline void CollectTupleRead(oid_t tile_group_id, size_t num_read) {
    for (auto &metric : metric_dispatch_[stats_event_type::TUPLE_READ])
      metric->OnTupleRead(tile_group_id, num_read);
  };

  inline void CollectTupleUpdate(oid_t tile_group_id) {
    for (auto &metric : metric_dispatch_[stats_event_type::TUPLE_UPDATE])
      metric->OnTupleUpdate(tile_group_id);
  };

  inline void CollectTupleInsert(oid_t tile_group_id) {
    for (auto &metric : metric_dispatch_[stats_event_type::TUPLE_INSERT])
      metric->OnTupleInsert(tile_group_id);
  };

  inline void CollectTupleDelete(oid_t tile_group_id) {
    for (auto &metric : metric_dispatch_[stats_event_type::TUPLE_DELETE])
      metric->OnTupleDelete(tile_group_id);
  };

  inline void CollectTableMemoryAlloc(oid_t database_id, oid_t table_id,
                                      size_t bytes) {
    if (table_id == INVALID_OID || database_id == INVALID_OID) return;

    for (auto &metric : metric_dispatch_[stats_event_type::TABLE_MEMORY_ALLOC])
      metric->OnMemoryAlloc({database_id, table_id}, bytes);
  };

  inline void CollectTableMemoryFree(oid_t database_id, oid_t table_id,
                                     size_t bytes) {
    if (table_id == INVALID_OID || database_id == INVALID_OID) return;
    for (auto &metric : metric_dispatch_[stats_event_type::TABLE_MEMORY_FREE])
      metric->OnMemoryFree({database_id, table_id}, bytes);
  };

  inline void CollectIndexRead(oid_t database_id, oid_t index_id,
                               size_t num_read) {
    for (auto &metric : metric_dispatch_[stats_event_type::INDEX_READ])
      metric->OnIndexRead({database_id, index_id}, num_read);
  };
  inline void CollectIndexUpdate(oid_t database_id, oid_t index_id) {
    for (auto &metric : metric_dispatch_[stats_event_type::INDEX_UPDATE])
      metric->OnIndexUpdate({database_id, index_id});
  };
  inline void CollectIndexInsert(oid_t database_id, oid_t index_id) {
    for (auto &metric : metric_dispatch_[stats_event_type::INDEX_INSERT])
      metric->OnIndexInsert({database_id, index_id});
  };
  inline void CollectIndexDelete(oid_t database_id, oid_t index_id) {
    for (auto &metric : metric_dispatch_[stats_event_type::INDEX_DELETE])
      metric->OnIndexDelete({database_id, index_id});
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
    for (auto &metric : metrics_) result.push_back(metric->Swap());
    return result;
  }

 private:
  /**
   * Registers a Metric so that its callbacks are invoked.
   * Use this only in the constructor.
   * @tparam metric type of Metric to register
   * @param types A list of event types to receive updates about.
   */
  template <typename metric>
  void RegisterMetric(std::vector<stats_event_type> types) {
    auto m = std::make_shared<metric>();
    metrics_.push_back(m);
    for (stats_event_type type : types) metric_dispatch_[type].push_back(m);
  }

  using MetricList = std::vector<std::shared_ptr<Metric>>;
  /**
   * List of all registered metrics
   */
  MetricList metrics_;
  /**
   * Mapping from each type of event to a list of metrics registered to
   * receive updates from that type of event.
   */
  std::unordered_map<stats_event_type, MetricList, EnumHash<stats_event_type>>
      metric_dispatch_;
};

}  // namespace stats
}  // namespace peloton
