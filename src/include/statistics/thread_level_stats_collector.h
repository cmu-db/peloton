//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// stats_collector.h
//
// Identification: src/include/statistics/thread_level_stats_collector.h
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

namespace peloton {

namespace concurrency {
class TransactionContext;
} // namespace concurrency

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
    std::thread::id tid = std::this_thread::get_id();
    return collector_map_[tid];
  }

  /**
   * @return A mapping from each thread to their assigned Collector
   */
  static CollectorsMap &GetAllCollectors() { return collector_map_; };

  /**
   * @brief Constructor of collector
   */
  ThreadLevelStatsCollector();

  /**
   * @brief Destructor of collector
   */
  ~ThreadLevelStatsCollector();

  /* See Metric for documentation on the following methods. They should correspond
   * to the "OnXxx" methods one-to-one */
  inline void CollectTransactionBegin(const concurrency::TransactionContext *txn) {
    for (auto &metric : metric_dispatch_[StatsEventType::TXN_BEGIN])
      metric->OnTransactionBegin(txn);
  };

  inline void CollectTransactionCommit(const concurrency::TransactionContext *txn,
                                       oid_t tile_group_id) {
    for (auto &metric : metric_dispatch_[StatsEventType::TXN_COMMIT])
      metric->OnTransactionCommit(txn, tile_group_id);
  };

  inline void CollectTransactionAbort(const concurrency::TransactionContext *txn,
                                      oid_t tile_group_id) {
    for (auto &metric : metric_dispatch_[StatsEventType::TXN_ABORT])
      metric->OnTransactionAbort(txn, tile_group_id);
  };

  inline void CollectTupleRead(const concurrency::TransactionContext *current_txn,
                               oid_t tile_group_id) {
    for (auto &metric : metric_dispatch_[StatsEventType::TUPLE_READ])
      metric->OnTupleRead(current_txn, tile_group_id);
  };
  inline void CollectTupleUpdate(const concurrency::TransactionContext *current_txn,
                                 oid_t tile_group_id) {
    for (auto &metric : metric_dispatch_[StatsEventType::TUPLE_UPDATE])
      metric->OnTupleUpdate(current_txn, tile_group_id);
  };
  inline void CollectTupleInsert(const concurrency::TransactionContext *current_txn,
                                 oid_t tile_group_id) {
    for (auto &metric : metric_dispatch_[StatsEventType::TUPLE_INSERT])
      metric->OnTupleInsert(current_txn, tile_group_id);
  };
  inline void CollectTupleDelete(const concurrency::TransactionContext *current_txn,
                                 oid_t tile_group_id) {
    for (auto &metric : metric_dispatch_[StatsEventType::TUPLE_DELETE])
      metric->OnTupleDelete(current_txn, tile_group_id);
  };
  inline void CollectTableMemoryAlloc(oid_t database_id, oid_t table_id,
                                      size_t bytes) {
    if (table_id == INVALID_OID || database_id == INVALID_OID) return;

    for (auto &metric : metric_dispatch_[StatsEventType::TABLE_MEMORY_ALLOC])
      metric->OnMemoryAlloc({database_id, table_id}, bytes);
  };
  inline void CollectTableMemoryFree(oid_t database_id, oid_t table_id,
                                     size_t bytes) {
    if (table_id == INVALID_OID || database_id == INVALID_OID) return;
    for (auto &metric : metric_dispatch_[StatsEventType::TABLE_MEMORY_FREE])
      metric->OnMemoryFree({database_id, table_id}, bytes);
  };
  inline void CollectIndexRead(oid_t database_id, oid_t index_id,
                               size_t num_read) {
    for (auto &metric : metric_dispatch_[StatsEventType::INDEX_READ])
      metric->OnIndexRead({database_id, index_id}, num_read);
  };
  inline void CollectIndexUpdate(oid_t database_id, oid_t index_id) {
    for (auto &metric : metric_dispatch_[StatsEventType::INDEX_UPDATE])
      metric->OnIndexUpdate({database_id, index_id});
  };
  inline void CollectIndexInsert(oid_t database_id, oid_t index_id) {
    for (auto &metric : metric_dispatch_[StatsEventType::INDEX_INSERT])
      metric->OnIndexInsert({database_id, index_id});
  };
  inline void CollectIndexDelete(oid_t database_id, oid_t index_id) {
    for (auto &metric : metric_dispatch_[StatsEventType::INDEX_DELETE])
      metric->OnIndexDelete({database_id, index_id});
  };
  inline void CollectIndexMemoryAlloc(oid_t database_id, oid_t index_id,
                                      size_t bytes) {
    for (auto &metric : metric_dispatch_[StatsEventType::INDEX_MEMORY_ALLOC])
      metric->OnMemoryAlloc({database_id, index_id}, bytes);
  };
  inline void CollectIndexMemoryUsage(oid_t database_id, oid_t index_id,
                                      size_t bytes) {
    for (auto &metric : metric_dispatch_[StatsEventType::INDEX_MEMORY_USAGE])
      metric->OnMemoryUsage({database_id, index_id}, bytes);
  };
  inline void CollectIndexMemoryFree(oid_t database_id, oid_t index_id,
                                     size_t bytes) {
    for (auto &metric : metric_dispatch_[StatsEventType::INDEX_MEMORY_FREE])
      metric->OnMemoryFree({database_id, index_id}, bytes);
  };
  inline void CollectIndexMemoryReclaim(oid_t database_id, oid_t index_id,
                                        size_t bytes) {
    for (auto &metric : metric_dispatch_[StatsEventType::INDEX_MEMORY_RECLAIM])
      metric->OnMemoryReclaim({database_id, index_id}, bytes);
  };
  inline void CollectQueryBegin() {
    for (auto &metric : metric_dispatch_[StatsEventType::QUERY_BEGIN])
      metric->OnQueryBegin();
  };
  inline void CollectQueryEnd() {
    for (auto &metric : metric_dispatch_[StatsEventType::QUERY_END])
      metric->OnQueryEnd();
  };
  inline void CollectTestNum(int number) {
    for (auto &metric : metric_dispatch_[StatsEventType::TEST])
      metric->OnTest(number);
  }

  /**
   * @return A vector of raw data, for each registered metric. Each piece of
   * data is guaranteed to be safe to read and remove, and the same type of
   * metric is guaranteed to be in the same positopn in the returned vector
   * for different instances of Collector.
   */
  std::vector<std::shared_ptr<AbstractRawData>> GetDataToAggregate();

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
    for (StatsEventType type : types) metric_dispatch_[type].push_back(m);
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
  std::unordered_map<StatsEventType, MetricList, EnumHash<StatsEventType>>
      metric_dispatch_;

  static CollectorsMap collector_map_;
};

}  // namespace stats
}  // namespace peloton
