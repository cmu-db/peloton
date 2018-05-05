//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// stats_aggregator.h
//
// Identification: src/statistics/stats_aggregator.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>
#include <map>
#include <vector>
#include <unordered_map>
#include <condition_variable>
#include <string>
#include <fstream>

#include "common/logger.h"
#include "common/macros.h"
#include "statistics/backend_stats_context.h"
#include "storage/database.h"
#include "storage/data_table.h"
#include "concurrency/transaction_context.h"
#include "common/dedicated_thread_task.h"
#include "thread_level_stats_collector.h"
#include "type/ephemeral_pool.h"

//===--------------------------------------------------------------------===//
// GUC Variables
//===--------------------------------------------------------------------===//

#define STATS_AGGREGATION_INTERVAL_MS 1000
#define STATS_LOG_INTERVALS 10
#define LATENCY_MAX_HISTORY_THREAD 100
#define LATENCY_MAX_HISTORY_AGGREGATOR 10000

class BackendStatsContext;

namespace peloton {
namespace stats {

class StatsAggregator : public DedicatedThreadTask {
 public:
  StatsAggregator(int64_t aggregation_interval)
      : aggregation_interval_ms_(aggregation_interval), lock_(mutex_) {}

  void Terminate() override {
    lock_.lock();
    exiting_ = true;
    while (exiting_) exec_finished_.wait(lock_);
    lock_.unlock();
  }

  void RunTask() override {
    LOG_INFO("Aggregator is now running.");

    while (exec_finished_.wait_for(
               lock_, std::chrono::milliseconds(aggregation_interval_ms_)) ==
               std::cv_status::timeout &&
           !exiting_)
      Aggregate();
    exiting_ = false;
    exec_finished_.notify_all();
    LOG_INFO("Aggregator done!");
  }

  void Aggregate() {
    std::vector<std::shared_ptr<AbstractRawData>> acc;
    for (auto &entry : ThreadLevelStatsCollector::GetAllCollectors()) {
      auto data_block = entry.second.GetDataToAggregate();
      if (acc.empty())
        acc = data_block;
      else
        for (size_t i = 0; i < acc.size(); i++)
          acc[i]->Aggregate(*data_block[i]);
    }
    for (auto &raw_data : acc) {
      raw_data->FetchData();
      raw_data->WriteToCatalog();
    }
  }

 private:
  int64_t aggregation_interval_ms_;
  std::mutex mutex_;
  std::unique_lock<std::mutex> lock_;
  std::condition_variable exec_finished_;
  bool exiting_ = false;
};
//===--------------------------------------------------------------------===//
// Stats Aggregator
//===--------------------------------------------------------------------===//

// One singleton stats aggregator over the whole DBMS. Worker threads register
// their BackendStatsContext pointer to this aggregator. And this singleton
// aggregator call Aggregate() periodically to aggregate stats from all worker
// threads. Then print them out or log them into a file.

/**
 * Global Stats Aggregator
 */
class StatsAggregatorOld {
 public:
  StatsAggregatorOld(const StatsAggregatorOld &) = delete;
  StatsAggregatorOld &operator=(const StatsAggregatorOld &) = delete;
  StatsAggregatorOld(StatsAggregatorOld &&) = delete;
  StatsAggregatorOld &operator=(StatsAggregatorOld &&) = delete;

  StatsAggregatorOld(int64_t aggregation_interval_ms);
  ~StatsAggregatorOld();

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  // Global singleton
  static StatsAggregatorOld &GetInstance(
      int64_t aggregation_interval_ms = STATS_AGGREGATION_INTERVAL_MS);

  // Get the aggregated stats history of all exited threads
  inline BackendStatsContext &GetStatsHistory() { return stats_history_; }

  // Get the current aggregated stats of all threads (including history)
  inline BackendStatsContext &GetAggregatedStats() { return aggregated_stats_; }

  //===--------------------------------------------------------------------===//
  // HELPER FUNCTIONS
  //===--------------------------------------------------------------------===//

  // Register the BackendStatsContext of a worker thread to global Stats
  // Aggregator
  void RegisterContext(std::thread::id id_, BackendStatsContext *context_);

  // Unregister a BackendStatsContext. Currently we directly reuse the thread id
  // instead of explicitly unregistering it.
  void UnregisterContext(std::thread::id id);

  // Utility function to get the metric table
  storage::DataTable *GetMetricTable(std::string table_name);

  // Aggregate the stats of current living threads
  void Aggregate(int64_t &interval_cnt, double &alpha,
                 double &weighted_avg_throughput);

  // Launch aggregator thread
  void LaunchAggregator();

  // Terminate aggregator thread
  void ShutdownAggregator();

 private:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // Stores stats of exited threads
  BackendStatsContext stats_history_;

  // Stores all aggregated stats
  BackendStatsContext aggregated_stats_;

  // Set of tile groups
  std::unordered_set<oid_t> tile_group_ids_;

  // Protect register and unregister of BackendStatsContext*
  std::mutex stats_mutex_{};

  // Map the thread id to the pointer of its BackendStatsContext
  std::unordered_map<std::thread::id, BackendStatsContext *> backend_stats_{};

  // How often to aggregate all worker thread stats
  int64_t aggregation_interval_ms_;

  // Number of threads registered
  int thread_number_;

  int64_t total_prev_txn_committed_;

  // Stats aggregator background thread
  std::thread aggregator_thread_;

  // CV to signal aggregator if finished
  std::condition_variable exec_finished_;

  // Output path of the stats log
  std::string peloton_stats_directory_ = "./stats_log";

  // Output file stream
  std::ofstream ofs_;

  // Whether the aggregator is running
  bool is_aggregating_ = false;

  // Abstract Pool to hold query strings
  std::unique_ptr<type::AbstractPool> pool_;

  //===--------------------------------------------------------------------===//
  // HELPER FUNCTIONS
  //===--------------------------------------------------------------------===//

  // Write all metrics to metric tables
  void UpdateMetrics();

  // Update the table metrics with a given database
  void UpdateTableMetrics(storage::Database *database, int64_t time_stamp,
                          concurrency::TransactionContext *txn);

  // Update the index metrics with a given table
  void UpdateIndexMetrics(storage::Database *database,
                          storage::DataTable *table, int64_t time_stamp,
                          concurrency::TransactionContext *txn);

  // Write all query metrics to a metric table
  void UpdateQueryMetrics(int64_t time_stamp,
                          concurrency::TransactionContext *txn);

  // Aggregate stats periodically
  void RunAggregator();

  /**
   * @brief Actively collect stats
   */
  void ActiveCollect();
};

}  // namespace stats
}  // namespace peloton
