//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// stats_aggregator.h
//
// Identification: src/backend/statistics/stats_aggregator.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>
#include <map>
#include <vector>
#include <unordered_map>
#include <iostream>
#include <condition_variable>
#include <string>
#include <fstream>

#include "backend/statistics/backend_stats_context.h"
#include "backend/common/logger.h"

//===--------------------------------------------------------------------===//
// GUC Variables
//===--------------------------------------------------------------------===//

#define STATS_AGGREGATION_INTERVAL_MS 1000
#define STATS_LOG_INTERVALS 10
#define LATENCY_MAX_HISTORY_THREAD 100
#define LATENCY_MAX_HISTORY_AGGREGATOR 10000

extern StatsType peloton_stats_mode;

namespace peloton {
namespace stats {

extern thread_local BackendStatsContext *backend_stats_context;

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
class StatsAggregator {
 public:
  StatsAggregator(const StatsAggregator &) = delete;
  StatsAggregator &operator=(const StatsAggregator &) = delete;
  StatsAggregator(StatsAggregator &&) = delete;
  StatsAggregator &operator=(StatsAggregator &&) = delete;

  StatsAggregator();
  StatsAggregator(int64_t aggregation_interval_ms);
  ~StatsAggregator();

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  // Global singleton
  static StatsAggregator &GetInstance(void);

  static StatsAggregator &GetInstanceForTest(void);

  // Get the aggregated stats history of all exited threads
  inline BackendStatsContext &GetStatsHistory() { return stats_history_; }

  // Get the current aggregated stats of all threads (including history)
  inline BackendStatsContext &GetAggregatedStats() { return aggregated_stats_; }

  // Allocate a BackendStatsContext for a new thread
  BackendStatsContext *GetBackendStatsContext();

  //===--------------------------------------------------------------------===//
  // HELPER FUNCTIONS
  //===--------------------------------------------------------------------===//

  // Register the BackendStatsContext of a worker thread to global Stats
  // Aggregator
  inline void RegisterContext(std::thread::id id_,
                              BackendStatsContext *context_) {
    {
      std::lock_guard<std::mutex> lock(stats_mutex_);

      // FIXME: This is sort of hacky. Eventually we want to free the
      // StatsContext
      // when the thread exit
      if (backend_stats_.find(id_) == backend_stats_.end()) {
        thread_number_++;
      } else {
        stats_history_.Aggregate(*backend_stats_[id_]);
        delete backend_stats_[id_];
      }
      backend_stats_[id_] = context_;
    }
    LOG_DEBUG("Stats aggregator hash map size: %ld\n", backend_stats_.size());
  }

  // Unregister a BackendStatsContext. Currently we directly reuse the thread id
  // instead of explicitly unregistering it.
  inline void UnregisterContext(std::thread::id id) {
    {
      std::lock_guard<std::mutex> lock(stats_mutex_);

      if (backend_stats_.find(id) != backend_stats_.end()) {
        stats_history_.Aggregate(*backend_stats_[id]);
        delete backend_stats_[id];
        backend_stats_.erase(id);
        thread_number_--;
      }
    }
  }

  // Aggregate the stats of current living threads
  void Aggregate(int64_t &interval_cnt, double &alpha,
                 double &weighted_avg_throughput);

  // Aggregate stats periodically
  void RunAggregator();

 private:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // Stores stats of exited threads
  BackendStatsContext stats_history_;

  // Stores all aggregated stats
  BackendStatsContext aggregated_stats_;

  // Protect register and unregister of BackendStatsContext*
  std::mutex stats_mutex_;

  // Map the thread id to the pointer of its BackendStatsContext
  std::unordered_map<std::thread::id, BackendStatsContext *> backend_stats_;

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
  std::ofstream ofs_;
};

}  // namespace stats
}  // namespace peloton
