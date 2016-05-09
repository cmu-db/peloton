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

#define STATS_AGGREGATION_INTERVAL_MS 1000
#define STATS_LOG_INTERVALS 10

//===--------------------------------------------------------------------===//
// GUC Variables
//===--------------------------------------------------------------------===//

extern StatsType peloton_stats_mode;

namespace peloton {
namespace stats {

extern thread_local BackendStatsContext* backend_stats_context;

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
  StatsAggregator (const StatsAggregator &) = delete;
  StatsAggregator &operator=(const StatsAggregator &) = delete;
  StatsAggregator (StatsAggregator &&) = delete;
  StatsAggregator &operator=(StatsAggregator &&) = delete;

  // global singleton
  static StatsAggregator  &GetInstance(void);

  // Register the BackendStatsContext of a worker thread to global Stats Aggregator
  inline void RegisterContext(std::thread::id id_, BackendStatsContext *context_) {
    stats_mutex_.lock();

    // FIXME: This is sort of hacky. Eventually we want to free the StatsContext
    // when the thread exit
    if (backend_stats_.find(id_) == backend_stats_.end()) {
      thread_number_++;
    } else {
      stats_history_.Aggregate(*backend_stats_[id_]);
      delete backend_stats_[id_];
    }

    LOG_DEBUG("Stats aggregator hash map size: %ld\n", backend_stats_.size());

    LOG_DEBUG("# registered thread: %d\n", thread_number_);

    backend_stats_[id_] = context_;

    // print out the id of the thread
    //std::cout << id_ << std::endl;

    stats_mutex_.unlock();
  }

  // Unregister a BackendStatsContext. Currently we directly reuse the thread id
  // instread of explicitly unregister it.
  inline void UnregisterContext(std::thread::id id) {
    stats_mutex_.lock();

    if (backend_stats_.find(id) != backend_stats_.end()) {
      backend_stats_.erase(id);
      thread_number_--;

      // print out the id of the thread
      //std::cout << id << std::endl;
    }

    stats_mutex_.unlock();
  }

  // Get the aggregated stats history of all exited threads
  inline const BackendStatsContext& GetStatsHistory() {
    return stats_history_;
  }

  // Get the current aggregated stats of all threads (including history)
  inline const BackendStatsContext& GetAggregatedStats() {
    return aggregated_stats_;
  }

  // Allocate a BackendStatsContext for a new thread
  BackendStatsContext *GetBackendStatsContext();
  
  // Aggregate the stats of current living threads
  void Aggregate(int64_t &interval_cnt, double &alpha,
      double &weighted_avg_throughput);

  // Aggregate stats periodically
  void RunAggregator();

  StatsAggregator();
  ~StatsAggregator();

 private:
  BackendStatsContext stats_history_;
  BackendStatsContext aggregated_stats_;

  // Protect register and unregister of BackendStatsContext*
  std::mutex stats_mutex_;

  // Map the thread id to the pointer of its BackendStatsContext
  std::unordered_map<std::thread::id, BackendStatsContext*> backend_stats_;

  int64_t total_prev_txn_committed_;

  int thread_number_;

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
