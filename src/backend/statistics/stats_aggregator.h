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

#define STATS_AGGREGATION_INTERVAL_MS 1000
#define STATS_LOG_INTERVALS 10
#define LATENCY_MAX_HISTORY_THREAD 100
#define LATENCY_MAX_HISTORY_AGGREGATOR 10000

//===--------------------------------------------------------------------===//
// GUC Variables
//===--------------------------------------------------------------------===//

extern StatsType peloton_stats_mode;

namespace peloton {
namespace stats {

extern thread_local BackendStatsContext* backend_stats_context;

//===--------------------------------------------------------------------===//
// Log Manager
//===--------------------------------------------------------------------===//

// Logging basically refers to the PROTOCOL -- like aries or peloton
// Logger refers to the implementation -- like frontend or backend
// Transition diagram :: standby -> recovery -> logging -> terminate -> sleep

/**
 * Global Log Manager
 */
class StatsAggregator {
 public:
  StatsAggregator (const StatsAggregator &) = delete;
  StatsAggregator &operator=(const StatsAggregator &) = delete;
  StatsAggregator (StatsAggregator &&) = delete;
  StatsAggregator &operator=(StatsAggregator &&) = delete;

  // global singleton
  static StatsAggregator  &GetInstance(void);

  inline void RegisterContext(std::thread::id id_, BackendStatsContext *context_) {
    stats_mutex_.lock();

    // FIXME: This is sort of hacky. Eventually we want to free the StatsContext when the thread exit
    if (backend_stats_.find(id_) == backend_stats_.end()) {
      thread_number_++;
    } else {
      stats_history_.Aggregate(*backend_stats_[id_]);
      delete backend_stats_[id_];
    }

    backend_stats_[id_] = context_;
    printf("hash map size: %ld\n", backend_stats_.size());

    printf("register: %d\n", thread_number_);

    std::cout << id_ << std::endl;

    stats_mutex_.unlock();
  }

  inline void UnregisterContext(std::thread::id id) {
    stats_mutex_.lock();

    if (backend_stats_.find(id) != backend_stats_.end()) {
      //printf("hash map size: %ld\n", backend_stats.size());
      backend_stats_.erase(id);
      //printf("unregister: %d\n", thread_number);
      thread_number_--;

      //printf("unregister: %d\n", thread_number);
      //printf("hash map size: %ld\n", backend_stats.size());
      std::cout << id << std::endl;
    }

    stats_mutex_.unlock();
  }

  inline const BackendStatsContext& GetStatsHistory() {
    return stats_history_;
  }

  inline const BackendStatsContext& GetAggregatedStats() {
    return aggregated_stats_;
  }

  BackendStatsContext *GetBackendStatsContext();
  
  void Aggregate(int64_t &interval_cnt, double &alpha,
      double &weighted_avg_throughput);
  void RunAggregator();

  StatsAggregator();
  ~StatsAggregator();

 private:
  BackendStatsContext stats_history_;
  BackendStatsContext aggregated_stats_;

  std::mutex stats_mutex_;

  std::unordered_map<std::thread::id, BackendStatsContext*> backend_stats_;

  int thread_number_;

  int64_t total_prev_txn_committed_;

  // Stats aggregator background thread
  std::thread aggregator_thread_;

  // CV to signal aggregator if finished
  std::condition_variable exec_finished_;

  std::string peloton_stats_directory_ = "./stats_log";
  std::ofstream ofs_;
};

}  // namespace stats
}  // namespace peloton
