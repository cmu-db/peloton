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

//===--------------------------------------------------------------------===//
// GUC Variables
//===--------------------------------------------------------------------===//


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
    stats_mutex.lock();

    if (backend_stats.find(id_) == backend_stats.end()) {

      backend_stats[id_] = context_;
      thread_number++;
      printf("hash map size: %ld\n", backend_stats.size());

      printf("register: %d\n", thread_number);

      std::cout << id_ << std::endl;
    }

    stats_mutex.unlock();
  }

  inline void UnregisterContext(std::thread::id id_) {
    stats_mutex.lock();

    if (backend_stats.find(id_) != backend_stats.end()) {
      printf("hash map size: %ld\n", backend_stats.size());
      backend_stats.erase(id_);
      printf("unregister: %d\n", thread_number);
      thread_number--;

      //printf("unregister: %d\n", thread_number);
      printf("hash map size: %ld\n", backend_stats.size());
      std::cout << id_ << std::endl;
    }

    stats_mutex.unlock();
  }

  BackendStatsContext *GetBackendStatsContext();

  void RunAggregator();

 private:
  StatsAggregator();
  ~StatsAggregator();

  BackendStatsContext stats_history;
  BackendStatsContext aggregated_stats;

  std::mutex stats_mutex;

  std::unordered_map<std::thread::id, BackendStatsContext*> backend_stats;

  int thread_number;

  // Stats aggregator background thread
  std::thread aggregator_thread;

  // CV to signal aggregator if finished
  std::condition_variable exec_finished;

  std::string peloton_stats_directory = "./stats_log";
  std::ofstream ofs;
};

}  // namespace stats
}  // namespace peloton
