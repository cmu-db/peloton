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

#include "backend/statistics/backend_stats_context.h"

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

    backend_stats[id_] = context_;
    thread_number++;

    stats_mutex.unlock();
  }

  inline void UnregisterContext(std::thread::id id_) {
    stats_mutex.lock();

    backend_stats.erase(id_);
    thread_number--;

    printf("unregister: %d\n", thread_number);

    stats_mutex.unlock();
  }

  BackendStatsContext *GetBackendStatsContext();

 private:
  StatsAggregator();
  ~StatsAggregator();

  BackendStatsContext stats_history;
  BackendStatsContext aggregated_stats;

  std::mutex stats_mutex;

  std::unordered_map<std::thread::id, BackendStatsContext*> backend_stats;

  int thread_number;
};

}  // namespace stats
}  // namespace peloton
