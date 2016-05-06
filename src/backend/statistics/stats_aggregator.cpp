//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// stats_aggregator.cpp
//
// Identification: src/backend/statistics/stats_aggregator.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <condition_variable>
#include <memory>
#include <fstream>

#include "backend/statistics/stats_aggregator.h"
#include "backend/statistics/backend_stats_context.h"

namespace peloton {
namespace stats {

// Each thread gets a backend logger
thread_local BackendStatsContext* backend_stats_context = nullptr;

StatsAggregator::StatsAggregator() {
  thread_number = 0;

  total_prev_txn_committed = 0;

  ofs.open (peloton_stats_directory, std::ofstream::out);

  aggregator_thread = std::thread(&StatsAggregator::RunAggregator, this);
}

StatsAggregator::~StatsAggregator() {
  printf("StatsAggregator destruction\n");
  ofs.close();
  exec_finished.notify_one();
  aggregator_thread.join();
}

void StatsAggregator::RunAggregator() {
  std::mutex mtx;
   std::unique_lock<std::mutex> lck(mtx);
   int64_t interval_cnt_ = 0;
   while (exec_finished.wait_for(lck,
         std::chrono::milliseconds(STATS_AGGREGATION_INTERVAL_MS)) == std::cv_status::timeout
       ) {
     interval_cnt_++;
     printf("At interval: %ld\n", interval_cnt_);


     aggregated_stats.Reset();
     for(auto& val : backend_stats )
     {
       aggregated_stats.Aggregate((*val.second));
     }
     aggregated_stats.Aggregate(stats_history);
     printf("%s", aggregated_stats.ToString().c_str());
     int64_t current_txns_committed = 0;
     for (auto database_item : aggregated_stats.database_metrics_) {
       current_txns_committed += database_item.second->GetTxnCommitted().GetCounter();
     }
     int64_t txns_committed_this_interval =
         current_txns_committed - total_prev_txn_committed;
     double throughput_ = (double)txns_committed_this_interval
             / interval_cnt_ * 1000 / STATS_AGGREGATION_INTERVAL_MS;
     total_prev_txn_committed = current_txns_committed;
     printf("Throughput: %lf txn/s\n\n", throughput_);
     if (interval_cnt_ % STATS_LOG_INTERVALS == 0) {
       ofs << "At interval: " << interval_cnt_ << std::endl;
       ofs << aggregated_stats.ToString();
       ofs << throughput_ << std::endl;
     }

   }
   printf("Aggregator done!\n");

}

/**
 * @brief Return the singleton log manager instance
 */
StatsAggregator &StatsAggregator::GetInstance() {
  static StatsAggregator stats_aggregator;
  return stats_aggregator;
}

/**
 * @brief Return the backend logger based on logging type
    and store it into the vector
 * @param logging type can be stdout(debug), aries, peloton
 */
BackendStatsContext *StatsAggregator::GetBackendStatsContext() {

  // Check whether the backend logger exists or not
  // if not, create a backend logger and store it in frontend logger
  if (backend_stats_context == nullptr) {
    backend_stats_context = new BackendStatsContext();

    RegisterContext(backend_stats_context->GetThreadId(), backend_stats_context);

  } else {
    printf("context pointer already had a value!\n");
  }

  return backend_stats_context;
}

}  // namespace stats
}  // namespace peloton
