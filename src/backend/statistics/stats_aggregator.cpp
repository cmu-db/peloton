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
  thread_number_ = 0;

  total_prev_txn_committed_ = 0;

  ofs_.open (peloton_stats_directory_, std::ofstream::out);

  aggregator_thread_ = std::thread(&StatsAggregator::RunAggregator, this);
}

StatsAggregator::~StatsAggregator() {
  printf("StatsAggregator destruction\n");
  for (auto& stats_item : backend_stats_) {
    delete stats_item.second;
  }
  ofs_.close();
  exec_finished_.notify_one();
  aggregator_thread_.join();
}

void StatsAggregator::RunAggregatorOnce() {
    std::mutex mtx;
    std::unique_lock<std::mutex> lck(mtx);
    int64_t interval_cnt = 0;
    double alpha = 0.4;
    double weighted_avg_throughput = 0.0;

    interval_cnt++;
    printf("\n////////////////////////////////////////////////////////////////////////////////////////////////////////////\n");
    printf("TIME ELAPSED: %ld sec\n", interval_cnt);
    
    aggregated_stats_.Reset();
    for(auto& val : backend_stats_ ) {
      aggregated_stats_.Aggregate((*val.second));
    }
    aggregated_stats_.Aggregate(stats_history_);
    printf("%s", aggregated_stats_.ToString().c_str());
    int64_t current_txns_committed = 0;
    for (auto database_item : aggregated_stats_.database_metrics_) {
      current_txns_committed += database_item.second->GetTxnCommitted().GetCounter();
    }
    int64_t txns_committed_this_interval =
      current_txns_committed - total_prev_txn_committed_;
    double throughput_ = (double)txns_committed_this_interval
	/ 1000 * STATS_AGGREGATION_INTERVAL_MS;
    double avg_throughput_ = (double)current_txns_committed
      / interval_cnt / STATS_AGGREGATION_INTERVAL_MS * 1000;
    if (interval_cnt == 1) {
      weighted_avg_throughput = throughput_;
    } else {
      weighted_avg_throughput = alpha * throughput_ + (1 - alpha) * weighted_avg_throughput;
    }
    
    total_prev_txn_committed_ = current_txns_committed;
    printf("Average throughput:     %lf txn/s\n", avg_throughput_);
    printf("Moving avg. throughput: %lf txn/s\n", weighted_avg_throughput);
    printf("Current throughput:     %lf txn/s\n\n", throughput_);
    if (interval_cnt % STATS_LOG_INTERVALS == 0) {
      ofs_ << "At interval: " << interval_cnt << std::endl;
      ofs_ << aggregated_stats_.ToString();
      ofs_ << weighted_avg_throughput << std::endl;
    }
    
    printf("Aggregator done!\n");
}

void StatsAggregator::RunAggregator() {
  std::mutex mtx;
  std::unique_lock<std::mutex> lck(mtx);
  int64_t interval_cnt = 0;
  double alpha = 0.4;
  double weighted_avg_throughput = 0.0;

  while (exec_finished_.wait_for(lck,
       std::chrono::milliseconds(STATS_AGGREGATION_INTERVAL_MS)) == std::cv_status::timeout
     ) {
   if (peloton_stats_mode == STATS_TYPE_INVALID) {
     printf("stats invalid!\n");
   }
   interval_cnt++;
   printf("\n////////////////////////////////////////////////////////////////////////////////////////////////////////////\n");
   printf("TIME ELAPSED: %ld sec\n", interval_cnt);

   aggregated_stats_.Reset();
   for(auto& val : backend_stats_ )
   {
     aggregated_stats_.Aggregate((*val.second));
   }
   aggregated_stats_.Aggregate(stats_history_);
   printf("%s", aggregated_stats_.ToString().c_str());
   int64_t current_txns_committed = 0;
   for (auto database_item : aggregated_stats_.database_metrics_) {
     current_txns_committed += database_item.second->GetTxnCommitted().GetCounter();
   }
   int64_t txns_committed_this_interval =
       current_txns_committed - total_prev_txn_committed_;
   double throughput_ = (double)txns_committed_this_interval
           / 1000 * STATS_AGGREGATION_INTERVAL_MS;
   double avg_throughput_ = (double)current_txns_committed
           / interval_cnt / STATS_AGGREGATION_INTERVAL_MS * 1000;
   if (interval_cnt == 1) {
     weighted_avg_throughput = throughput_;
   } else {
     weighted_avg_throughput = alpha * throughput_ + (1 - alpha) * weighted_avg_throughput;
   }

   total_prev_txn_committed_ = current_txns_committed;
   printf("Average throughput:     %lf txn/s\n", avg_throughput_);
   printf("Moving avg. throughput: %lf txn/s\n", weighted_avg_throughput);
   printf("Current throughput:     %lf txn/s\n\n", throughput_);
   if (interval_cnt % STATS_LOG_INTERVALS == 0) {
     ofs_ << "At interval: " << interval_cnt << std::endl;
     ofs_ << aggregated_stats_.ToString();
     ofs_ << weighted_avg_throughput << std::endl;
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
