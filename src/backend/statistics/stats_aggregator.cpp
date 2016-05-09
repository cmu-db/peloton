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
  LOG_DEBUG("StatsAggregator destruction\n");
  for (auto& stats_item : backend_stats_) {
    delete stats_item.second;
  }
  ofs_.close();
  exec_finished_.notify_one();
  aggregator_thread_.join();
}

void StatsAggregator::Aggregate(int64_t &interval_cnt, double &alpha,
    double &weighted_avg_throughput) {

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
    // Traverse the metric of all threads to get the total number of committed txns.
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

void StatsAggregator::RunAggregator() {
  std::mutex mtx;
  std::unique_lock<std::mutex> lck(mtx);
  int64_t interval_cnt = 0;
  double alpha = 0.4;
  double weighted_avg_throughput = 0.0;

  while (exec_finished_.wait_for(lck,
       std::chrono::milliseconds(STATS_AGGREGATION_INTERVAL_MS)) == std::cv_status::timeout
     ) {
    Aggregate(interval_cnt, alpha, weighted_avg_throughput);
  }
  printf("Aggregator done!\n");

}

StatsAggregator &StatsAggregator::GetInstance() {
  static StatsAggregator stats_aggregator;
  return stats_aggregator;
}

BackendStatsContext *StatsAggregator::GetBackendStatsContext() {

  // Check whether the backend_stats_context exists or not
  // if not, create a backend_stats_context and store it to thread_local pointer
  if (backend_stats_context == nullptr) {
    backend_stats_context = new BackendStatsContext();

    RegisterContext(backend_stats_context->GetThreadId(), backend_stats_context);

  } else {
    LOG_ERROR("context pointer already had a value!\n");
  }

  return backend_stats_context;
}

}  // namespace stats
}  // namespace peloton
