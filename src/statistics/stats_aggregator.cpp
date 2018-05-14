//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// stats_aggregator.cpp
//
// Identification: src/statistics/stats_aggregator.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "statistics/stats_aggregator.h"
#include <cinttypes>
namespace peloton {
namespace stats {

void StatsAggregator::Terminate() {
  std::unique_lock<std::mutex> lock(mutex_);
  exiting_ = true;
  while (exiting_) exec_finished_.wait(lock);
}

void StatsAggregator::RunTask() {
  LOG_INFO("Aggregator is now running.");
  std::unique_lock<std::mutex> lock(mutex_);
  while (exec_finished_.wait_for(
             lock, std::chrono::milliseconds(aggregation_interval_ms_)) ==
             std::cv_status::timeout &&
         !exiting_)
    Aggregate();
  exiting_ = false;
  exec_finished_.notify_all();
  LOG_INFO("Aggregator done!");
}

using RawDataCollect = std::vector<std::shared_ptr<AbstractRawData>>;
RawDataCollect StatsAggregator::AggregateRawData() {
  RawDataCollect acc;
  for (auto &entry : ThreadLevelStatsCollector::GetAllCollectors()) {
    auto data_block = entry.second.GetDataToAggregate();
    if (acc.empty())
      acc = data_block;
    else
      for (size_t i = 0; i < data_block.size(); i++) {
        acc[i]->Aggregate(*data_block[i]);
      }
  };
  return acc;
}

void StatsAggregator::Aggregate() {
  auto acc = AggregateRawData();
  for (auto &raw_data : acc) {
    raw_data->UpdateAndPersist();
  }
}

}  // namespace stats
}  // namespace peloton
