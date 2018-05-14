//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// stats_aggregator.h
//
// Identification: src/include/statistics/stats_aggregator.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <condition_variable>
#include <fstream>
#include <map>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/logger.h"
#include "common/macros.h"
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

namespace peloton {
namespace stats {

class StatsAggregator : public DedicatedThreadTask {
 public:
  StatsAggregator(int64_t aggregation_interval)
      : aggregation_interval_ms_(aggregation_interval) {}

  void Terminate() override;

  void RunTask() override;

  /**
   * Aggregate metrics from all threads which have collected stats,
   * combine with what was previously in catalog
   * and insert new total into catalog
   */
  void Aggregate();

  std::vector<std::shared_ptr<AbstractRawData>> AggregateRawData();

 private:
  int64_t aggregation_interval_ms_;
  // mutex for aggregate task scheduling. No conflict generally
  std::mutex mutex_;
  std::condition_variable exec_finished_;
  bool exiting_ = false;
};

}  // namespace stats
}  // namespace peloton
