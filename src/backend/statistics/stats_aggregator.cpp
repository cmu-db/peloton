//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_manager.cpp
//
// Identification: src/backend/logging/log_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <condition_variable>
#include <memory>

/*
#include "backend/logging/log_manager.h"
#include "backend/logging/records/transaction_record.h"
#include "backend/common/logger.h"
#include "backend/executor/executor_context.h"
#include "backend/catalog/manager.h"
#include "backend/storage/tuple.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/data_table.h"
*/

#include "backend/statistics/stats_aggregator.h"

namespace peloton {
namespace stats {

// Each thread gets a backend logger
thread_local static int* backend_stats_context = nullptr;

StatsAggregator::StatsAggregator() {
  thread_number = 0;
}

StatsAggregator::~StatsAggregator() {
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
 int *StatsAggregator::GetBackendStatsContext() {

  // Check whether the backend logger exists or not
  // if not, create a backend logger and store it in frontend logger
  if (backend_stats_context == nullptr) {
    backend_stats_context = new int;
    *backend_stats_context = 0;
    thread_number += 1;
    printf("register: %d\n", thread_number);
  }

  return backend_stats_context;
}

}  // namespace logging
}  // namespace peloton
