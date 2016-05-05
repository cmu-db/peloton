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
#include "backend/statistics/backend_stats_context.h"

namespace peloton {
namespace stats {

// Each thread gets a backend logger
thread_local BackendStatsContext* backend_stats_context = nullptr;

StatsAggregator::StatsAggregator() {
  thread_number = 0;

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
   while (exec_finished.wait_for(lck,
         std::chrono::milliseconds(STATS_AGGREGATION_INTERVAL_MS)) == std::cv_status::timeout
       ) {
     printf("epoch!\n");

     aggregated_stats.Reset();
     for(auto& val : backend_stats )
     {
       aggregated_stats.Aggregtate((*val.second));
     }
     printf("%s\n", aggregated_stats.ToString().c_str());
     ofs << aggregated_stats.ToString();

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

  }

  return backend_stats_context;
}

}  // namespace stats
}  // namespace peloton
