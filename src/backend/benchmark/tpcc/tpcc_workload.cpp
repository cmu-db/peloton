//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// workload.cpp
//
// Identification: benchmark/tpcc/workload.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <iostream>
#include <ctime>
#include <cassert>
#include <thread>
#include <algorithm>
#include <random>
#include <cstddef>
#include <limits>


#include "backend/benchmark/tpcc/tpcc_workload.h"
#include "backend/benchmark/tpcc/tpcc_configuration.h"
#include "backend/benchmark/tpcc/tpcc_loader.h"

#include "backend/catalog/manager.h"
#include "backend/catalog/schema.h"

#include "backend/common/types.h"
#include "backend/common/value.h"
#include "backend/common/value_factory.h"
#include "backend/common/logger.h"
#include "backend/common/timer.h"
#include "backend/common/generator.h"

#include "backend/concurrency/transaction.h"

#include "backend/executor/executor_context.h"
#include "backend/executor/abstract_executor.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/executor/materialization_executor.h"
#include "backend/executor/update_executor.h"
#include "backend/executor/index_scan_executor.h"

#include "backend/expression/abstract_expression.h"
#include "backend/expression/constant_value_expression.h"
#include "backend/expression/tuple_value_expression.h"
#include "backend/expression/comparison_expression.h"
#include "backend/expression/expression_util.h"

#include "backend/index/index_factory.h"

#include "backend/logging/log_manager.h"

#include "backend/planner/abstract_plan.h"
#include "backend/planner/materialization_plan.h"
#include "backend/planner/insert_plan.h"
#include "backend/planner/update_plan.h"
#include "backend/planner/index_scan_plan.h"

#include "backend/storage/data_table.h"
#include "backend/storage/table_factory.h"

namespace peloton {
namespace benchmark {
namespace tpcc {

/////////////////////////////////////////////////////////
// TRANSACTION TYPES
/////////////////////////////////////////////////////////

void RunStockLevel();

void RunDelivery();

void RunOrderStatus();

void RunPayment();

void RunNewOrder();

/////////////////////////////////////////////////////////
// WORKLOAD
/////////////////////////////////////////////////////////

std::vector<double> durations;

void RunBackend(oid_t thread_id) {
  auto txn_count = state.transaction_count;

  UniformGenerator generator;
  Timer<> timer;

  // Start timer
  timer.Reset();
  timer.Start();

  // Run these many transactions
  for (oid_t txn_itr = 0; txn_itr < txn_count; txn_itr++) {
    auto rng_val = generator.GetSample();

    if (rng_val <= 0.04) {
      RunStockLevel();
    } else if (rng_val <= 0.08) {
      RunDelivery();
    } else if (rng_val <= 0.12) {
      RunOrderStatus();
    } else if (rng_val <= 0.55) {
      RunPayment();
    } else {
      RunNewOrder();
    }

  }

  // Stop timer
  timer.Stop();

  // Set duration
  durations[thread_id] = timer.GetDuration();
}

double RunWorkload() {

  // Execute the workload to build the log
  std::vector<std::thread> thread_group;
  oid_t num_threads = state.backend_count;
  double max_duration = std::numeric_limits<double>::min();
  durations.reserve(num_threads);

  // Launch a group of threads
  for (oid_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::thread(RunBackend, thread_itr));
  }

  // Join the threads with the main thread
  for (oid_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }

  // Compute max duration
  for (oid_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    max_duration = std::max(max_duration, durations[thread_itr]);
  }

  double throughput = (state.transaction_count * num_threads)/max_duration;

  return throughput;
}

/////////////////////////////////////////////////////////
// HARNESS
/////////////////////////////////////////////////////////

static void ExecuteTest(std::vector<executor::AbstractExecutor *> &executors) {
  time_point_ start, end;
  bool status = false;

  // Run all the executors
  for (auto executor : executors) {
    status = executor->Init();
    if (status == false) {
      throw Exception("Init failed");
    }

    std::vector<std::unique_ptr<executor::LogicalTile>> result_tiles;

    // Execute stuff
    while (executor->Execute() == true) {
      std::unique_ptr<executor::LogicalTile> result_tile(
          executor->GetOutput());
      result_tiles.emplace_back(result_tile.release());
    }
  }

}

/////////////////////////////////////////////////////////
// TRANSACTIONS
/////////////////////////////////////////////////////////

void RunStockLevel() {
  std::vector<executor::AbstractExecutor *> executors;
  ExecuteTest(executors);
}

void RunDelivery(){

}

void RunOrderStatus(){

}

void RunPayment(){

}

void RunNewOrder(){

}

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton
