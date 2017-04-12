//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ycsb_workload.cpp
//
// Identification: src/main/ycsb/ycsb_workload.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <iostream>
#include <ctime>
#include <thread>
#include <algorithm>
#include <random>
#include <cstddef>
#include <limits>

#include "benchmark/ycsb/ycsb_workload.h"
#include "benchmark/ycsb/ycsb_configuration.h"
#include "benchmark/ycsb/ycsb_loader.h"

#include "catalog/manager.h"
#include "catalog/schema.h"

#include "type/types.h"
#include "type/value.h"
#include "type/value_factory.h"
#include "common/logger.h"
#include "common/timer.h"
#include "common/generator.h"
#include "common/platform.h"

#include "concurrency/transaction.h"
#include "concurrency/transaction_manager_factory.h"

#include "executor/executor_context.h"
#include "executor/abstract_executor.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "executor/materialization_executor.h"
#include "executor/update_executor.h"
#include "executor/index_scan_executor.h"

#include "expression/abstract_expression.h"
#include "expression/constant_value_expression.h"
#include "expression/tuple_value_expression.h"
#include "expression/comparison_expression.h"
#include "expression/expression_util.h"

#include "index/index_factory.h"

#include "logging/log_manager.h"

#include "planner/abstract_plan.h"
#include "planner/materialization_plan.h"
#include "planner/insert_plan.h"
#include "planner/update_plan.h"
#include "planner/index_scan_plan.h"

#include "storage/data_table.h"
#include "storage/table_factory.h"

namespace peloton {
namespace benchmark {
namespace ycsb {


/////////////////////////////////////////////////////////
// WORKLOAD
/////////////////////////////////////////////////////////

volatile bool is_running = true;

PadInt *abort_counts;
PadInt *commit_counts;

void PinToCore(size_t core) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core, &cpuset);
  pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
}

void RunBackend(const size_t thread_id) {
  
  PinToCore(thread_id);
  
  PadInt &execution_count_ref = abort_counts[thread_id];
  PadInt &transaction_count_ref = commit_counts[thread_id];

  ZipfDistribution zipf((state.scale_factor * 1000) - 1,
                        state.zipf_theta);

  FastRandom rng(rand());

  // backoff
  uint32_t backoff_shifts = 0;

  while (true) {
    if (is_running == false) {
      break;
    }
    while (RunMixed(thread_id, zipf, rng) == false) {
      if (is_running == false) {
        break;
      }
      execution_count_ref.data++;
      // backoff
      if (state.exp_backoff) {
        if (backoff_shifts < 13) {
          ++backoff_shifts;
        }
        uint64_t sleep_duration = 1UL << backoff_shifts;
        sleep_duration *= 100;
        std::this_thread::sleep_for(std::chrono::microseconds(sleep_duration));
      }
    }
    backoff_shifts >>= 1;
    transaction_count_ref.data++;
  }
}

void RunWorkload() {
  // Execute the workload to build the log
  std::vector<std::thread> thread_group;
  size_t num_threads = state.backend_count;

  abort_counts = new PadInt[num_threads];
  PL_MEMSET(abort_counts, 0, sizeof(PadInt) * num_threads);

  commit_counts = new PadInt[num_threads];
  PL_MEMSET(commit_counts, 0, sizeof(PadInt) * num_threads);

  size_t profile_round = (size_t)(state.duration / state.profile_duration);

  PadInt **abort_counts_profiles = new PadInt *[profile_round];
  for (size_t round_id = 0; round_id < profile_round; ++round_id) {
    abort_counts_profiles[round_id] = new PadInt[num_threads];
  }

  PadInt **commit_counts_profiles = new PadInt *[profile_round];
  for (size_t round_id = 0; round_id < profile_round; ++round_id) {
    commit_counts_profiles[round_id] = new PadInt[num_threads];
  }

  // Launch a group of threads
  for (size_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::move(std::thread(RunBackend, thread_itr)));
  }

  //////////////////////////////////////
  oid_t last_tile_group_id = 0;
  for (size_t round_id = 0; round_id < profile_round; ++round_id) {
    std::this_thread::sleep_for(
        std::chrono::milliseconds(int(state.profile_duration * 1000)));
    PL_MEMCPY(abort_counts_profiles[round_id], abort_counts,
           sizeof(PadInt) * num_threads);
    PL_MEMCPY(commit_counts_profiles[round_id], commit_counts,
           sizeof(PadInt) * num_threads);
    
    auto& manager = catalog::Manager::GetInstance();
    oid_t current_tile_group_id = manager.GetCurrentTileGroupId();
    if (round_id != 0) {
      state.profile_memory.push_back(current_tile_group_id - last_tile_group_id);
    }
    last_tile_group_id = current_tile_group_id;
  
  }
  
  state.profile_memory.push_back(state.profile_memory.at(state.profile_memory.size() - 1));

  is_running = false;

  // Join the threads with the main thread
  for (size_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }

  // calculate the throughput and abort rate for the first round.
  uint64_t total_commit_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_commit_count += commit_counts_profiles[0][i].data;
  }

  uint64_t total_abort_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_abort_count += abort_counts_profiles[0][i].data;
  }

  state.profile_throughput.push_back(total_commit_count * 1.0 /
                                      state.profile_duration);
  state.profile_abort_rate.push_back(total_abort_count * 1.0 /
                                      total_commit_count);

  // calculate the throughput and abort rate for the remaining rounds.
  for (size_t round_id = 0; round_id < profile_round - 1; ++round_id) {
    total_commit_count = 0;
    for (size_t i = 0; i < num_threads; ++i) {
      total_commit_count += commit_counts_profiles[round_id + 1][i].data -
                            commit_counts_profiles[round_id][i].data;
    }

    total_abort_count = 0;
    for (size_t i = 0; i < num_threads; ++i) {
      total_abort_count += abort_counts_profiles[round_id + 1][i].data -
                           abort_counts_profiles[round_id][i].data;
    }

    state.profile_throughput.push_back(total_commit_count * 1.0 /
                                        state.profile_duration);
    state.profile_abort_rate.push_back(total_abort_count * 1.0 /
                                        total_commit_count);
  }

  //////////////////////////////////////////////////
  // calculate the aggregated throughput and abort rate.
  total_commit_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_commit_count += commit_counts_profiles[profile_round - 1][i].data;
  }

  total_abort_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_abort_count += abort_counts_profiles[profile_round - 1][i].data;
  }

  state.throughput = total_commit_count * 1.0 / state.duration;
  state.abort_rate = total_abort_count * 1.0 / total_commit_count;

  //////////////////////////////////////////////////

  // cleanup everything.
  for (size_t round_id = 0; round_id < profile_round; ++round_id) {
    delete[] abort_counts_profiles[round_id];
    abort_counts_profiles[round_id] = nullptr;
  }

  for (size_t round_id = 0; round_id < profile_round; ++round_id) {
    delete[] commit_counts_profiles[round_id];
    commit_counts_profiles[round_id] = nullptr;
  }

  delete[] abort_counts_profiles;
  abort_counts_profiles = nullptr;
  delete[] commit_counts_profiles;
  commit_counts_profiles = nullptr;

  delete[] abort_counts;
  abort_counts = nullptr;
  delete[] commit_counts;
  commit_counts = nullptr;

}


/////////////////////////////////////////////////////////
// HARNESS
/////////////////////////////////////////////////////////

std::vector<std::vector<type::Value >> ExecuteRead(executor::AbstractExecutor* executor) {
  executor->Init();

  std::vector<std::vector<type::Value >> logical_tile_values;

  // Execute stuff
  while (executor->Execute() == true) {
    std::unique_ptr<executor::LogicalTile> result_tile(executor->GetOutput());

    if(result_tile == nullptr) {
      break;
    }

    auto column_count = result_tile->GetColumnCount();
    LOG_TRACE("result column count = %d\n", (int)column_count);

    for (oid_t tuple_id : *result_tile) {
      expression::ContainerTuple<executor::LogicalTile> cur_tuple(result_tile.get(),
                                                                  tuple_id);
      std::vector<type::Value > tuple_values;
      for (oid_t column_itr = 0; column_itr < column_count; column_itr++){
         auto value = cur_tuple.GetValue(column_itr);
         tuple_values.push_back(value);
      }

      // Move the tuple list
      logical_tile_values.push_back(std::move(tuple_values));
    }
  }

  return std::move(logical_tile_values);
}

void ExecuteUpdate(executor::AbstractExecutor* executor) {
  executor->Init();
  // Execute stuff
  while (executor->Execute() == true);
}



}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
