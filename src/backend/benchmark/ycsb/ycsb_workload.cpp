//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// ycsb_workload.cpp
//
// Identification: benchmark/ycsb/ycsb_workload.cpp
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

#include "backend/benchmark/ycsb/ycsb_workload.h"
#include "backend/benchmark/ycsb/ycsb_configuration.h"
#include "backend/benchmark/ycsb/ycsb_loader.h"

#include "backend/catalog/manager.h"
#include "backend/catalog/schema.h"

#include "backend/common/types.h"
#include "backend/common/value.h"
#include "backend/common/value_factory.h"
#include "backend/common/logger.h"
#include "backend/common/timer.h"
#include "backend/common/generator.h"

#include "backend/concurrency/transaction.h"
#include "backend/concurrency/transaction_manager_factory.h"

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
namespace ycsb {

/////////////////////////////
///// Random Generator //////
/////////////////////////////

// Fast random number generator
class fast_random {
 public:
  fast_random(unsigned long seed) : seed(0) { set_seed0(seed); }

  inline unsigned long next() {
    return ((unsigned long)next(32) << 32) + next(32);
  }

  inline uint32_t next_u32() { return next(32); }

  inline uint16_t next_u16() { return (uint16_t)next(16); }

  /** [0.0, 1.0) */
  inline double next_uniform() {
    return (((unsigned long)next(26) << 27) + next(27)) / (double)(1L << 53);
  }

  inline char next_char() { return next(8) % 256; }

  inline char next_readable_char() {
    static const char readables[] =
        "0123456789@ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz";
    return readables[next(6)];
  }

  inline std::string next_string(size_t len) {
    std::string s(len, 0);
    for (size_t i = 0; i < len; i++) s[i] = next_char();
    return s;
  }

  inline std::string next_readable_string(size_t len) {
    std::string s(len, 0);
    for (size_t i = 0; i < len; i++) s[i] = next_readable_char();
    return s;
  }

  inline unsigned long get_seed() { return seed; }

  inline void set_seed(unsigned long seed) { this->seed = seed; }

 private:
  inline void set_seed0(unsigned long seed) {
    this->seed = (seed ^ 0x5DEECE66DL) & ((1L << 48) - 1);
  }

  inline unsigned long next(unsigned int bits) {
    seed = (seed * 0x5DEECE66DL + 0xBL) & ((1L << 48) - 1);
    return (unsigned long)(seed >> (48 - bits));
  }

  unsigned long seed;
};

class ZipfDistribution {
 public:
  ZipfDistribution(const uint64_t &n, const double &theta)
      : rand_generator(rand()) {
    // range: 1-n
    the_n = n;
    zipf_theta = theta;
    zeta_2_theta = zeta(2, zipf_theta);
    denom = zeta(the_n, zipf_theta);
  }
  double zeta(uint64_t n, double theta) {
    double sum = 0;
    for (uint64_t i = 1; i <= n; i++) sum += pow(1.0 / i, theta);
    return sum;
  }
  int GenerateInteger(const int &min, const int &max) {
    return rand_generator.next() % (max - min + 1) + min;
  }
  uint64_t GetNextNumber() {
    double alpha = 1 / (1 - zipf_theta);
    double zetan = denom;
    double eta =
        (1 - pow(2.0 / the_n, 1 - zipf_theta)) / (1 - zeta_2_theta / zetan);
    double u = (double)(GenerateInteger(1, 10000000) % 10000000) / 10000000;
    double uz = u * zetan;
    if (uz < 1) return 1;
    if (uz < 1 + pow(0.5, zipf_theta)) return 2;
    return 1 + (uint64_t)(the_n * pow(eta * u - eta + 1, alpha));
  }

  uint64_t the_n;
  double zipf_theta;
  double denom;
  double zeta_2_theta;
  fast_random rand_generator;
};

/////////////////////////////////////////////////////////
// TRANSACTION TYPES
/////////////////////////////////////////////////////////

bool RunRead(ZipfDistribution &zipf);

bool RunUpdate(ZipfDistribution &zipf);

bool RunMixed(ZipfDistribution &zipf, int read_count, int write_count);

/////////////////////////////////////////////////////////
// WORKLOAD
/////////////////////////////////////////////////////////

volatile bool is_running = true;

oid_t *abort_counts;
oid_t *commit_counts;

// Helper function to pin current thread to a specific core
static void PinToCore(size_t core) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core, &cpuset);
  pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
}

void RunBackend(oid_t thread_id) {
  PinToCore(thread_id);

  auto update_ratio = state.update_ratio;

  oid_t &execution_count_ref = abort_counts[thread_id];
  oid_t &transaction_count_ref = commit_counts[thread_id];

  fast_random rng(rand());
  ZipfDistribution zipf(state.scale_factor * DEFAULT_TUPLES_PER_TILEGROUP,
                        state.zipf_theta);

  // Run these many transactions
  while (true) {
    if (is_running == false) {
      break;
    }

    if (state.run_mix) {
      while (RunMixed(zipf, 12, 2) == false) {
        execution_count_ref++;
      }
    } else {
      auto rng_val = rng.next_uniform();

      if (rng_val < update_ratio) {
        while (RunUpdate(zipf) == false) {
          execution_count_ref++;
        }
      } else {
        while (RunRead(zipf) == false) {
          execution_count_ref++;
        }
      }
    }

    transaction_count_ref++;
  }
}

void RunWorkload() {
  // Execute the workload to build the log
  std::vector<std::thread> thread_group;
  oid_t num_threads = state.backend_count;

  abort_counts = new oid_t[num_threads];
  memset(abort_counts, 0, sizeof(oid_t) * num_threads);

  commit_counts = new oid_t[num_threads];
  memset(commit_counts, 0, sizeof(oid_t) * num_threads);

  size_t snapshot_round = (size_t)(state.duration / state.snapshot_duration);

  oid_t **abort_counts_snapshots = new oid_t *[snapshot_round];
  for (size_t round_id = 0; round_id < snapshot_round; ++round_id) {
    abort_counts_snapshots[round_id] = new oid_t[num_threads];
  }

  oid_t **commit_counts_snapshots = new oid_t *[snapshot_round];
  for (size_t round_id = 0; round_id < snapshot_round; ++round_id) {
    commit_counts_snapshots[round_id] = new oid_t[num_threads];
  }

  // Launch a group of threads
  for (oid_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::move(std::thread(RunBackend, thread_itr)));
  }

  for (size_t round_id = 0; round_id < snapshot_round; ++round_id) {
    std::this_thread::sleep_for(
        std::chrono::milliseconds(int(state.snapshot_duration * 1000)));
    memcpy(abort_counts_snapshots[round_id], abort_counts,
           sizeof(oid_t) * num_threads);
    memcpy(commit_counts_snapshots[round_id], commit_counts,
           sizeof(oid_t) * num_threads);
  }

  is_running = false;

  // Join the threads with the main thread
  for (oid_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }

  // calculate the throughput and abort rate for the first round.
  oid_t total_commit_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_commit_count += commit_counts_snapshots[0][i];
  }

  oid_t total_abort_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_abort_count += abort_counts_snapshots[0][i];
  }

  state.snapshot_throughput.push_back(total_commit_count * 1.0 /
                                      state.snapshot_duration);
  state.snapshot_abort_rate.push_back(total_abort_count * 1.0 /
                                      total_commit_count);

  // calculate the throughput and abort rate for the remaining rounds.
  for (size_t round_id = 0; round_id < snapshot_round - 1; ++round_id) {
    total_commit_count = 0;
    for (size_t i = 0; i < num_threads; ++i) {
      total_commit_count += commit_counts_snapshots[round_id + 1][i] -
                            commit_counts_snapshots[round_id][i];
    }

    total_abort_count = 0;
    for (size_t i = 0; i < num_threads; ++i) {
      total_abort_count += abort_counts_snapshots[round_id + 1][i] -
                           abort_counts_snapshots[round_id][i];
    }

    state.snapshot_throughput.push_back(total_commit_count * 1.0 /
                                        state.snapshot_duration);
    state.snapshot_abort_rate.push_back(total_abort_count * 1.0 /
                                        total_commit_count);
  }

  // calculate the aggregated throughput and abort rate.
  total_commit_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_commit_count += commit_counts_snapshots[snapshot_round - 1][i];
  }

  total_abort_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_abort_count += abort_counts_snapshots[snapshot_round - 1][i];
  }

  state.throughput = total_commit_count * 1.0 / state.duration;
  state.abort_rate = total_abort_count * 1.0 / total_commit_count;

  // cleanup everything.
  for (size_t round_id = 0; round_id < snapshot_round; ++round_id) {
    delete[] abort_counts_snapshots[round_id];
    abort_counts_snapshots[round_id] = nullptr;
  }

  for (size_t round_id = 0; round_id < snapshot_round; ++round_id) {
    delete[] commit_counts_snapshots[round_id];
    commit_counts_snapshots[round_id] = nullptr;
  }
  delete[] abort_counts_snapshots;
  abort_counts_snapshots = nullptr;
  delete[] commit_counts_snapshots;
  commit_counts_snapshots = nullptr;

  delete[] abort_counts;
  abort_counts = nullptr;
  delete[] commit_counts;
  commit_counts = nullptr;
}

/////////////////////////////////////////////////////////
// HARNESS
/////////////////////////////////////////////////////////

/**
 * Run executors. These executors should be running under @transaction, whenever
 * the @transaction has a non-successful result this function will abort the
 * transaction and return false. If all executors successfully executed, meaning
 * that @transaction has a successful result in the end, the transaction will
 * be committed. Only when this commit succeeds, this function will return true.
 * Notice that the transaction needs to be began before the executor is initialized
 * because it is passed in as part of the executor context.
 */
static bool ExecuteTest(concurrency::Transaction *transaction, const std::vector<executor::AbstractExecutor *> &executors) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  for (auto executor : executors) {
    bool status = executor->Init();
    if (status == false) {
      throw Exception("Init failed");
    }

    std::vector<std::unique_ptr<executor::LogicalTile>> result_tiles;
    // Run the executor
    while (executor->Execute() == true) {
      // I don't know why we have to get the output from the executor
      std::unique_ptr<executor::LogicalTile> result_tile(executor->GetOutput());
      result_tiles.emplace_back(result_tile.release());
    }

    if (transaction->GetResult() != Result::RESULT_SUCCESS) {
      txn_manager.AbortTransaction();
      return false;
    }
  }

  assert(transaction->GetResult() == Result::RESULT_SUCCESS);

  // Finally we commit it
  auto result = txn_manager.CommitTransaction();

  if (result == Result::RESULT_SUCCESS) {
    return true;
  } else {
    return false;
  }
}

/////////////////////////////////////////////////////////
// TRANSACTIONS
/////////////////////////////////////////////////////////

bool RunRead(ZipfDistribution &zipf) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto txn = txn_manager.BeginTransaction();

  /////////////////////////////////////////////////////////
  // INDEX SCAN + PREDICATE
  /////////////////////////////////////////////////////////

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));
  std::vector<executor::AbstractExecutor *> executors;
  std::vector<planner::AbstractPlan *> plans;

  std::vector<oid_t> key_column_ids;
  std::vector<ExpressionType> expr_types;
  key_column_ids.push_back(0);
  expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);

  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids;
  oid_t column_count = state.column_count + 1;

  for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
    column_ids.push_back(col_itr);
  }

  std::vector<expression::AbstractExpression *> runtime_keys;

  auto ycsb_pkey_index = user_table->GetIndexWithOid(user_table_pkey_index_oid);

  // Create and set up index scan executor
  std::vector<Value> values;

  auto lookup_key = zipf.GetNextNumber();

  values.push_back(ValueFactory::GetIntegerValue(lookup_key));

  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      ycsb_pkey_index, key_column_ids, expr_types, values, runtime_keys);

  // Create plan node.
  auto predicate = nullptr;

  planner::IndexScanPlan *index_scan_node = new planner::IndexScanPlan(
      user_table, predicate, column_ids, index_scan_desc);
  // Run the executor
  executor::IndexScanExecutor *index_scan_executor =
      new executor::IndexScanExecutor(index_scan_node, context.get());

  executors.push_back(index_scan_executor);
  plans.push_back(index_scan_node);

  /////////////////////////////////////////////////////////
  // MATERIALIZE
  /////////////////////////////////////////////////////////

  // Create and set up materialization executor
  // std::unordered_map<oid_t, oid_t> old_to_new_cols;
  // for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
  //   old_to_new_cols[col_itr] = col_itr;
  // }

  // std::shared_ptr<const catalog::Schema> output_schema {
  //   catalog::Schema::CopySchema(user_table->GetSchema())
  // }
  // ;
  // bool physify_flag = true;  // is going to create a physical tile
  // planner::MaterializationPlan mat_node(old_to_new_cols, output_schema,
  //                                       physify_flag);

  // executor::MaterializationExecutor mat_executor(&mat_node, nullptr);
  // mat_executor.AddChild(&index_scan_executor);

  /////////////////////////////////////////////////////////
  // EXECUTE
  /////////////////////////////////////////////////////////

  bool result = ExecuteTest(txn, executors);

  for (auto executor : executors) {
    delete executor;
  }

  for (auto plan : plans) {
    delete plan;
  }

  return result;
}

bool RunMixed(ZipfDistribution &zipf, int read_count, int write_count) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto txn = txn_manager.BeginTransaction();

  /////////////////////////////////////////////////////////
  // INDEX SCAN + PREDICATE
  /////////////////////////////////////////////////////////

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));
  std::vector<executor::AbstractExecutor *> executors;
  std::vector<planner::AbstractPlan *> plans;

  std::vector<oid_t> key_column_ids;
  std::vector<ExpressionType> expr_types;
  key_column_ids.push_back(0);
  expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);

  std::vector<oid_t> column_ids;
  oid_t column_count = state.column_count + 1;

  // Column ids to be added to logical tile after scan.
  for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
    column_ids.push_back(col_itr);
  }

  std::vector<expression::AbstractExpression *> runtime_keys;

  auto ycsb_pkey_index = user_table->GetIndexWithOid(user_table_pkey_index_oid);

  for (int i = 0; i < read_count; i++) {
    // Create and set up index scan executor

    std::vector<Value> values;

    auto lookup_key = zipf.GetNextNumber();

    values.push_back(ValueFactory::GetIntegerValue(lookup_key));

    planner::IndexScanPlan::IndexScanDesc index_scan_desc(
        ycsb_pkey_index, key_column_ids, expr_types, values, runtime_keys);

    // Create plan node.
    auto predicate = nullptr;

     planner::IndexScanPlan *index_scan_node = new planner::IndexScanPlan(
        user_table, predicate, column_ids, index_scan_desc);
    // Run the executor
    executor::IndexScanExecutor *index_scan_executor =
        new executor::IndexScanExecutor(index_scan_node, context.get());

    executors.push_back(index_scan_executor);
    plans.push_back(index_scan_node);
  }

  /////////////////////////////////////////////////////////
  // INDEX SCAN + PREDICATE
  /////////////////////////////////////////////////////////

   for (int i = 0; i < write_count; i++) {
    // Create and set up index scan executor

    std::vector<Value> values;

    auto lookup_key = zipf.GetNextNumber();

    values.push_back(ValueFactory::GetIntegerValue(lookup_key));

    planner::IndexScanPlan::IndexScanDesc index_scan_desc(
        ycsb_pkey_index, key_column_ids, expr_types, values, runtime_keys);

    // Create plan node.
    auto predicate = nullptr;

    planner::IndexScanPlan *index_scan_node = new planner::IndexScanPlan(
        user_table, predicate, column_ids, index_scan_desc);
    plans.push_back(index_scan_node);

    // Run the executor
    executor::IndexScanExecutor *index_scan_executor =
        new executor::IndexScanExecutor(index_scan_node, context.get());

    /////////////////////////////////////////////////////////
    // UPDATE
    /////////////////////////////////////////////////////////

    planner::ProjectInfo::TargetList target_list;
    planner::ProjectInfo::DirectMapList direct_map_list;

    // Update the second attribute
    for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
      if (col_itr != 1) {
        direct_map_list.emplace_back(col_itr,
                                     std::pair<oid_t, oid_t>(0, col_itr));
      }
    }

    // std::string update_raw_value(ycsb_field_length - 1, 'u');
    int update_raw_value = 2;
    Value update_val = ValueFactory::GetIntegerValue(update_raw_value);
    target_list.emplace_back(
        1, expression::ExpressionUtil::ConstantValueFactory(update_val));

    std::unique_ptr<const planner::ProjectInfo> project_info(
        new planner::ProjectInfo(std::move(target_list),
                                 std::move(direct_map_list)));
    planner::UpdatePlan *update_node =
        new planner::UpdatePlan(user_table, std::move(project_info));
    plans.push_back(update_node);

    executor::UpdateExecutor *update_executor =
        new executor::UpdateExecutor(update_node, context.get());
    update_executor->AddChild(index_scan_executor);
    executors.push_back(update_executor);
  }

  bool result = ExecuteTest(txn, executors);

  for (auto executor : executors) {
    delete executor;
  }

  for (auto plan : plans) {
    delete plan;
  }

  return result;
}

bool RunUpdate(ZipfDistribution &zipf) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto txn = txn_manager.BeginTransaction();

  /////////////////////////////////////////////////////////
  // INDEX SCAN + PREDICATE
  /////////////////////////////////////////////////////////

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids;
  oid_t column_count = state.column_count + 1;

  for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
    column_ids.push_back(col_itr);
  }

  // Create and set up index scan executor

  std::vector<oid_t> key_column_ids;
  std::vector<ExpressionType> expr_types;
  std::vector<Value> values;
  std::vector<expression::AbstractExpression *> runtime_keys;

  auto lookup_key = zipf.GetNextNumber();

  key_column_ids.push_back(0);
  expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  values.push_back(ValueFactory::GetIntegerValue(lookup_key));

  auto ycsb_pkey_index = user_table->GetIndexWithOid(user_table_pkey_index_oid);

  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      ycsb_pkey_index, key_column_ids, expr_types, values, runtime_keys);

  // Create plan node.
  auto predicate = nullptr;

  planner::IndexScanPlan index_scan_node(user_table, predicate, column_ids,
                                         index_scan_desc);

  // Run the executor
  executor::IndexScanExecutor index_scan_executor(&index_scan_node,
                                                  context.get());

  /////////////////////////////////////////////////////////
  // UPDATE
  /////////////////////////////////////////////////////////

  planner::ProjectInfo::TargetList target_list;
  planner::ProjectInfo::DirectMapList direct_map_list;

  // Update the second attribute
  for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
    if (col_itr != 1) {
      direct_map_list.emplace_back(col_itr,
                                   std::pair<oid_t, oid_t>(0, col_itr));
    }
  }

  // std::string update_raw_value(ycsb_field_length - 1, 'u');
  int update_raw_value = 2;
  Value update_val = ValueFactory::GetIntegerValue(update_raw_value);
  target_list.emplace_back(
      1, expression::ExpressionUtil::ConstantValueFactory(update_val));

  std::unique_ptr<const planner::ProjectInfo> project_info(
      new planner::ProjectInfo(std::move(target_list),
                               std::move(direct_map_list)));
  planner::UpdatePlan update_node(user_table, std::move(project_info));

  executor::UpdateExecutor update_executor(&update_node, context.get());
  update_executor.AddChild(&index_scan_executor);

  std::vector<executor::AbstractExecutor *> executors;
  executors.push_back(&update_executor);

  /////////////////////////////////////////////////////////
  // EXECUTE
  /////////////////////////////////////////////////////////

  bool result = ExecuteTest(txn, executors);

  return result;
}

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
