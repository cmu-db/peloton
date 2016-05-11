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

#undef NDEBUG

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
#include "backend/common/platform.h"

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

// Helper function to pin current thread to a specific core
static void PinToCore(size_t core) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core, &cpuset);
  pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
}

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

bool RunInsert(ZipfDistribution &zipf, oid_t next_insert_key);

/////////////////////////////////////////////////////////
// WORKLOAD
/////////////////////////////////////////////////////////

// Used to control backend execution
volatile bool run_backends = true;

// Committed transaction counts
std::vector<double> transaction_counts;

void RunBackend(oid_t thread_id) {
  PinToCore(thread_id);

  auto update_ratio = state.update_ratio;

  // Set zipfian skew
  auto zipf_theta = 0.0;
  if(state.skew_factor == SKEW_FACTOR_HIGH) {
    zipf_theta = 0.5;
  }

  fast_random rng(rand());
  ZipfDistribution zipf((state.scale_factor * DEFAULT_TUPLES_PER_TILEGROUP) - 1, zipf_theta);
  auto committed_transaction_count = 0;

  // Partition the domain across backends
  auto insert_key_offset = state.scale_factor * DEFAULT_TUPLES_PER_TILEGROUP;
  auto next_insert_key = insert_key_offset + thread_id + 1;

  // Run these many transactions
  while (true) {
    // Check if the backend should stop
    if (run_backends == false) {
      break;
    }

    auto rng_val = rng.next_uniform();
    auto transaction_status = false;

    // Run transaction
    if (rng_val < update_ratio) {
      next_insert_key += state.backend_count;
      transaction_status = RunInsert(zipf, next_insert_key);
    }
    else {
      transaction_status = RunRead(zipf);
    }

    // Update transaction count if it committed
    if(transaction_status == true){
      committed_transaction_count++;
    }
  }

  // Set committed_transaction_count
  transaction_counts[thread_id] = committed_transaction_count;
}

void RunWorkload() {
  // Execute the workload to build the log
  std::vector<std::thread> thread_group;
  oid_t num_threads = state.backend_count;
  transaction_counts.resize(num_threads);

  // Launch a group of threads
  for (oid_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::move(std::thread(RunBackend, thread_itr)));
  }

  // Sleep for duration specified by user and then stop the backends
  auto sleep_period = std::chrono::milliseconds(state.duration);
  std::this_thread::sleep_for(sleep_period);
  run_backends = false;

  // Join the threads with the main thread
  for (oid_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }

  // Compute total committed transactions
  auto sum_transaction_count = 0;
  for(auto transaction_count : transaction_counts){
    sum_transaction_count += transaction_count;
  }

  // Compute average throughput and latency
  state.throughput = (sum_transaction_count * 1000)/state.duration;
  state.latency = state.backend_count/state.throughput;
}

/////////////////////////////////////////////////////////
// HARNESS
/////////////////////////////////////////////////////////

static void ExecuteTest(std::vector<executor::AbstractExecutor *> &executors) {
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
      std::unique_ptr<executor::LogicalTile> result_tile(executor->GetOutput());
      result_tiles.emplace_back(result_tile.release());
    }
  }
}

static bool EndTransaction(concurrency::Transaction *txn) {
  auto result = txn->GetResult();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  // transaction passed execution.
  if (result == Result::RESULT_SUCCESS) {
    result = txn_manager.CommitTransaction();

    if (result == Result::RESULT_SUCCESS) {
      // transaction committed
      return true;
    } else {
      // transaction aborted or failed
      assert(result == Result::RESULT_ABORTED ||
             result == Result::RESULT_FAILURE);
      return false;
    }
  }
  // transaction aborted during execution.
  else {
    assert(result == Result::RESULT_ABORTED ||
           result == Result::RESULT_FAILURE);
    result = txn_manager.AbortTransaction();
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
  expr_types.push_back(
      ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  values.push_back(ValueFactory::GetIntegerValue(lookup_key));

  auto ycsb_pkey_index = user_table->GetIndexWithOid(
      user_table_pkey_index_oid);

  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      ycsb_pkey_index, key_column_ids, expr_types, values, runtime_keys);

  // Create plan node.
  auto predicate = nullptr;

  planner::IndexScanPlan index_scan_node(user_table,
                                         predicate, column_ids,
                                         index_scan_desc);

  // Run the executor
  executor::IndexScanExecutor index_scan_executor(&index_scan_node,
                                                  context.get());

  /////////////////////////////////////////////////////////
  // MATERIALIZE
  /////////////////////////////////////////////////////////

  // Create and set up materialization executor
  std::unordered_map<oid_t, oid_t> old_to_new_cols;
  for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
    old_to_new_cols[col_itr] = col_itr;
  }

  std::shared_ptr<const catalog::Schema> output_schema {
    catalog::Schema::CopySchema(user_table->GetSchema())
  };

  bool physify_flag = true;  // is going to create a physical tile
  planner::MaterializationPlan mat_node(old_to_new_cols,
                                        output_schema,
                                        physify_flag);

  executor::MaterializationExecutor mat_executor(&mat_node, nullptr);
  mat_executor.AddChild(&index_scan_executor);

  /////////////////////////////////////////////////////////
  // EXECUTE
  /////////////////////////////////////////////////////////

  std::vector<executor::AbstractExecutor *> executors;
  executors.push_back(&mat_executor);

  ExecuteTest(executors);

  auto txn_status = EndTransaction(txn);
  return txn_status;
}

bool RunInsert(__attribute__((unused)) ZipfDistribution &zipf,
               oid_t next_insert_key) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  const oid_t col_count = state.column_count + 1;
  auto table_schema = user_table->GetSchema();
  const bool allocate = true;
  std::string field_raw_value(ycsb_field_length - 1, 'o');

  std::unique_ptr<VarlenPool> pool(new VarlenPool(BACKEND_TYPE_MM));

  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  /////////////////////////////////////////////////////////
  // INSERT
  /////////////////////////////////////////////////////////

  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(table_schema, allocate));
  auto key_value = ValueFactory::GetIntegerValue(next_insert_key);
  auto field_value = ValueFactory::GetStringValue(field_raw_value);

  tuple->SetValue(0, key_value, nullptr);
  for (oid_t col_itr = 1; col_itr < col_count; col_itr++) {
    tuple->SetValue(col_itr, field_value, pool.get());
  }

  planner::InsertPlan insert_node(user_table, std::move(tuple));
  executor::InsertExecutor insert_executor(&insert_node, context.get());

  /////////////////////////////////////////////////////////
  // EXECUTE
  /////////////////////////////////////////////////////////

  std::vector<executor::AbstractExecutor *> executors;
  executors.push_back(&insert_executor);

  ExecuteTest(executors);

  auto txn_status = EndTransaction(txn);
  return txn_status;
}

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
