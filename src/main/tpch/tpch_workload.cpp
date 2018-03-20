//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpch_workload.cpp
//
// Identification: src/main/tpch/tpch_workload.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "benchmark/tpch/tpch_workload.h"

#include "codegen/query.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/plan_executor.h"
#include "planner/abstract_plan.h"
#include "planner/binding_context.h"
#include "codegen/counting_consumer.h"

namespace peloton {
namespace benchmark {
namespace tpch {

TPCHBenchmark::TPCHBenchmark(const Configuration &config, TPCHDatabase &db)
    : config_(config), db_(db) {
  query_configs_ = {
      {"Q1",
       QueryId::Q1,
       {TableId::Lineitem},
       [&]() { return ConstructQ1Plan(); }},

      {"Q2",
       QueryId::Q2,
       {TableId::Lineitem},
       [&]() { return ConstructQ1Plan(); }},

      {"Q3",
       QueryId::Q3,
       {TableId::Lineitem, TableId::Customer, TableId::Orders},
       [&]() { return ConstructQ3Plan(); }},

      {"Q4",
       QueryId::Q4,
       {TableId::Lineitem},
       [&]() { return ConstructQ1Plan(); }},

      {"Q5",
       QueryId::Q5,
       {TableId::Lineitem},
       [&]() { return ConstructQ1Plan(); }},

      {"Q6",
       QueryId::Q6,
       {TableId::Lineitem},
       [&]() { return ConstructQ6Plan(); }},

      {"Q7",
       QueryId::Q7,
       {TableId::Lineitem},
       [&]() { return ConstructQ1Plan(); }},

      {"Q8",
       QueryId::Q8,
       {TableId::Lineitem},
       [&]() { return ConstructQ1Plan(); }},

      {"Q9",
       QueryId::Q9,
       {TableId::Lineitem},
       [&]() { return ConstructQ1Plan(); }},

      {"Q10",
       QueryId::Q10,
       {TableId::Lineitem},
       [&]() { return ConstructQ1Plan(); }},

      {"Q11",
       QueryId::Q11,
       {TableId::Lineitem},
       [&]() { return ConstructQ1Plan(); }},

      {"Q12",
       QueryId::Q12,
       {TableId::Lineitem},
       [&]() { return ConstructQ1Plan(); }},

      {"Q13",
       QueryId::Q13,
       {TableId::Lineitem},
       [&]() { return ConstructQ1Plan(); }},

      {"Q14",
       QueryId::Q14,
       {TableId::Lineitem},
       [&]() { return ConstructQ1Plan(); }},

      {"Q15",
       QueryId::Q15,
       {TableId::Lineitem},
       [&]() { return ConstructQ1Plan(); }},

      {"Q16",
       QueryId::Q16,
       {TableId::Lineitem},
       [&]() { return ConstructQ1Plan(); }},

      {"Q17",
       QueryId::Q17,
       {TableId::Lineitem},
       [&]() { return ConstructQ1Plan(); }},

      {"Q18",
       QueryId::Q18,
       {TableId::Lineitem},
       [&]() { return ConstructQ1Plan(); }},

      {"Q19",
       QueryId::Q19,
       {TableId::Lineitem},
       [&]() { return ConstructQ1Plan(); }},

      {"Q20",
       QueryId::Q20,
       {TableId::Lineitem},
       [&]() { return ConstructQ1Plan(); }},

      {"Q21",
       QueryId::Q21,
       {TableId::Lineitem},
       [&]() { return ConstructQ1Plan(); }},

      {"Q22",
       QueryId::Q22,
       {TableId::Lineitem},
       [&]() { return ConstructQ1Plan(); }},
  };
}

void TPCHBenchmark::RunBenchmark() {
  for (uint32_t i = 0; i < 22; i++) {
    const auto &query_config = query_configs_[i];
    if (config_.ShouldRunQuery(query_config.query_id)) {
      RunQuery(query_config);
    }
  }
}

void TPCHBenchmark::RunQuery(const TPCHBenchmark::QueryConfig &query_config) {
  LOG_INFO("Running TPCH %s", query_config.query_name.c_str());

  // Load all the necessary tables
  for (auto tid : query_config.required_tables) {
    db_.LoadTable(tid);
  }

  // Construct the plan for Q1
  std::unique_ptr<planner::AbstractPlan> plan = query_config.PlanConstructor();

  // Do attribute binding
  planner::BindingContext binding_context;
  plan->PerformBinding(binding_context);

  // The consumer
  codegen::CountingConsumer counter;

  // Compile
  codegen::QueryCompiler::CompileStats compile_stats;
  codegen::QueryCompiler compiler;
  auto compiled_query = compiler.Compile(*plan, counter, &compile_stats);

  codegen::Query::RuntimeStats overall_stats;
  overall_stats.init_ms = 0.0;
  overall_stats.plan_ms = 0.0;
  overall_stats.tear_down_ms = 0.0;
  for (uint32_t i = 0; i < config_.num_runs; i++) {
    // Reset the counter for this run
    counter.ResetCount();

    // Begin a transaction
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto *txn = txn_manager.BeginTransaction();

    // Execute query in a transaction
    codegen::Query::RuntimeStats runtime_stats;
    compiled_query->Execute(*txn, counter.GetCountAsState(), &runtime_stats);

    // Commit transaction
    txn_manager.CommitTransaction(txn);

    // Collect stats
    overall_stats.init_ms += runtime_stats.init_ms;
    overall_stats.plan_ms += runtime_stats.plan_ms;
    overall_stats.tear_down_ms += runtime_stats.tear_down_ms;
  }

  LOG_INFO("%s: %s",
           query_config.query_name.c_str(), peloton::GETINFO_THICK_LINE.c_str());
  LOG_INFO("# Runs: %u, # Result tuples: %lu", config_.num_runs,
           counter.GetCount());
  LOG_INFO("Setup: %.2lf, IR Gen: %.2lf, Compile: %.2lf",
           compile_stats.setup_ms, compile_stats.ir_gen_ms,
           compile_stats.jit_ms);
  LOG_INFO("Init: %.2lf ms, Plan: %.2lf ms, TearDown: %.2lf ms",
           overall_stats.init_ms / config_.num_runs,
           overall_stats.plan_ms / config_.num_runs,
           overall_stats.tear_down_ms / config_.num_runs);
}


}  // namespace tpch
}  // namespace benchmark
}  // namespace peloton