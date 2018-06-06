//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpch_workload.h
//
// Identification: src/include/benchmark/tpch/tpch_workload.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "benchmark/tpch/tpch_configuration.h"
#include "benchmark/tpch/tpch_database.h"

#include "codegen/compilation_context.h"
#include "codegen/execution_consumer.h"

namespace peloton {
namespace benchmark {
namespace tpch {

// The benchmark
class TPCHBenchmark {
 public:
  TPCHBenchmark(const Configuration &config, TPCHDatabase &db);

  // Run the benchmark
  void RunBenchmark();

 private:

  struct QueryConfig {
   public:
    // The name of the query
    std::string query_name;

    // The ID of the query
    QueryId query_id;

    // The list of tables this query uses
    std::vector<TableId> required_tables;

    // A function that constructs a plan for this query
    std::function<std::unique_ptr<planner::AbstractPlan>()> PlanConstructor;
  };

  // Run the given query
  void RunQuery(const QueryConfig &query_config);

  // Plan constructors
  std::unique_ptr<planner::AbstractPlan> ConstructQ1Plan() const;
  std::unique_ptr<planner::AbstractPlan> ConstructQ2Plan() const;
  std::unique_ptr<planner::AbstractPlan> ConstructQ3Plan() const;
  std::unique_ptr<planner::AbstractPlan> ConstructQ4Plan() const;
  std::unique_ptr<planner::AbstractPlan> ConstructQ5Plan() const;
  std::unique_ptr<planner::AbstractPlan> ConstructQ6Plan() const;
  std::unique_ptr<planner::AbstractPlan> ConstructQ7Plan() const;
  std::unique_ptr<planner::AbstractPlan> ConstructQ8Plan() const;
  std::unique_ptr<planner::AbstractPlan> ConstructQ9Plan() const;
  std::unique_ptr<planner::AbstractPlan> ConstructQ10Plan() const;
  std::unique_ptr<planner::AbstractPlan> ConstructQ11Plan() const;
  std::unique_ptr<planner::AbstractPlan> ConstructQ12Plan() const;
  std::unique_ptr<planner::AbstractPlan> ConstructQ13Plan() const;
  std::unique_ptr<planner::AbstractPlan> ConstructQ14Plan() const;
  std::unique_ptr<planner::AbstractPlan> ConstructQ15Plan() const;
  std::unique_ptr<planner::AbstractPlan> ConstructQ16Plan() const;
  std::unique_ptr<planner::AbstractPlan> ConstructQ17Plan() const;
  std::unique_ptr<planner::AbstractPlan> ConstructQ18Plan() const;
  std::unique_ptr<planner::AbstractPlan> ConstructQ19Plan() const;
  std::unique_ptr<planner::AbstractPlan> ConstructQ20Plan() const;
  std::unique_ptr<planner::AbstractPlan> ConstructQ21Plan() const;
  std::unique_ptr<planner::AbstractPlan> ConstructQ22Plan() const;

 private:
  // The benchmark configuration
  const Configuration &config_;

  // The TPCH database
  TPCHDatabase &db_;

  // All query configurations
  std::vector<QueryConfig> query_configs_;
};

}  // namespace tpch
}  // namespace benchmark
}  // namespace peloton