//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// scan_benchmark_test.cpp
//
// Identification: test/codegen/scan_benchmark_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "codegen/buffering_consumer.h"
#include "codegen/query_compiler.h"
#include "common/timer.h"
#include "common/container_tuple.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/seq_scan_executor.h"
#include "executor/executor_context.h"
#include "expression/comparison_expression.h"
#include "expression/conjunction_expression.h"
#include "logging/log_manager.h"
#include "planner/seq_scan_plan.h"
#include "storage/table_factory.h"
#include "type/types.h"

#include "codegen/testing_codegen_util.h"

namespace peloton {
namespace test {

enum ScanComplexity { SIMPLE, MODERATE, COMPLEX };

struct TestConfig {
  uint32_t scale_factor = 10;
  ScanComplexity scan_complexity = MODERATE;
  double selectivity;
};

class BenchmarkScanTest : public PelotonCodeGenTest {
 public:
  BenchmarkScanTest() : PelotonCodeGenTest() {
    // Load test table
    LoadTestTable(TestTableId(), num_rows_to_insert);
  }

  // uint32_t NumRowsInTestTable() const { return num_rows_to_insert; }

  uint32_t TestTableId() { return test_table_oids[0]; }

  expression::AbstractExpression *ConstructSimplePredicate(TestConfig &config) {
    // Setup the predicate of the form a >= ? such that the selectivity
    // is equal to that in the configuration
    double param = (1 - config.selectivity) *
        num_rows_to_insert * config.scale_factor;
    auto *a_col_exp =
        new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
    auto *const_exp = new expression::ConstantValueExpression(
        type::ValueFactory::GetIntegerValue(param));
    return new expression::ComparisonExpression(
        ExpressionType::COMPARE_GREATERTHANOREQUALTO, a_col_exp, const_exp);
  }

  expression::AbstractExpression *ConstructModeratePredicate(
      TestConfig &config) {
    // Setup the predicate a >= ? and b >= a such that the predicate has
    // the selectivity as configured for the experiment

    double param = (1 - config.selectivity) *
        num_rows_to_insert * config.scale_factor;
    auto *a_col_exp =
        new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
    auto *const_exp = new expression::ConstantValueExpression(
        type::ValueFactory::GetIntegerValue(param));
    auto *a_gte_param = new expression::ComparisonExpression(
        ExpressionType::COMPARE_GREATERTHANOREQUALTO, a_col_exp, const_exp);

    auto *b_col_exp =
        new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 1);
    auto *a_col_exp2 =
        new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
    auto *b_gte_a = new expression::ComparisonExpression(
        ExpressionType::COMPARE_GREATERTHANOREQUALTO, b_col_exp, a_col_exp2);

    return new expression::ConjunctionExpression(
        ExpressionType::CONJUNCTION_AND, a_gte_param, b_gte_a);
  }

  expression::AbstractExpression *ConstructComplexPredicate(
      __attribute((unused)) TestConfig &config) {
    // Setup the predicate a >= ? and b >= a and c >= b
    double param = (1 - config.selectivity) *
        num_rows_to_insert * config.scale_factor;
    auto *a_col_exp =
        new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
    auto *const_exp = new expression::ConstantValueExpression(
        type::ValueFactory::GetIntegerValue(param));
    auto *a_gte_param = new expression::ComparisonExpression(
        ExpressionType::COMPARE_GREATERTHANOREQUALTO, a_col_exp, const_exp);

    auto *b_col_exp =
        new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 1);
    auto *a_col_exp2 =
        new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
    auto *b_gte_a = new expression::ComparisonExpression(
        ExpressionType::COMPARE_GREATERTHANOREQUALTO, b_col_exp, a_col_exp2);

    auto *c_col_exp =
        new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 2);
    auto *b_col_exp2 =
        new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 1);
    auto *c_gte_b = new expression::ComparisonExpression(
        ExpressionType::COMPARE_LESSTHANOREQUALTO, c_col_exp, b_col_exp2);

    auto *b_gte_a_gte_param =
        new expression::ConjunctionExpression(
            ExpressionType::CONJUNCTION_AND, a_gte_param, b_gte_a);
    return new expression::ConjunctionExpression(
        ExpressionType::CONJUNCTION_AND, b_gte_a_gte_param, c_gte_b);
  }

  std::unique_ptr<planner::SeqScanPlan> ConstructScanPlan(TestConfig &config) {
    expression::AbstractExpression *predicate = nullptr;
    switch (config.scan_complexity) {
      case ScanComplexity::SIMPLE:
        predicate = ConstructSimplePredicate(config);
        break;
      case ScanComplexity::MODERATE:
        predicate = ConstructModeratePredicate(config);
        break;
      case ScanComplexity::COMPLEX:
        predicate = ConstructComplexPredicate(config);
        break;
      default: { throw Exception{"nope"}; }
    }

    // Setup the scan plan node
    return std::unique_ptr<planner::SeqScanPlan>{
        new planner::SeqScanPlan(&GetTestTable(TestTableId()), predicate, {0, 1, 2})};
  }

  Stats RunCompiledExperiment(TestConfig &config, uint32_t num_runs = 5) {
    // Keep one copy of compile and runtime stats
    Stats stats;

    for (uint32_t i = 0; i < num_runs; i++) {
      auto scan = ConstructScanPlan(config);

      // Do binding
      planner::BindingContext context;
      scan->PerformBinding(context);

      // We collect the results of the query into an in-memory buffer
      codegen::BufferingConsumer buffer{{0}, context};

      // COMPILE and execute
      codegen::Query::RuntimeStats runtime_stats;
      codegen::QueryCompiler::CompileStats compile_stats = CompileAndExecute(
          *scan, buffer, reinterpret_cast<char*>(buffer.GetState()));

      stats.Merge(compile_stats, runtime_stats,
          buffer.GetOutputTuples().size());
    }

    stats.Finalize();
    return stats;
  }

  Stats RunInterpretedExperiment(TestConfig &config, uint32_t num_runs = 5) {
    // Keep one copy of compile and runtime stats
    Stats stats;

    for (uint32_t i = 0; i < num_runs; i++) {
      auto scan = ConstructScanPlan(config);
      std::vector<std::vector<type::Value>> vals;

      codegen::QueryCompiler::CompileStats compile_stats;
      codegen::Query::RuntimeStats runtime_stats;

      auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
      auto txn = txn_manager.BeginTransaction();

      executor::ExecutorContext ctx{txn};
      executor::SeqScanExecutor executor{scan.get(), &ctx};

      Timer<std::ratio<1, 1000>> timer;
      timer.Start();
      executor.Init();
      timer.Stop();
      runtime_stats.init_ms = timer.GetDuration();
      timer.Reset();

      timer.Start();
      while (executor.Execute()) {
        auto tile = executor.GetOutput();
        for (oid_t tuple_id : *tile) {
          const ContainerTuple<executor::LogicalTile> tuple{
              tile, tuple_id};
          std::vector<type::Value> tv;
          for (oid_t col_id : scan->GetColumnIds()) {
            tv.push_back(tuple.GetValue(col_id));
          }
          vals.push_back(std::move(tv));
        }
      }
      timer.Stop();
      runtime_stats.plan_ms = timer.GetDuration();

      stats.Merge(compile_stats, runtime_stats, vals.size());

      txn_manager.CommitTransaction(txn);
    }

    stats.Finalize();
    return stats;
  }

 private:
  uint32_t num_rows_to_insert = 10000;
};

void PrintName(std::string test_name) {
  std::cerr << "NAME:\n===============\n" << test_name << std::endl;
}

void PrintConfig(TestConfig &config) {
    fprintf(stderr, "CONFIGURATION:\n===============\n");
    fprintf(stderr,
            "Scan complexity: %d, Selectivity: %.2f\n",
            config.scan_complexity, config.selectivity);
  }

// TEST_F(BenchmarkScanTest, ScanTestWithCompilation) {
//   PrintName("SCAN: COMPILATION");
//   auto stats = RunCompiledExperiment();
//   stats.PrintStats();
// }

// TEST_F(BenchmarkScanTest, ScanTestWithInterpretation) {
//   PrintName("SCAN: INTERPRETATION");
//   auto stats = RunInterpretedExperiment();
//   stats.PrintStats();
// }

TEST_F(BenchmarkScanTest, SelectivityTestWithCompilation) {
  double selectivities[] = {0.0, 0.25, 0.5, 0.75, 1.0};

  PrintName("SCAN_SELECTIVITY: COMPILATION");
  for (double selectivity : selectivities) {
    TestConfig config;
    config.selectivity = selectivity;

    auto stats = RunCompiledExperiment(config);
    PrintConfig(config);
    stats.PrintStats();
  }
}

TEST_F(BenchmarkScanTest, SelectivityTestWithInterpretation) {
  double selectivities[] = {0.0, 0.25, 0.5, 0.75, 1.0};

  PrintName("SCAN_SELECTIVITY: INTERPRETATION");
  for (double selectivity : selectivities) {
    TestConfig config;
    config.selectivity = selectivity;

    auto stats = RunInterpretedExperiment(config);
    PrintConfig(config);
    stats.PrintStats();
  }
}

TEST_F(BenchmarkScanTest, PredicateComplexityTestWithCompilation) {
 ScanComplexity complexities[] = { SIMPLE, MODERATE };

 PrintName("SCAN_COMPLEXITY: COMPILATION");
 for (ScanComplexity complexity : complexities) {
   TestConfig config;
   config.selectivity = 0.5;
   config.scan_complexity = complexity;

   auto stats = RunCompiledExperiment(config);
   PrintConfig(config);
   stats.PrintStats();
 }
}

TEST_F(BenchmarkScanTest, PredicateComplexityTestWithInterpretation) {
 ScanComplexity complexities[] = { SIMPLE, MODERATE };

 PrintName("SCAN_COMPLEXITY: INTERPRETATION");
 for (ScanComplexity complexity : complexities) {
   TestConfig config;
   config.scan_complexity = complexity;

   auto stats = RunInterpretedExperiment(config);
   PrintConfig(config);
   stats.PrintStats();
 }
}

}  // namespace test
}  // namespace peloton