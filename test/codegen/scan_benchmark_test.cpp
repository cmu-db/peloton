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

#include <cstdlib>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "codegen/buffering_consumer.h"
#include "codegen/query_compiler.h"
#include "common/timer.h"
#include "common/container_tuple.h"
#include "threadpool/mono_queue_pool.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/seq_scan_executor.h"
#include "executor/executor_context.h"
#include "expression/comparison_expression.h"
#include "expression/conjunction_expression.h"
#include "logging/log_manager.h"
#include "planner/seq_scan_plan.h"
#include "storage/table_factory.h"

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
  BenchmarkScanTest() : PelotonCodeGenTest(1250000) {
    // Load test table
    LoadTestTable(TestTableId(), num_rows_to_insert - 1);
    std::cout << "Test table has "
              << GetTestTable(TestTableId()).GetTileGroupCount()
              << " tile groups" << std::endl;
  }
  uint32_t TestTableId() { return test_table_oids[0]; }

  expression::AbstractExpression *ConstructSimplePredicate(
      const TestConfig &config) {
    // Setup the predicate of the form a >= ? such that the selectivity
    // is equal to that in the configuration
    double param = (1 - config.selectivity) *
                   num_rows_to_insert * config.scale_factor;
    auto *a_col_exp =
        new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
    auto *const_exp = new expression::ConstantValueExpression(
        type::ValueFactory::GetIntegerValue(static_cast<int32_t>(param)));
    return new expression::ComparisonExpression(
        ExpressionType::COMPARE_GREATERTHANOREQUALTO, a_col_exp, const_exp);
  }

  expression::AbstractExpression *ConstructModeratePredicate(
      const TestConfig &config) {
    // Setup the predicate a >= ? and b >= a such that the predicate has
    // the selectivity as configured for the experiment

    double param = (1 - config.selectivity) *
                   num_rows_to_insert * config.scale_factor;
    auto *a_col_exp =
        new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
    auto *const_exp = new expression::ConstantValueExpression(
        type::ValueFactory::GetIntegerValue(static_cast<int32_t>(param)));
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
      UNUSED_ATTRIBUTE const TestConfig &config) {
    // Setup the predicate a >= ? and b >= a and c >= b
    double param = (1 - config.selectivity) *
                   num_rows_to_insert * config.scale_factor;
    auto *a_col_exp =
        new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
    auto *const_exp = new expression::ConstantValueExpression(
        type::ValueFactory::GetIntegerValue(static_cast<int32_t>(param)));
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

  std::unique_ptr<planner::SeqScanPlan> ConstructScanPlan(
      const TestConfig &config) {
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
        new planner::SeqScanPlan(&GetTestTable(TestTableId()), predicate,
                                 {0, 1, 2})};
  }

  void RunCompiledExperiment(TestConfig &config, uint32_t num_runs = 5) {
    for (uint32_t i = 0; i < num_runs; i++) {
      auto scan = ConstructScanPlan(config);

      // Do binding
      planner::BindingContext context;
      scan->PerformBinding(context);

      // We collect the results of the query into an in-memory buffer
      codegen::BufferingConsumer buffer{{0}, context};

      // COMPILE and execute
      codegen::Query::RuntimeStats runtime_stats;
      CompileAndExecute(*scan, buffer, &runtime_stats);

      LOG_INFO("plan time: %lf", runtime_stats.plan_ms);
    }
  }

 private:
  constexpr static uint32_t num_rows_to_insert = 10;
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

TEST_F(BenchmarkScanTest, SelectivityTestWithCompilation) {
  TestConfig config;
  config.selectivity = 0.50;

  PrintConfig(config);
  RunCompiledExperiment(config);
}

}  // namespace test
}  // namespace peloton
