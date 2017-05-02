//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_join_translator_test.cpp
//
// Identification: test/codegen/hash_join_translator_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/buffering_consumer.h"
#include "codegen/query_compiler.h"
#include "codegen/runtime_functions_proxy.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/hash_join_executor.h"
#include "executor/seq_scan_executor.h"
#include "expression/comparison_expression.h"
#include "expression/conjunction_expression.h"
#include "expression/operator_expression.h"
#include "storage/table_factory.h"
#include "planner/hash_join_plan.h"
#include "planner/hash_plan.h"
#include "planner/seq_scan_plan.h"
#include "type/types.h"

#include "codegen/codegen_test_util.h"

namespace peloton {
namespace test {

enum JoinComplexity { SIMPLE, MODERATE, COMPLEX };

class BenchmarkJoinTest : public PelotonCodeGenTest {
 public:
  typedef std::unique_ptr<const expression::AbstractExpression> AbstractExprPtr;

  BenchmarkJoinTest() : PelotonCodeGenTest() {
    // Load test table
    LoadTestTable(LeftTableId(), num_rows_to_insert);
    LoadTestTable(RightTableId(), 4 * num_rows_to_insert);
  }

  uint32_t LeftTableId() const { return test_table1_id; }

  uint32_t RightTableId() const { return test_table2_id; }

  storage::DataTable& GetLeftTable() const {
    return GetTestTable(LeftTableId());
  }

  storage::DataTable& GetRightTable() const {
    return GetTestTable(RightTableId());
  }

  AbstractExprPtr ConstructSimplePredicate() {
    // Construct join predicate:
    //   left_table.a = right_table.a
    auto *left_a =
        new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
    auto *right_a =
        new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
    AbstractExprPtr left_a_eq_right_a{
        new expression::ComparisonExpression(
            ExpressionType::COMPARE_EQUAL, left_a, right_a)};
    return left_a_eq_right_a;
  }

  AbstractExprPtr ConstructModeratePredicate() {
    // Construct join predicate:
    //   left_table.a = right_table.a AND left_table.b = right_table.b + 100

    // left_table.a = right_table.a
    auto *left_a =
        new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
    auto *right_a =
        new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
    auto *left_a_eq_right_a =
        new expression::ComparisonExpression(
            ExpressionType::COMPARE_EQUAL, left_a, right_a);

    // right_table.b + 100
    auto *right_b =
        new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 1);
    auto *const_100_exp = new expression::ConstantValueExpression(
        type::ValueFactory::GetIntegerValue(100));
    auto *right_b_plus_100 =
        new expression::OperatorExpression(
            ExpressionType::OPERATOR_PLUS, type::Type::TypeId::INTEGER, right_b, const_100_exp);

    // left_table.b = right_table.b + 100
    auto *left_b =
        new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 1);
    auto *left_b_eq_right_b_plus_100 =
        new expression::ComparisonExpression(
            ExpressionType::COMPARE_EQUAL, left_b, right_b_plus_100);

    // left_table.a = right_table.a AND left_table.b = right_table.b + 100
    AbstractExprPtr conj_exp{
        new expression::ConjunctionExpression(
            ExpressionType::CONJUNCTION_AND, left_a_eq_right_a,
            left_b_eq_right_b_plus_100)};
    return conj_exp;
  }

  AbstractExprPtr ConstructComplexPredicate() {
    // Construct join predicate:
    //   left_table.a = right_table.a AND left_table.b = right_table.b + 100
    //       AND left_table.c = right_table.a * 1000

    // left_table.a = right_table.a
    auto *left_a =
        new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
    auto *right_a1 =
        new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
    auto *left_a_eq_right_a =
        new expression::ComparisonExpression(
            ExpressionType::COMPARE_EQUAL, left_a, right_a1);

    // right_table.b + 100
    auto *right_b =
        new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 1);
    auto *const_100_exp = new expression::ConstantValueExpression(
        type::ValueFactory::GetIntegerValue(100));
    auto *right_b_plus_100 =
        new expression::OperatorExpression(
            ExpressionType::OPERATOR_PLUS, type::Type::TypeId::INTEGER, right_b, const_100_exp);

    // left_table.b = right_table.b + 100
    auto *left_b =
        new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 1);
    auto *left_b_eq_right_b_plus_100 =
        new expression::ComparisonExpression(
            ExpressionType::COMPARE_EQUAL, left_b, right_b_plus_100);

    // left_table.a = right_table.a AND left_table.b = right_table.b + 100
    auto *conj_exp_1 =
        new expression::ConjunctionExpression(
            ExpressionType::CONJUNCTION_AND, left_a_eq_right_a,
            left_b_eq_right_b_plus_100);

    // right_table.a * 1000
    auto *right_a2 =
        new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
    auto *const_1000_exp = new expression::ConstantValueExpression(
        type::ValueFactory::GetIntegerValue(1000));
    auto *right_a_times_1000 =
        new expression::OperatorExpression(
            ExpressionType::OPERATOR_MULTIPLY, type::Type::TypeId::INTEGER, right_a2, const_1000_exp);

    // left_table.c = right_table.a * 1000
    auto *left_c =
        new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 2);
    auto *left_c_eq_right_a_mul_1000 =
        new expression::ComparisonExpression(
            ExpressionType::COMPARE_EQUAL, left_c, right_a_times_1000);

    // left_table.a = right_table.a AND left_table.b = right_table.b + 100
    //     AND left_table.c = right_table.a * 1000
    AbstractExprPtr conj_exp_2{
        new expression::ConjunctionExpression(
            ExpressionType::CONJUNCTION_AND, conj_exp_1,
            left_c_eq_right_a_mul_1000)};
    return conj_exp_2;
  }

  std::unique_ptr<planner::HashJoinPlan> ConstructJoinPlan(
      JoinComplexity complexity) {
    AbstractExprPtr predicate = nullptr;
    switch (complexity) {
      case JoinComplexity::SIMPLE:
        predicate = ConstructSimplePredicate();
        break;
      case JoinComplexity::MODERATE:
        predicate = ConstructModeratePredicate();
        break;
      case JoinComplexity::COMPLEX:
        predicate = ConstructModeratePredicate();
        break;
      default: { throw Exception{"nope"}; }
    }

    // Projection:  [left_table.a, right_table.a, left_table.b, right_table.c]
    DirectMap dm1 = std::make_pair(0, std::make_pair(0, 0));
    DirectMap dm2 = std::make_pair(1, std::make_pair(1, 0));
    DirectMap dm3 = std::make_pair(2, std::make_pair(0, 1));
    DirectMap dm4 = std::make_pair(3, std::make_pair(1, 2));
    DirectMapList direct_map_list = {dm1, dm2, dm3, dm4};
    std::unique_ptr<planner::ProjectInfo> projection{
        new planner::ProjectInfo(TargetList{}, std::move(direct_map_list))};

    // Output schema
    auto schema = std::shared_ptr<const catalog::Schema>(
        new catalog::Schema({TestingExecutorUtil::GetColumnInfo(0),
                             TestingExecutorUtil::GetColumnInfo(0),
                             TestingExecutorUtil::GetColumnInfo(1),
                             TestingExecutorUtil::GetColumnInfo(2)}));

    // Left and right hash keys
    std::vector<AbstractExprPtr> left_hash_keys;
    left_hash_keys.emplace_back(
        new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0));

    std::vector<AbstractExprPtr> right_hash_keys;
    right_hash_keys.emplace_back(
        new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 1, 0));

    std::vector<AbstractExprPtr> hash_keys;
    hash_keys.emplace_back(
        new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 1, 0));

    // Finally, the fucking join node
    std::unique_ptr<planner::HashJoinPlan> hj_plan{new planner::HashJoinPlan(
        JoinType::INNER, std::move(predicate), std::move(projection), schema,
        left_hash_keys, right_hash_keys)};
    std::unique_ptr<planner::HashPlan> hash_plan{
        new planner::HashPlan(hash_keys)};

    std::unique_ptr<planner::AbstractPlan> left_scan{
        new planner::SeqScanPlan(&GetLeftTable(), nullptr, {0, 1, 2})};
    std::unique_ptr<planner::AbstractPlan> right_scan{
        new planner::SeqScanPlan(&GetRightTable(), nullptr, {0, 1, 2})};

    hash_plan->AddChild(std::move(right_scan));
    hj_plan->AddChild(std::move(left_scan));
    hj_plan->AddChild(std::move(hash_plan));

    return hj_plan;
  }

  Stats RunCompiledExperiment(JoinComplexity complexity, uint32_t num_runs = 5) {
    // Keep one copy of compile and runtime stats
    Stats stats;

    for (uint32_t i = 0; i < num_runs; i++) {

      auto join_plan = ConstructJoinPlan(complexity);

      // Do binding
      planner::BindingContext context;
      join_plan->PerformBinding(context);

      // We collect the results of the query into an in-memory buffer
      codegen::BufferingConsumer buffer{{0, 1, 2}, context};

      // COMPILE and execute
      codegen::Query::RuntimeStats runtime_stats;
      codegen::QueryCompiler::CompileStats compile_stats = CompileAndExecute(
          *join_plan, buffer, reinterpret_cast<char*>(buffer.GetState()), &runtime_stats);

      stats.Merge(compile_stats, runtime_stats,
                  buffer.GetOutputTuples().size());
    }

    stats.Finalize();
    return stats;
  }

  Stats RunInterpretedExperiment(JoinComplexity complexity, uint32_t num_runs = 5) {
    // Keep one copy of compile and runtime stats
    Stats stats;

    for (uint32_t i = 0; i < num_runs; i++) {
      auto join_plan = ConstructJoinPlan(complexity);
      std::vector<std::vector<type::Value>> vals;

      codegen::QueryCompiler::CompileStats compile_stats;
      codegen::Query::RuntimeStats runtime_stats;

      auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
      auto txn = txn_manager.BeginTransaction();

      executor::ExecutorContext ctx{txn};
      executor::HashJoinExecutor hj_exec{join_plan.get(), &ctx};

      executor::SeqScanExecutor left_exec{join_plan->GetChild(0), &ctx};
      executor::HashExecutor hash_exec{join_plan->GetChild(1), &ctx};
      executor::SeqScanExecutor right_exec{join_plan->GetChild(1)->GetChild(0), &ctx};


      hj_exec.AddChild(&left_exec);
      hj_exec.AddChild(&hash_exec);
      hash_exec.AddChild(&right_exec);

      Timer<std::ratio<1, 1000>> timer;
      timer.Start();
      hj_exec.Init();
      timer.Stop();
      runtime_stats.init_ms = timer.GetDuration();
      timer.Reset();

      // Run the hash_join_executor
      timer.Start();
      while (hj_exec.Execute()) {
        auto tile = hj_exec.GetOutput();
        for (oid_t tuple_id : *tile) {
          const expression::ContainerTuple<executor::LogicalTile> tuple{
              tile, tuple_id};
          std::vector<type::Value> tv;
          for (oid_t col_id : {0, 1, 2}) {
            tv.push_back(tuple.GetValue(col_id));
          }
          vals.push_back(std::move(tv));
        }
      }
      timer.Stop();
      runtime_stats.plan_ms = timer.GetDuration();

      stats.Merge(compile_stats, runtime_stats, vals.size());
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

TEST_F(BenchmarkJoinTest, RowLayoutWithCompilationTest) {
  JoinComplexity complexities[] = {SIMPLE, MODERATE, COMPLEX};

  PrintName("JOIN_COMPLEXITY: COMPILATION");
  for (JoinComplexity complexity : complexities) {
    auto stats = RunCompiledExperiment(complexity);
    stats.PrintStats();
  }
}

TEST_F(BenchmarkJoinTest, RowLayoutWithInterpretationTest) {
  JoinComplexity complexities[] = {SIMPLE, MODERATE, COMPLEX};

  PrintName("JOIN_COMPLEXITY: INTERPRETATION");
  for (JoinComplexity complexity : complexities) {
    auto stats = RunInterpretedExperiment(complexity);
    stats.PrintStats();
  }
}

}  // namespace test
}  // namespace peloton
