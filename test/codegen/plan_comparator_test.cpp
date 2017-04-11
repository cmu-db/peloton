//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_scan_translator_test.cpp
//
// Identification: test/codegen/plan_comparator_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/catalog.h"
#include "codegen/query_compiler.h"
#include "common/harness.h"
#include "expression/conjunction_expression.h"
#include "expression/operator_expression.h"
#include "planner/seq_scan_plan.h"
#include "planner/order_by_plan.h"
#include "codegen/plan_comparator.h"
#include "codegen/codegen_test_util.h"

namespace peloton {
namespace test {

//===----------------------------------------------------------------------===//
// This class contains code to test the correctness of plan comparator by
// generating pairs of same/different plans of different kinds and testing
// the comparison result.
// The schema of the table is as follows:
//
// +---------+---------+---------+-------------+
// | A (int) | B (int) | C (int) | D (varchar) |
// +---------+---------+---------+-------------+
//
// The database and tables are created in CreateDatabase() and
// CreateTestTables(), respectively.
//
// By default, the test table is loaded with 64 rows of random values.
// The right table is loaded with 256 rows of random values.
//===----------------------------------------------------------------------===//
typedef std::unique_ptr<const expression::AbstractExpression> AbstractExprPtr;

class PlanComparatorTest : public PelotonCodeGenTest {
public:
  PlanComparatorTest() : PelotonCodeGenTest(), num_rows_to_insert(64) {
    // Load test table
    LoadTestTable(TestTableId(), num_rows_to_insert);
    LoadTestTable(RightTableId(), 4 * num_rows_to_insert);
  }

  uint32_t TestTableId() { return test_table1_id; }
  uint32_t RightTableId() const { return test_table2_id; }

private:
  uint32_t num_rows_to_insert = 64;
};

TEST_F(PlanComparatorTest, SeqScanConjunctionPredicateEqualityCheck) {
  //
  // SELECT a, b, c FROM table where a >= 20 and b = 21;
  //
  auto* a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* const_20_exp = CodegenTestUtils::ConstIntExpression(20);
  auto* a_gt_20 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_GREATERTHANOREQUALTO, a_col_exp, const_20_exp);

  auto* a_col_exp_2 =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* const_20_exp_2 = CodegenTestUtils::ConstIntExpression(20);
  auto* a_gt_20_2 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_GREATERTHANOREQUALTO, a_col_exp_2, const_20_exp_2);

  // b = 21
  auto* b_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 1);
  auto* const_21_exp = CodegenTestUtils::ConstIntExpression(21);
  auto* b_eq_21 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, b_col_exp, const_21_exp);

  auto* b_col_exp_2 =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 1);
  auto* const_21_exp_2 = CodegenTestUtils::ConstIntExpression(21);
  auto* b_eq_21_2 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, b_col_exp_2, const_21_exp_2);

  // a >= 20 AND b = 21
  auto* conj_eq = new expression::ConjunctionExpression(
      ExpressionType::CONJUNCTION_AND, b_eq_21, a_gt_20);

  auto* conj_eq_2 = new expression::ConjunctionExpression(
      ExpressionType::CONJUNCTION_AND, b_eq_21_2, a_gt_20_2);

  // 2) Setup the scan plan node
  planner::SeqScanPlan scan{&GetTestTable(TestTableId()), conj_eq, {0, 1, 2}};
  planner::SeqScanPlan scan2{&GetTestTable(TestTableId()), conj_eq_2, {0, 1, 2}};

  // 3) Do binding
  planner::BindingContext context, context2;
  scan.PerformBinding(context);
  scan2.PerformBinding(context2);

  int ret = codegen::PlanComparator::Compare(scan, scan2);
  EXPECT_EQ(ret, 0);
}

TEST_F(PlanComparatorTest, SeqScanAddPredicateEqualityCheck) {
  //
  // SELECT a, b FROM table where b = a + 1;
  //
  // a + 1
  auto *a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto *const_1_exp = new expression::ConstantValueExpression(type::ValueFactory::GetSmallIntValue(1));
  auto *a_plus_1 = new expression::OperatorExpression(
      ExpressionType::OPERATOR_PLUS, type::Type::TypeId::INTEGER, a_col_exp,
      const_1_exp);

  auto *a_col_exp_2 =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto *const_1_exp_2 = new expression::ConstantValueExpression(type::ValueFactory::GetSmallIntValue(1));
  auto *a_plus_1_2 = new expression::OperatorExpression(
      ExpressionType::OPERATOR_PLUS, type::Type::TypeId::INTEGER, a_col_exp_2,
      const_1_exp_2);

  // b = a + 1
  auto *b_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 1);
  auto *b_eq_a_plus_1 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, b_col_exp, a_plus_1);

  auto *b_col_exp_2 =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 1);
  auto *b_eq_a_plus_1_2 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, b_col_exp_2, a_plus_1_2);

  // 2) Setup the scan plan node
  planner::SeqScanPlan scan{&GetTestTable(TestTableId()), b_eq_a_plus_1, {0, 1, 2}};
  planner::SeqScanPlan scan2{&GetTestTable(TestTableId()), b_eq_a_plus_1_2, {0, 1, 2}};

  // 3) Do binding
  planner::BindingContext context, context2;
  scan.PerformBinding(context);
  scan2.PerformBinding(context2);

  int ret = codegen::PlanComparator::Compare(scan, scan2);
  EXPECT_EQ(ret, 0);
}

TEST_F(PlanComparatorTest, OrderByEqualityCheck) {
  //
  // SELECT * FROM test_table ORDER BY a DESC;
  //
  std::unique_ptr<planner::OrderByPlan> order_by_plan{
      new planner::OrderByPlan({1, 0}, {true, false}, {0, 1, 2, 3})};
  std::unique_ptr<planner::OrderByPlan> order_by_plan_2{
      new planner::OrderByPlan({1, 0}, {true, false}, {0, 1, 2, 3})};
  std::unique_ptr<planner::SeqScanPlan> seq_scan_plan{new planner::SeqScanPlan(
      &GetTestTable(TestTableId()), nullptr, {0, 1, 2, 3})};
  std::unique_ptr<planner::SeqScanPlan> seq_scan_plan_2{new planner::SeqScanPlan(
      &GetTestTable(TestTableId()), nullptr, {0, 1, 2, 3})};

  order_by_plan->AddChild(std::move(seq_scan_plan));
  order_by_plan_2->AddChild(std::move(seq_scan_plan_2));

  planner::BindingContext context, context2;
  order_by_plan->PerformBinding(context);
  order_by_plan_2->PerformBinding(context2);
  int ret = codegen::PlanComparator::Compare(*order_by_plan.get(), *order_by_plan_2.get());
  EXPECT_EQ(ret, 0);
}

TEST_F(PlanComparatorTest, OrderByInequalityCheck) {
  //
  // SELECT * FROM test_table ORDER BY b, a ASC;
  // SELECT * FROM test_table ORDER BY b DESC a ASC;
  //
  std::unique_ptr<planner::OrderByPlan> order_by_plan{
      new planner::OrderByPlan({1, 0}, {false, false}, {0, 1, 2, 3})};
  std::unique_ptr<planner::OrderByPlan> order_by_plan_2{
      new planner::OrderByPlan({1, 0}, {true, false}, {0, 1, 2, 3})};
  std::unique_ptr<planner::SeqScanPlan> seq_scan_plan{new planner::SeqScanPlan(
      &GetTestTable(TestTableId()), nullptr, {0, 1, 2, 3})};
  std::unique_ptr<planner::SeqScanPlan> seq_scan_plan_2{new planner::SeqScanPlan(
      &GetTestTable(TestTableId()), nullptr, {0, 1, 2, 3})};

  order_by_plan->AddChild(std::move(seq_scan_plan));
  order_by_plan_2->AddChild(std::move(seq_scan_plan_2));

  planner::BindingContext context, context2;
  order_by_plan->PerformBinding(context);
  order_by_plan_2->PerformBinding(context2);
  int ret = (codegen::PlanComparator::Compare(*order_by_plan.get(), *order_by_plan_2.get()) == 0);
  EXPECT_EQ(ret, 0);
}

TEST_F(PlanComparatorTest, AggregatePlanEqualityCheck) {
  //
  // SELECT a, avg(b) as x FROM table GROUP BY a WHERE x > 50;
  //

  // 1) Set up projection (just a direct map)
  DirectMapList direct_map_list = {{0, {0, 0}}, {1, {1, 0}}};
  std::unique_ptr<planner::ProjectInfo> proj_info{
      new planner::ProjectInfo(TargetList(), std::move(direct_map_list))};
  std::unique_ptr<planner::ProjectInfo> proj_info_2{
      new planner::ProjectInfo(TargetList(), std::move(direct_map_list))};

  // 2) Setup the average over 'b'
  auto* tve_expr =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 1);
  auto* tve_expr_2 =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 1);
  std::vector<planner::AggregatePlan::AggTerm> agg_terms = {
      {ExpressionType::AGGREGATE_AVG, tve_expr}};
  std::vector<planner::AggregatePlan::AggTerm> agg_terms_2 = {
      {ExpressionType::AGGREGATE_AVG, tve_expr_2}};
  agg_terms[0].agg_ai.type = type::Type::TypeId::DECIMAL;
  agg_terms_2[0].agg_ai.type = type::Type::TypeId::DECIMAL;

  // 3) The grouping column
  std::vector<oid_t> gb_cols = {0};

  // 4) The output schema
  std::shared_ptr<const catalog::Schema> output_schema{
      new catalog::Schema({{type::Type::TypeId::INTEGER, 4, "COL_A"},
                           {type::Type::TypeId::DECIMAL, 8, "AVG(COL_B)"}})};

  // 5) The predicate on the average aggregate
  auto* x_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 1);
  auto* x_exp_2 =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 1);
  auto* const_50 = CodegenTestUtils::ConstIntExpression(50);
  auto* const_50_2 = CodegenTestUtils::ConstIntExpression(50);
  std::unique_ptr<expression::AbstractExpression> x_gt_50{
      new expression::ComparisonExpression(ExpressionType::COMPARE_GREATERTHAN,
                                           x_exp, const_50)};
  std::unique_ptr<expression::AbstractExpression> x_gt_50_2{
      new expression::ComparisonExpression(ExpressionType::COMPARE_GREATERTHAN,
                                           x_exp_2, const_50_2)};

  // 6) Finally, the aggregation node
  std::unique_ptr<planner::AbstractPlan> agg_plan{new planner::AggregatePlan(
      std::move(proj_info), std::move(x_gt_50), std::move(agg_terms),
      std::move(gb_cols), output_schema, AggregateType::HASH)};
  std::unique_ptr<planner::AbstractPlan> agg_plan_2{new planner::AggregatePlan(
      std::move(proj_info_2), std::move(x_gt_50_2), std::move(agg_terms_2),
      std::move(gb_cols), output_schema, AggregateType::HASH)};

  // 7) The scan that feeds the aggregation
  std::unique_ptr<planner::AbstractPlan> scan_plan{
      new planner::SeqScanPlan(&GetTestTable(TestTableId()), nullptr, {0, 1})};
  std::unique_ptr<planner::AbstractPlan> scan_plan_2{
      new planner::SeqScanPlan(&GetTestTable(TestTableId()), nullptr, {0, 1})};

  agg_plan->AddChild(std::move(scan_plan));
  agg_plan_2->AddChild(std::move(scan_plan_2));
  planner::BindingContext context, context2;
  agg_plan->PerformBinding(context);
  agg_plan_2->PerformBinding(context2);
  int ret = codegen::PlanComparator::Compare(*agg_plan.get(), *agg_plan_2.get());
  EXPECT_EQ(ret, 0);
}


TEST_F(PlanComparatorTest, AggregatePlanInequalityCheck) {
  //
  // SELECT a, avg(b) as x FROM table GROUP BY a WHERE x > 50;
  // SELECT a, avg(b) as x FROM table GROUP BY a WHERE a > 50;
  //

  // 1) Set up projection (just a direct map)
  DirectMapList direct_map_list = {{0, {0, 0}}, {1, {1, 0}}};
  std::unique_ptr<planner::ProjectInfo> proj_info{
      new planner::ProjectInfo(TargetList(), std::move(direct_map_list))};
  std::unique_ptr<planner::ProjectInfo> proj_info_2{
      new planner::ProjectInfo(TargetList(), std::move(direct_map_list))};

  // 2) Setup the average over 'b'
  auto* tve_expr =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 1);
  auto* tve_expr_2 =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 1);
  std::vector<planner::AggregatePlan::AggTerm> agg_terms = {
      {ExpressionType::AGGREGATE_AVG, tve_expr}};
  std::vector<planner::AggregatePlan::AggTerm> agg_terms_2 = {
      {ExpressionType::AGGREGATE_AVG, tve_expr_2}};
  agg_terms[0].agg_ai.type = type::Type::TypeId::DECIMAL;
  agg_terms_2[0].agg_ai.type = type::Type::TypeId::DECIMAL;

  // 3) The grouping column
  std::vector<oid_t> gb_cols = {0};

  // 4) The output schema
  std::shared_ptr<const catalog::Schema> output_schema{
      new catalog::Schema({{type::Type::TypeId::INTEGER, 4, "COL_A"},
                           {type::Type::TypeId::DECIMAL, 8, "AVG(COL_B)"}})};

  // 5) The predicate on the average aggregate
  auto* x_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 1);
  auto* a_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* const_50 = CodegenTestUtils::ConstIntExpression(50);
  auto* const_50_2 = CodegenTestUtils::ConstIntExpression(50);
  std::unique_ptr<expression::AbstractExpression> x_gt_50{
      new expression::ComparisonExpression(ExpressionType::COMPARE_GREATERTHAN,
                                           x_exp, const_50)};
  auto* a_gt_50 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_GREATERTHAN, a_exp, const_50_2);

  // 6) Finally, the aggregation node
  std::unique_ptr<planner::AbstractPlan> agg_plan{new planner::AggregatePlan(
      std::move(proj_info), std::move(x_gt_50), std::move(agg_terms),
      std::move(gb_cols), output_schema, AggregateType::HASH)};

  std::unique_ptr<planner::AbstractPlan> agg_plan_2{new planner::AggregatePlan(
      std::move(proj_info_2), nullptr, std::move(agg_terms_2),
      std::move(gb_cols), output_schema, AggregateType::HASH)};

  // 7) The scan that feeds the aggregation
  std::unique_ptr<planner::AbstractPlan> scan_plan{
      new planner::SeqScanPlan(&GetTestTable(TestTableId()), nullptr, {0, 1})};
  std::unique_ptr<planner::AbstractPlan> scan_plan_2{
      new planner::SeqScanPlan(&GetTestTable(TestTableId()), a_gt_50, {0, 1})};

  agg_plan->AddChild(std::move(scan_plan));
  agg_plan_2->AddChild(std::move(scan_plan_2));
  planner::BindingContext context, context2;
  agg_plan->PerformBinding(context);
  agg_plan_2->PerformBinding(context2);
  int ret = (codegen::PlanComparator::Compare(*agg_plan.get(), *agg_plan_2.get()) == 0);
  EXPECT_EQ(ret, 0);
}


TEST_F(PlanComparatorTest, HashJoinPlanEqualityCheck) {
  //
  // SELECT
  //   left_table.a, right_table.a, left_table.b, right_table.c,
  // FROM
  //   left_table
  // JOIN
  //   right_table ON left_table.a = right_table.a
  //

  // Projection:  [left_table.a, right_table.a, left_table.b, right_table.c]
  DirectMap dm1 = std::make_pair(0, std::make_pair(0, 0));
  DirectMap dm2 = std::make_pair(1, std::make_pair(1, 0));
  DirectMap dm3 = std::make_pair(2, std::make_pair(0, 1));
  DirectMap dm4 = std::make_pair(3, std::make_pair(1, 2));
  DirectMapList direct_map_list = {dm1, dm2, dm3, dm4};
  std::unique_ptr<planner::ProjectInfo> projection{
      new planner::ProjectInfo(TargetList{}, std::move(direct_map_list))};
  std::unique_ptr<planner::ProjectInfo> projection_2{
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
  std::vector<AbstractExprPtr> left_hash_keys_2;
  left_hash_keys.emplace_back(
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0));

  std::vector<AbstractExprPtr> right_hash_keys;
  right_hash_keys.emplace_back(
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 1, 0));
  std::vector<AbstractExprPtr> right_hash_keys_2;
  right_hash_keys_2.emplace_back(
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 1, 0));

  std::vector<AbstractExprPtr> hash_keys;
  hash_keys.emplace_back(
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 1, 0));
  std::vector<AbstractExprPtr> hash_keys_2;
  hash_keys_2.emplace_back(
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 1, 0));

  // Finally, the fucking join node
  std::unique_ptr<planner::HashJoinPlan> hj_plan{
      new planner::HashJoinPlan(JoinType::INNER, nullptr, std::move(projection),
                                schema, left_hash_keys, right_hash_keys)};
  std::unique_ptr<planner::HashPlan> hash_plan{
      new planner::HashPlan(hash_keys)};

  std::unique_ptr<planner::HashJoinPlan> hj_plan_2{
      new planner::HashJoinPlan(JoinType::INNER, nullptr, std::move(projection_2),
                                schema, left_hash_keys_2, right_hash_keys_2)};
  std::unique_ptr<planner::HashPlan> hash_plan_2{
      new planner::HashPlan(hash_keys_2)};


  std::unique_ptr<planner::AbstractPlan> left_scan{
      new planner::SeqScanPlan(&GetTestTable(TestTableId()), nullptr, {0, 1, 2})};
  std::unique_ptr<planner::AbstractPlan> right_scan{
      new planner::SeqScanPlan(&GetTestTable(RightTableId()), nullptr, {0, 1, 2})};

  std::unique_ptr<planner::AbstractPlan> left_scan_2{
      new planner::SeqScanPlan(&GetTestTable(TestTableId()), nullptr, {0, 1, 2})};
  std::unique_ptr<planner::AbstractPlan> right_scan_2{
      new planner::SeqScanPlan(&GetTestTable(RightTableId()), nullptr, {0, 1, 2})};

  hash_plan->AddChild(std::move(right_scan));
  hj_plan->AddChild(std::move(left_scan));
  hj_plan->AddChild(std::move(hash_plan));

  hash_plan_2->AddChild(std::move(right_scan_2));
  hj_plan_2->AddChild(std::move(left_scan_2));
  hj_plan_2->AddChild(std::move(hash_plan_2));

  // Do binding
  planner::BindingContext context, context2;
  hj_plan->PerformBinding(context);
  hj_plan_2->PerformBinding(context2);
  int ret = codegen::PlanComparator::Compare(*hj_plan.get(), *hj_plan_2.get());
  EXPECT_EQ(ret, 0);
}

}
}