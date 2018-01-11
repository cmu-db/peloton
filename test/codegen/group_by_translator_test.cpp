//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// group_by_translator_test.cpp
//
// Identification: test/codegen/group_by_translator_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/catalog.h"
#include "codegen/proxy/runtime_functions_proxy.h"
#include "codegen/query_compiler.h"
#include "common/harness.h"
#include "expression/comparison_expression.h"
#include "expression/conjunction_expression.h"
#include "expression/tuple_value_expression.h"
#include "planner/aggregate_plan.h"
#include "planner/seq_scan_plan.h"

#include "codegen/testing_codegen_util.h"

namespace peloton {
namespace test {

class GroupByTranslatorTest : public PelotonCodeGenTest {
 public:
  GroupByTranslatorTest() : PelotonCodeGenTest() {
    uint32_t num_rows = 10;
    LoadTestTable(TestTableId(), num_rows);
  }

  oid_t TestTableId() const { return test_table_oids[0]; }
};

TEST_F(GroupByTranslatorTest, SingleColumnGrouping) {
  //
  // SELECT a, count(*) FROM table GROUP BY a;
  //

  LOG_INFO("Query: SELECT a, COUNT(*) FROM table1 GROUP BY a;");

  // 1) Set up projection (just a direct map)
  DirectMapList direct_map_list = {{0, {0, 0}}, {1, {1, 0}}};
  std::unique_ptr<planner::ProjectInfo> proj_info{
      new planner::ProjectInfo(TargetList{}, std::move(direct_map_list))};

  // 2) Setup the aggregations
  // For count(*) just use a TVE
  auto *tve_expr =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
  std::vector<planner::AggregatePlan::AggTerm> agg_terms = {
      {ExpressionType::AGGREGATE_COUNT_STAR, tve_expr}};

  // 3) The grouping column
  std::vector<oid_t> gb_cols = {0};

  // 4) The output schema
  std::shared_ptr<const catalog::Schema> output_schema{
      new catalog::Schema({{type::TypeId::INTEGER, 4, "COL_A"},
                           {type::TypeId::BIGINT, 8, "COUNT_A"}})};

  // 5) Finally, the aggregation node
  std::unique_ptr<planner::AbstractPlan> agg_plan{new planner::AggregatePlan(
      std::move(proj_info), nullptr, std::move(agg_terms), std::move(gb_cols),
      output_schema, AggregateType::HASH)};

  // 6) The scan that feeds the aggregation
  std::unique_ptr<planner::AbstractPlan> scan_plan{
      new planner::SeqScanPlan(&GetTestTable(TestTableId()), nullptr, {0})};

  agg_plan->AddChild(std::move(scan_plan));

  // Do binding
  planner::BindingContext context;
  agg_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1}, context};

  // Compile and run
  CompileAndExecute(*agg_plan, buffer);

  // Check results
  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(10, results.size());

  // Each group should have a count of one (since the grouping column is unique)
  type::Value const_one = type::ValueFactory::GetIntegerValue(1);
  for (const auto &tuple : results) {
    EXPECT_TRUE(tuple.GetValue(1).CompareEquals(const_one) ==
                CmpBool::TRUE);
  }
}

TEST_F(GroupByTranslatorTest, MultiColumnGrouping) {
  //
  // SELECT a, b, count(*) FROM table GROUP BY a, b;
  //

  LOG_INFO("Query: SELECT a, b, COUNT(*) FROM table1 GROUP BY a, b;");

  // 1) Set up projection (just a direct map)
  DirectMapList direct_map_list = {{0, {0, 0}}, {1, {0, 1}}, {2, {1, 0}}};
  std::unique_ptr<planner::ProjectInfo> proj_info{
      new planner::ProjectInfo(TargetList(), std::move(direct_map_list))};

  // 2) Setup the aggregations
  // For count(*) just use a TVE
  auto *tve_expr =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
  std::vector<planner::AggregatePlan::AggTerm> agg_terms = {
      {ExpressionType::AGGREGATE_COUNT_STAR, tve_expr}};

  // 3) The grouping column
  std::vector<oid_t> gb_cols = {0, 1};

  // 4) The output schema
  std::shared_ptr<const catalog::Schema> output_schema{
      new catalog::Schema({{type::TypeId::INTEGER, 4, "COL_A"},
                           {type::TypeId::INTEGER, 4, "COL_B"},
                           {type::TypeId::BIGINT, 8, "COUNT_*"}})};

  // 5) Finally, the aggregation node
  std::unique_ptr<planner::AbstractPlan> agg_plan{new planner::AggregatePlan(
      std::move(proj_info), nullptr, std::move(agg_terms), std::move(gb_cols),
      output_schema, AggregateType::HASH)};

  // 6) The scan that feeds the aggregation
  std::unique_ptr<planner::AbstractPlan> scan_plan{
      new planner::SeqScanPlan(&GetTestTable(TestTableId()), nullptr, {0, 1})};

  agg_plan->AddChild(std::move(scan_plan));

  // Do binding
  planner::BindingContext context;
  agg_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1, 2}, context};

  // Compile it all
  CompileAndExecute(*agg_plan, buffer);

  // Check results
  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(10, results.size());

  // Each group should have a count of one since the grouping columns are unique
  type::Value const_one = type::ValueFactory::GetIntegerValue(1);
  for (const auto &tuple : results) {
    type::Value group_count = tuple.GetValue(2);
    EXPECT_TRUE(group_count.CompareEquals(const_one) == CmpBool::TRUE);
  }
}

TEST_F(GroupByTranslatorTest, AverageAggregation) {
  //
  // SELECT a, avg(b) FROM table GROUP BY a;
  //

  LOG_INFO("Query: SELECT a, AVG(b) FROM table1 GROUP BY a;");

  // 1) Set up projection (just a direct map)
  DirectMapList direct_map_list = {{0, {0, 0}}, {1, {1, 0}}};
  std::unique_ptr<planner::ProjectInfo> proj_info{
      new planner::ProjectInfo(TargetList(), std::move(direct_map_list))};

  // 2) Setup the average over 'b'
  auto *tve_expr =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 1);
  std::vector<planner::AggregatePlan::AggTerm> agg_terms = {
      {ExpressionType::AGGREGATE_AVG, tve_expr}};

  // 3) The grouping column
  std::vector<oid_t> gb_cols = {0};

  // 4) The output schema
  std::shared_ptr<const catalog::Schema> output_schema{
      new catalog::Schema({{type::TypeId::INTEGER, 4, "COL_A"},
                           {type::TypeId::DECIMAL, 8, "AVG(COL_B)"}})};

  // 5) Finally, the aggregation node
  std::unique_ptr<planner::AbstractPlan> agg_plan{new planner::AggregatePlan(
      std::move(proj_info), nullptr, std::move(agg_terms), std::move(gb_cols),
      output_schema, AggregateType::HASH)};

  // 6) The scan that feeds the aggregation
  std::unique_ptr<planner::AbstractPlan> scan_plan{
      new planner::SeqScanPlan(&GetTestTable(TestTableId()), nullptr, {0, 1})};

  agg_plan->AddChild(std::move(scan_plan));

  // Do binding
  planner::BindingContext context;
  agg_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1}, context};

  // Compile it all
  CompileAndExecute(*agg_plan, buffer);

  // Check results
  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(10, results.size());
}

TEST_F(GroupByTranslatorTest, AggregationWithOutputPredicate) {
  //
  // SELECT a, avg(b) as x FROM table GROUP BY a WHERE x > 50;
  //

  LOG_INFO("Query: SELECT a, avg(b) as x FROM table GROUP BY a WHERE x > 50;");

  // 1) Set up projection (just a direct map)
  DirectMapList direct_map_list = {{0, {0, 0}}, {1, {1, 0}}};
  std::unique_ptr<planner::ProjectInfo> proj_info{
      new planner::ProjectInfo(TargetList(), std::move(direct_map_list))};

  // 2) Setup the average over 'b'
  auto *tve_expr =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 1);
  std::vector<planner::AggregatePlan::AggTerm> agg_terms = {
      {ExpressionType::AGGREGATE_AVG, tve_expr}};

  // 3) The grouping column
  std::vector<oid_t> gb_cols = {0};

  // 4) The output schema
  std::shared_ptr<const catalog::Schema> output_schema{
      new catalog::Schema({{type::TypeId::INTEGER, 4, "COL_A"},
                           {type::TypeId::DECIMAL, 8, "AVG(COL_B)"}})};

  // 5) The predicate on the average aggregate
  auto *x_exp =
      new expression::TupleValueExpression(type::TypeId::DECIMAL, 1, 0);
  auto *const_50 = new expression::ConstantValueExpression(
      type::ValueFactory::GetDecimalValue(50.0));
  ExpressionPtr x_gt_50{
      new expression::ComparisonExpression(ExpressionType::COMPARE_GREATERTHAN,
                                           x_exp, const_50)};

  // 6) Finally, the aggregation node
  std::unique_ptr<planner::AbstractPlan> agg_plan{new planner::AggregatePlan(
      std::move(proj_info), std::move(x_gt_50), std::move(agg_terms),
      std::move(gb_cols), output_schema, AggregateType::HASH)};

  // 7) The scan that feeds the aggregation
  std::unique_ptr<planner::AbstractPlan> scan_plan{
      new planner::SeqScanPlan(&GetTestTable(TestTableId()), nullptr, {0, 1})};

  agg_plan->AddChild(std::move(scan_plan));

  // Do binding
  planner::BindingContext context;
  agg_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1}, context};

  // Compile it all
  CompileAndExecute(*agg_plan, buffer);

  // Check results
  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(5, results.size());
}

TEST_F(GroupByTranslatorTest, AggregationWithInputPredciate) {
  //
  // SELECT a, avg(b) as x FROM table GROUP BY a WHERE a > 50;
  //

  LOG_INFO("Query: SELECT a, avg(b) as x FROM table GROUP BY a WHERE a > 50;");

  // 1) Set up projection (just a direct map)
  DirectMapList direct_map_list = {{0, {0, 0}}, {1, {1, 0}}};
  std::unique_ptr<planner::ProjectInfo> proj_info{
      new planner::ProjectInfo(TargetList(), std::move(direct_map_list))};

  // 2) Setup the average over 'b'
  auto *tve_expr =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 1);
  std::vector<planner::AggregatePlan::AggTerm> agg_terms = {
      {ExpressionType::AGGREGATE_AVG, tve_expr}};

  // 3) The grouping column
  std::vector<oid_t> gb_cols = {0};

  // 4) The output schema
  std::shared_ptr<const catalog::Schema> output_schema{
      new catalog::Schema({{type::TypeId::INTEGER, 4, "COL_A"},
                           {type::TypeId::DECIMAL, 8, "AVG(COL_B)"}})};

  // 5) Finally, the aggregation node
  std::unique_ptr<planner::AbstractPlan> agg_plan{new planner::AggregatePlan(
      std::move(proj_info), nullptr, std::move(agg_terms), std::move(gb_cols),
      output_schema, AggregateType::HASH)};

  // 6) The predicate on the grouping column
  auto *a_exp =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
  auto *const_50 = ConstIntExpr(50).release();
  auto *a_gt_50 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_GREATERTHAN, a_exp, const_50);

  // 7) The scan that feeds the aggregation
  std::unique_ptr<planner::AbstractPlan> scan_plan{
      new planner::SeqScanPlan(&GetTestTable(TestTableId()), a_gt_50, {0, 1})};

  agg_plan->AddChild(std::move(scan_plan));

  // Do binding
  planner::BindingContext context;
  agg_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1}, context};

  // Compile it all
  CompileAndExecute(*agg_plan, buffer);

  // Check results. We expect four because the "A" col increases by 10 for each
  // row. For 10 rows, the four rows with A = 60, 70, 80, 90 are valid.
  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(4, results.size());
}

TEST_F(GroupByTranslatorTest, SingleCountStar) {
  //
  // SELECT count(*) FROM table;
  //

  LOG_INFO("Query: SELECT count(*) FROM table1");

  // 1) Set up projection (just a direct map)
  DirectMapList direct_map_list = {{0, {1, 0}}};
  std::unique_ptr<planner::ProjectInfo> proj_info{
      new planner::ProjectInfo(TargetList(), std::move(direct_map_list))};

  // 2) Setup the aggregations
  // For count(*) just use a TVE
  auto *tve_expr =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
  std::vector<planner::AggregatePlan::AggTerm> agg_terms = {
      {ExpressionType::AGGREGATE_COUNT_STAR, tve_expr}};

  // 3) No grouping
  std::vector<oid_t> gb_cols = {};

  // 4) The output schema
  std::shared_ptr<const catalog::Schema> output_schema{
      new catalog::Schema({{type::TypeId::BIGINT, 8, "COUNT_A"}})};

  // 5) Finally, the aggregation node
  std::unique_ptr<planner::AbstractPlan> agg_plan{new planner::AggregatePlan(
      std::move(proj_info), nullptr, std::move(agg_terms), std::move(gb_cols),
      output_schema, AggregateType::HASH)};

  // 6) The scan that feeds the aggregation
  std::unique_ptr<planner::AbstractPlan> scan_plan{
      new planner::SeqScanPlan(&GetTestTable(TestTableId()), nullptr, {0})};

  agg_plan->AddChild(std::move(scan_plan));

  // Do binding
  planner::BindingContext context;
  agg_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0}, context};

  // Compile it all
  CompileAndExecute(*agg_plan, buffer);

  // Check results
  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(1, results.size());
  EXPECT_TRUE(results[0].GetValue(0).CompareEquals(
                  type::ValueFactory::GetBigIntValue(10)) ==
              CmpBool::TRUE);
}

TEST_F(GroupByTranslatorTest, MinAndMax) {
  //
  // SELECT MAX(a), MIN(b) FROM table;
  //

  LOG_INFO("Query: SELECT MAX(a), MIN(b) FROM table1;");

  // 1) Set up projection (just a direct map)
  DirectMapList direct_map_list = {{0, {1, 0}}, {1, {1, 1}}};
  std::unique_ptr<planner::ProjectInfo> proj_info{
      new planner::ProjectInfo(TargetList{}, std::move(direct_map_list))};

  // 2) Setup MAX() aggregation on column 'a' and MIN() on 'b'
  auto *a_col =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
  auto *b_col =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 1);
  std::vector<planner::AggregatePlan::AggTerm> agg_terms = {
      {ExpressionType::AGGREGATE_MAX, a_col},
      {ExpressionType::AGGREGATE_MIN, b_col}};

  // 3) No grouping
  std::vector<oid_t> gb_cols = {};

  // 4) The output schema
  std::shared_ptr<const catalog::Schema> output_schema{
      new catalog::Schema({{type::TypeId::INTEGER, 4, "MAX_A"},
                           {type::TypeId::INTEGER, 4, "MIN_B"}})};

  // 5) Finally, the aggregation node
  std::unique_ptr<planner::AbstractPlan> agg_plan{new planner::AggregatePlan(
      std::move(proj_info), nullptr, std::move(agg_terms), std::move(gb_cols),
      output_schema, AggregateType::HASH)};

  // 6) The scan that feeds the aggregation
  std::unique_ptr<planner::AbstractPlan> scan_plan{
      new planner::SeqScanPlan(&GetTestTable(TestTableId()), nullptr, {0, 1})};

  agg_plan->AddChild(std::move(scan_plan));

  // Do binding
  planner::BindingContext context;
  agg_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1}, context};

  // Compile it all
  CompileAndExecute(*agg_plan, buffer);

  // There should only be a single output row
  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(1, results.size());

  LOG_INFO("max: %s, min: %s", results[0].GetValue(0).ToString().c_str(),
           results[0].GetValue(1).ToString().c_str());

  // The values of column 'a' are equal to the (zero-indexed) row ID * 10. The
  // maximum row ID is equal to # inserted - 1. Therefore:
  // MAX(a) = (# inserted - 1) * 10 = (10 - 1) * 10 = 9 * 10 = 90
  EXPECT_TRUE(results[0].GetValue(0).CompareEquals(
                  type::ValueFactory::GetBigIntValue(90)) ==
              CmpBool::TRUE);

  // The values of 'b' are equal to the (zero-indexed) row ID * 10 + 1. The
  // minimum row ID is 0. Therefore: MIN(b) = 0 * 10 + 1 = 1
  EXPECT_TRUE(results[0].GetValue(1).CompareEquals(
                  type::ValueFactory::GetBigIntValue(1)) ==
              CmpBool::TRUE);
}

}  // namespace test
}  // namespace peloton
