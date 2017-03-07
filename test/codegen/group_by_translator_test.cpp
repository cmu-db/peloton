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

#include "codegen/runtime_functions_proxy.h"
#include "codegen/query_compiler.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "expression/comparison_expression.h"
#include "expression/conjunction_expression.h"
#include "planner/aggregate_plan.h"
#include "planner/seq_scan_plan.h"

#include "codegen/codegen_test_util.h"
#include "executor/executor_tests_util.h"

namespace peloton {
namespace test {

//===----------------------------------------------------------------------===//
// This class contains code to test code generation and compilation of
// aggregation and group-by plans. All the tests use a single table created and
// loaded at test construction time (i.e., the constructor
// GroupByTranslatorTest()). Similarily, the resources are torn-down when the
// test class's destructor is called (i.e., ~GroupByTranslatorTest()).
//
// The schema of the testtable is as follows:
//
// +---------+---------+---------+-------------+
// | A (int) | B (int) | C (int) | D (varchar) |
// +---------+---------+---------+-------------+
//
// The database and tables are created in CreateDatabase() and
// CreateTestTables(), respectively.
//
// By default, the table is loaded with 10 rows of random values.
//===----------------------------------------------------------------------===//

class GroupByTranslatorTest : public PelotonTest {
 public:
  GroupByTranslatorTest() {
    CreateDatabase();
    CreateTestTable();
    LoadTestTable();
  }

  ~GroupByTranslatorTest() override {
    catalog::Manager::GetInstance().DropDatabaseWithOid(database->GetOid());
  }

  void CreateDatabase() {
    database = new storage::Database(INVALID_OID);
    catalog::Manager::GetInstance().AddDatabase(database);
  }

  void CreateTestTable() {
    const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;
    auto* data_table = ExecutorTestsUtil::CreateTable(tuple_count, false);
    GetDatabase().AddTable(data_table);
  }

  void LoadTestTable(uint32_t num_rows = 10) {
    auto& data_table = GetTestTable();
    auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    txn_manager.BeginTransaction();
    ExecutorTestsUtil::PopulateTable(&data_table, num_rows, false, false,
                                     false);
    txn_manager.CommitTransaction();
  }

  storage::Database& GetDatabase() { return *database; }

  storage::DataTable& GetTestTable() {
    return *GetDatabase().GetTableWithOid(INVALID_OID);
  }

 private:
  storage::Database* database;
};

TEST_F(GroupByTranslatorTest, SingleColumnGrouping) {
  //
  // SELECT a, count(*) FROM table GROUP BY a;
  //

  // 1) Set up projection (just a direct map)
  planner::ProjectInfo::DirectMapList direct_map_list = {{0, {0, 0}},
                                                         {1, {1, 0}}};
  std::unique_ptr<planner::ProjectInfo> proj_info{new planner::ProjectInfo(
      planner::ProjectInfo::TargetList(), std::move(direct_map_list))};

  // 2) Setup the aggregations
  // For count(*) just use a TVE
  auto* tve_expr =
      new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 0);
  std::vector<planner::AggregatePlan::AggTerm> agg_terms = {
      {EXPRESSION_TYPE_AGGREGATE_COUNT_STAR, tve_expr}};
  agg_terms[0].agg_ai.type = VALUE_TYPE_BIGINT;

  // 3) The grouping column
  std::vector<oid_t> gb_cols = {0};

  // 4) The output schema
  std::shared_ptr<const catalog::Schema> output_schema{new catalog::Schema(
      {{VALUE_TYPE_INTEGER, 4, "COL_A"}, {VALUE_TYPE_BIGINT, 8, "COUNT_A"}})};

  // 5) Finally, the aggregation node
  std::unique_ptr<planner::AbstractPlan> agg_plan{new planner::AggregatePlan(
      std::move(proj_info), nullptr, std::move(agg_terms), std::move(gb_cols),
      output_schema, AGGREGATE_TYPE_HASH)};

  // 6) The scan that feeds the aggregateion
  std::unique_ptr<planner::AbstractPlan> scan_plan{
      new planner::SeqScanPlan(&GetTestTable(), nullptr, {0})};

  agg_plan->AddChild(std::move(scan_plan));

  // Do binding
  planner::BindingContext context;
  agg_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  BufferingConsumer buffer{{0, 1}, context};

  // COMPILE and execute
  codegen::QueryCompiler compiler;
  auto query_statement = compiler.Compile(*agg_plan, buffer);
  query_statement->Execute(catalog::Manager::GetInstance(),
                           reinterpret_cast<char*>(buffer.GetState()));

  // Check results
  const auto& results = buffer.GetOutputTuples();
  EXPECT_EQ(results.size(), 10);

  // Each group should have a count of one (since the grouping column is unique)
  for (const auto& tuple : results) {
    EXPECT_TRUE(
        tuple.GetValue(1).OpEquals(ValueFactory::GetIntegerValue(1)).IsTrue());
  }
}

TEST_F(GroupByTranslatorTest, MultiColumnGrouping) {
  //
  // SELECT a, b, count(*) FROM table GROUP BY a, b;
  //

  // 1) Set up projection (just a direct map)
  planner::ProjectInfo::DirectMapList direct_map_list = {
      {0, {0, 0}}, {1, {0, 1}}, {2, {1, 0}}};
  std::unique_ptr<planner::ProjectInfo> proj_info{new planner::ProjectInfo(
      planner::ProjectInfo::TargetList(), std::move(direct_map_list))};

  // 2) Setup the aggregations
  // For count(*) just use a TVE
  auto* tve_expr =
      new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 0);
  std::vector<planner::AggregatePlan::AggTerm> agg_terms = {
      {EXPRESSION_TYPE_AGGREGATE_COUNT_STAR, tve_expr}};
  agg_terms[0].agg_ai.type = VALUE_TYPE_BIGINT;

  // 3) The grouping column
  std::vector<oid_t> gb_cols = {0, 1};

  // 4) The output schema
  std::shared_ptr<const catalog::Schema> output_schema{
      new catalog::Schema({{VALUE_TYPE_INTEGER, 4, "COL_A"},
                           {VALUE_TYPE_INTEGER, 4, "COL_B"},
                           {VALUE_TYPE_BIGINT, 8, "COUNT_*"}})};

  // 5) Finally, the aggregation node
  std::unique_ptr<planner::AbstractPlan> agg_plan{new planner::AggregatePlan(
      std::move(proj_info), nullptr, std::move(agg_terms), std::move(gb_cols),
      output_schema, AGGREGATE_TYPE_HASH)};

  // 6) The scan that feeds the aggregateion
  std::unique_ptr<planner::AbstractPlan> scan_plan{
      new planner::SeqScanPlan(&GetTestTable(), nullptr, {0, 1})};

  agg_plan->AddChild(std::move(scan_plan));

  // Do binding
  planner::BindingContext context;
  agg_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  BufferingConsumer buffer{{0, 1, 2}, context};

  // COMPILE and execute
  codegen::QueryCompiler compiler;
  auto query_statement = compiler.Compile(*agg_plan, buffer);
  query_statement->Execute(catalog::Manager::GetInstance(),
                           reinterpret_cast<char*>(buffer.GetState()));

  // Check results
  const auto& results = buffer.GetOutputTuples();
  EXPECT_EQ(results.size(), 10);

  // Each group should have a count of one since the grouping columns are unique
  for (const auto& tuple : results) {
    Value group_count = tuple.GetValue(2);
    EXPECT_TRUE(
        group_count.OpEquals(ValueFactory::GetIntegerValue(1)).IsTrue());
  }
}

TEST_F(GroupByTranslatorTest, AverageAggregation) {
  //
  // SELECT a, avg(b) FROM table GROUP BY a;
  //

  // 1) Set up projection (just a direct map)
  planner::ProjectInfo::DirectMapList direct_map_list = {{0, {0, 0}},
                                                         {1, {1, 0}}};
  std::unique_ptr<planner::ProjectInfo> proj_info{new planner::ProjectInfo(
      planner::ProjectInfo::TargetList(), std::move(direct_map_list))};

  // 2) Setup the average over 'b'
  auto* tve_expr =
      new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 1);
  std::vector<planner::AggregatePlan::AggTerm> agg_terms = {
      {EXPRESSION_TYPE_AGGREGATE_AVG, tve_expr}};
  agg_terms[0].agg_ai.type = VALUE_TYPE_DOUBLE;

  // 3) The grouping column
  std::vector<oid_t> gb_cols = {0};

  // 4) The output schema
  std::shared_ptr<const catalog::Schema> output_schema{
      new catalog::Schema({{VALUE_TYPE_INTEGER, 4, "COL_A"},
                           {VALUE_TYPE_DOUBLE, 8, "AVG(COL_B)"}})};

  // 5) Finally, the aggregation node
  std::unique_ptr<planner::AbstractPlan> agg_plan{new planner::AggregatePlan(
      std::move(proj_info), nullptr, std::move(agg_terms), std::move(gb_cols),
      output_schema, AGGREGATE_TYPE_HASH)};

  // 6) The scan that feeds the aggregation
  std::unique_ptr<planner::AbstractPlan> scan_plan{
      new planner::SeqScanPlan(&GetTestTable(), nullptr, {0, 1})};

  agg_plan->AddChild(std::move(scan_plan));

  // Do binding
  planner::BindingContext context;
  agg_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  BufferingConsumer buffer{{0, 1}, context};

  // COMPILE and execute
  codegen::QueryCompiler compiler;
  auto query_statement = compiler.Compile(*agg_plan, buffer);
  query_statement->Execute(catalog::Manager::GetInstance(),
                           reinterpret_cast<char*>(buffer.GetState()));

  // Check results
  const auto& results = buffer.GetOutputTuples();
  EXPECT_EQ(results.size(), 10);
}

TEST_F(GroupByTranslatorTest, AggregationWithPredicate) {
  //
  // SELECT a, avg(b) as x FROM table GROUP BY a WHERE x > 50;
  //

  // 1) Set up projection (just a direct map)
  planner::ProjectInfo::DirectMapList direct_map_list = {{0, {0, 0}},
                                                         {1, {1, 0}}};
  std::unique_ptr<planner::ProjectInfo> proj_info{new planner::ProjectInfo(
      planner::ProjectInfo::TargetList(), std::move(direct_map_list))};

  // 2) Setup the average over 'b'
  auto* tve_expr =
      new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 1);
  std::vector<planner::AggregatePlan::AggTerm> agg_terms = {
      {EXPRESSION_TYPE_AGGREGATE_AVG, tve_expr}};
  agg_terms[0].agg_ai.type = VALUE_TYPE_DOUBLE;

  // 3) The grouping column
  std::vector<oid_t> gb_cols = {0};

  // 4) The output schema
  std::shared_ptr<const catalog::Schema> output_schema{
      new catalog::Schema({{VALUE_TYPE_INTEGER, 4, "COL_A"},
                           {VALUE_TYPE_DOUBLE, 8, "AVG(COL_B)"}})};

  // 5) The predicate on the average aggregate
  auto* x_exp = new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 1);
  auto* const_50 = CodegenTestUtils::ConstIntExpression(50);
  std::unique_ptr<expression::AbstractExpression> x_gt_50{
      new expression::ComparisonExpression<expression::CmpGt>(
          EXPRESSION_TYPE_COMPARE_GREATERTHAN, x_exp, const_50)};

  // 6) Finally, the aggregation node
  std::unique_ptr<planner::AbstractPlan> agg_plan{new planner::AggregatePlan(
      std::move(proj_info), std::move(x_gt_50), std::move(agg_terms),
      std::move(gb_cols), output_schema, AGGREGATE_TYPE_HASH)};

  // 7) The scan that feeds the aggregation
  std::unique_ptr<planner::AbstractPlan> scan_plan{
      new planner::SeqScanPlan(&GetTestTable(), nullptr, {0, 1})};

  agg_plan->AddChild(std::move(scan_plan));

  // Do binding
  planner::BindingContext context;
  agg_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  BufferingConsumer buffer{{0, 1}, context};

  // COMPILE and execute
  codegen::QueryCompiler compiler;
  auto query_statement = compiler.Compile(*agg_plan, buffer);
  query_statement->Execute(catalog::Manager::GetInstance(),
                           reinterpret_cast<char*>(buffer.GetState()));

  // Check results
  const auto& results = buffer.GetOutputTuples();
  EXPECT_EQ(results.size(), 5);
}

TEST_F(GroupByTranslatorTest, AggregationWithInputPredciate) {
  //
  // SELECT a, avg(b) as x FROM table GROUP BY a WHERE a > 50;
  //

  // 1) Set up projection (just a direct map)
  planner::ProjectInfo::DirectMapList direct_map_list = {{0, {0, 0}},
                                                         {1, {1, 0}}};
  std::unique_ptr<planner::ProjectInfo> proj_info{new planner::ProjectInfo(
      planner::ProjectInfo::TargetList(), std::move(direct_map_list))};

  // 2) Setup the average over 'b'
  auto* tve_expr =
      new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 1);
  std::vector<planner::AggregatePlan::AggTerm> agg_terms = {
      {EXPRESSION_TYPE_AGGREGATE_AVG, tve_expr}};
  agg_terms[0].agg_ai.type = VALUE_TYPE_DOUBLE;

  // 3) The grouping column
  std::vector<oid_t> gb_cols = {0};

  // 4) The output schema
  std::shared_ptr<const catalog::Schema> output_schema{
      new catalog::Schema({{VALUE_TYPE_INTEGER, 4, "COL_A"},
                           {VALUE_TYPE_DOUBLE, 8, "AVG(COL_B)"}})};

  // 5) Finally, the aggregation node
  std::unique_ptr<planner::AbstractPlan> agg_plan{new planner::AggregatePlan(
      std::move(proj_info), nullptr, std::move(agg_terms), std::move(gb_cols),
      output_schema, AGGREGATE_TYPE_HASH)};

  // 6) The predicate on the grouping column
  auto* a_exp = new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 0);
  auto* const_50 = CodegenTestUtils::ConstIntExpression(50);
  auto* a_gt_50 = new expression::ComparisonExpression<expression::CmpGt>(
      EXPRESSION_TYPE_COMPARE_GREATERTHAN, a_exp, const_50);

  // 7) The scan that feeds the aggregation
  std::unique_ptr<planner::AbstractPlan> scan_plan{
      new planner::SeqScanPlan(&GetTestTable(), a_gt_50, {0, 1})};

  agg_plan->AddChild(std::move(scan_plan));

  // Do binding
  planner::BindingContext context;
  agg_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  BufferingConsumer buffer{{0, 1}, context};

  // COMPILE and execute
  codegen::QueryCompiler compiler;
  auto query_statement = compiler.Compile(*agg_plan, buffer);
  query_statement->Execute(catalog::Manager::GetInstance(),
                           reinterpret_cast<char*>(buffer.GetState()));

  // Check results. We expect four because the "A" col increases by 10 for each
  // row. For 10 rows, the four rows with A = 60, 70, 80, 90 are valid.
  const auto& results = buffer.GetOutputTuples();
  EXPECT_EQ(results.size(), 4);
}

TEST_F(GroupByTranslatorTest, SingleCountStar) {
  //
  // SELECT count(*) FROM table;
  //

  // 1) Set up projection (just a direct map)
  planner::ProjectInfo::DirectMapList direct_map_list = {{0, {1, 0}}};
  std::unique_ptr<planner::ProjectInfo> proj_info{new planner::ProjectInfo(
      planner::ProjectInfo::TargetList(), std::move(direct_map_list))};

  // 2) Setup the aggregations
  // For count(*) just use a TVE
  auto* tve_expr =
      new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 0);
  std::vector<planner::AggregatePlan::AggTerm> agg_terms = {
      {EXPRESSION_TYPE_AGGREGATE_COUNT_STAR, tve_expr}};
  agg_terms[0].agg_ai.type = VALUE_TYPE_BIGINT;

  // 3) No grouping
  std::vector<oid_t> gb_cols = {};

  // 4) The output schema
  std::shared_ptr<const catalog::Schema> output_schema{
      new catalog::Schema({{VALUE_TYPE_BIGINT, 8, "COUNT_A"}})};

  // 5) Finally, the aggregation node
  std::unique_ptr<planner::AbstractPlan> agg_plan{new planner::AggregatePlan(
      std::move(proj_info), nullptr, std::move(agg_terms), std::move(gb_cols),
      output_schema, AGGREGATE_TYPE_HASH)};

  // 6) The scan that feeds the aggregation
  std::unique_ptr<planner::AbstractPlan> scan_plan{
      new planner::SeqScanPlan(&GetTestTable(), nullptr, {0})};

  agg_plan->AddChild(std::move(scan_plan));

  // Do binding
  planner::BindingContext context;
  agg_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  BufferingConsumer buffer{{0}, context};

  // COMPILE and execute
  codegen::QueryCompiler compiler;
  auto query_statement = compiler.Compile(*agg_plan, buffer);
  query_statement->Execute(catalog::Manager::GetInstance(),
                           reinterpret_cast<char*>(buffer.GetState()));

  // Check results
  const auto& results = buffer.GetOutputTuples();
  EXPECT_EQ(results.size(), 1);
  EXPECT_TRUE(results[0]
                  .GetValue(0)
                  .OpEquals(ValueFactory::GetBigIntValue(10))
                  .IsTrue());
}

TEST_F(GroupByTranslatorTest, MinAndMax) {
  //
  // SELECT MAX(a), MIN(a) FROM table;
  //

  // 1) Set up projection (just a direct map)
  planner::ProjectInfo::DirectMapList direct_map_list = {{0, {1, 0}},
                                                         {1, {1, 1}}};
  std::unique_ptr<planner::ProjectInfo> proj_info{new planner::ProjectInfo(
      planner::ProjectInfo::TargetList(), std::move(direct_map_list))};

  // 2) Setup MAX() aggregation on column 'a' and MIN() on 'b'
  auto* a_col = new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 0);
  auto* b_col = new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 1);
  std::vector<planner::AggregatePlan::AggTerm> agg_terms = {
      {EXPRESSION_TYPE_AGGREGATE_MAX, a_col},
      {EXPRESSION_TYPE_AGGREGATE_MIN, b_col}};
  agg_terms[0].agg_ai.type = VALUE_TYPE_INTEGER;
  agg_terms[1].agg_ai.type = VALUE_TYPE_INTEGER;

  // 3) No grouping
  std::vector<oid_t> gb_cols = {};

  // 4) The output schema
  std::shared_ptr<const catalog::Schema> output_schema{new catalog::Schema(
      {{VALUE_TYPE_INTEGER, 4, "MAX_A"}, {VALUE_TYPE_INTEGER, 4, "MIN_B"}})};

  // 5) Finally, the aggregation node
  std::unique_ptr<planner::AbstractPlan> agg_plan{new planner::AggregatePlan(
      std::move(proj_info), nullptr, std::move(agg_terms), std::move(gb_cols),
      output_schema, AGGREGATE_TYPE_HASH)};

  // 6) The scan that feeds the aggregation
  std::unique_ptr<planner::AbstractPlan> scan_plan{
      new planner::SeqScanPlan(&GetTestTable(), nullptr, {0, 1})};

  agg_plan->AddChild(std::move(scan_plan));

  // Do binding
  planner::BindingContext context;
  agg_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  BufferingConsumer buffer{{0, 1}, context};

  // COMPILE and execute
  codegen::QueryCompiler compiler;
  auto query_statement = compiler.Compile(*agg_plan, buffer);
  query_statement->Execute(catalog::Manager::GetInstance(),
                           reinterpret_cast<char*>(buffer.GetState()));

  // There should only be a single output row
  const auto& results = buffer.GetOutputTuples();
  EXPECT_EQ(results.size(), 1);

  // The values of column 'a' are equal to the (zero-indexed) row ID * 10. The
  // maximum row ID is equal to # inserted - 1. Therefore:
  // MAX(a) = (# inserted - 1) * 10 = (10 - 1) * 10 = 9 * 10 = 90
  EXPECT_TRUE(results[0]
                  .GetValue(0)
                  .OpEquals(ValueFactory::GetBigIntValue(90))
                  .IsTrue());

  // The values of 'b' are equal to the (zero-indexed) row ID * 10 + 1. The
  // minimum row ID is 0. Therefore: MIN(b) = 0 * 10 + 1 = 1
  EXPECT_TRUE(results[0]
                  .GetValue(1)
                  .OpEquals(ValueFactory::GetBigIntValue(1))
                  .IsTrue());
}

}  // namespace test
}  // namespace peloton
