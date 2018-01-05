//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_cache_test.cpp
//
// Identification: test/codegen/query_cache_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/testing_codegen_util.h"

#include "codegen/query_cache.h"
#include "codegen/testing_codegen_util.h"
#include "codegen/type/decimal_type.h"
#include "common/timer.h"
#include "catalog/catalog.h"
#include "expression/conjunction_expression.h"
#include "expression/operator_expression.h"
#include "planner/aggregate_plan.h"
#include "planner/hash_plan.h"
#include "planner/hash_join_plan.h"
#include "planner/nested_loop_join_plan.h"
#include "planner/order_by_plan.h"
#include "planner/seq_scan_plan.h"

namespace peloton {
namespace test {

class QueryCacheTest : public PelotonCodeGenTest {
 public:
  QueryCacheTest() : PelotonCodeGenTest(), num_rows_to_insert(64) {
    // Load test table
    LoadTestTable(TestTableId(), num_rows_to_insert);
    LoadTestTable(RightTableId(), 4 * num_rows_to_insert);
  }
  uint32_t NumRowsInTestTable() const { return num_rows_to_insert; }
  oid_t TestTableId() { return test_table_oids[0]; }
  oid_t RightTableId() { return test_table_oids[1]; }

  // SELECT b FROM table where a >= 40;
  std::shared_ptr<planner::SeqScanPlan> GetSeqScanPlan() {
    auto *a_col_exp =
        new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
    auto *const_40_exp = PelotonCodeGenTest::ConstIntExpr(40).release();
    auto *a_gt_40 = new expression::ComparisonExpression(
        ExpressionType::COMPARE_GREATERTHANOREQUALTO, a_col_exp, const_40_exp);
    return std::shared_ptr<planner::SeqScanPlan>(new planner::SeqScanPlan(
        &GetTestTable(TestTableId()), a_gt_40, {0, 1}));
  }

  // SELECT a, b, c FROM table where a >= 20 and b = 21;
  std::shared_ptr<planner::SeqScanPlan> GetSeqScanPlanWithPredicate() {
    auto *a_col_exp =
        new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
    auto *const_20_exp = PelotonCodeGenTest::ConstIntExpr(20).release();
    auto *a_gt_20 = new expression::ComparisonExpression(
        ExpressionType::COMPARE_GREATERTHANOREQUALTO, a_col_exp, const_20_exp);

    auto *b_col_exp =
        new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 1);
    auto *const_21_exp = PelotonCodeGenTest::ConstIntExpr(21).release();
    auto *b_eq_21 = new expression::ComparisonExpression(
        ExpressionType::COMPARE_EQUAL, b_col_exp, const_21_exp);

    auto *conj_eq = new expression::ConjunctionExpression(
        ExpressionType::CONJUNCTION_AND, b_eq_21, a_gt_20);

    return std::shared_ptr<planner::SeqScanPlan>(new planner::SeqScanPlan(
        &GetTestTable(TestTableId()), conj_eq, {0, 1, 2}));
  }

  // SELECT left_table.a, right_table.a, left_table.b, right_table.c,
  // FROM left_table
  // JOIN right_table ON left_table.a = right_table.a
  std::shared_ptr<planner::HashJoinPlan> GetHashJoinPlan() {
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
    std::vector<ConstExpressionPtr> left_hash_keys;
    left_hash_keys.emplace_back(
        new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0));

    std::vector<ConstExpressionPtr> right_hash_keys;
    right_hash_keys.emplace_back(
        new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0));

    std::vector<ConstExpressionPtr> hash_keys;
    hash_keys.emplace_back(
        new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0));

    // Finally, the fucking join node
    std::shared_ptr<planner::HashJoinPlan> hj_plan{new planner::HashJoinPlan(
        JoinType::INNER, nullptr, std::move(projection), schema, left_hash_keys,
        right_hash_keys)};
    std::unique_ptr<planner::HashPlan> hash_plan{
        new planner::HashPlan(hash_keys)};

    PlanPtr left_scan{new planner::SeqScanPlan(&GetTestTable(TestTableId()),
                                               nullptr, {0, 1, 2})};
    PlanPtr right_scan{new planner::SeqScanPlan(&GetTestTable(RightTableId()),
                                                nullptr, {0, 1, 2})};

    hash_plan->AddChild(std::move(right_scan));
    hj_plan->AddChild(std::move(left_scan));
    hj_plan->AddChild(std::move(hash_plan));

    return hj_plan;
  }

  // SELECT a, avg(b) as x FROM table GROUP BY a WHERE x > 50;
  std::shared_ptr<planner::AggregatePlan> GetAggregatePlan() {
    DirectMapList direct_map_list = {{0, {0, 0}}, {1, {1, 0}}};
    std::unique_ptr<planner::ProjectInfo> proj_info{
        new planner::ProjectInfo(TargetList(), std::move(direct_map_list))};

    // 2) Setup the average over 'b'
    auto *tve_expr =
        new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 1);
    std::vector<planner::AggregatePlan::AggTerm> agg_terms = {
        {ExpressionType::AGGREGATE_AVG, tve_expr}};
    agg_terms[0].agg_ai.type = codegen::type::Decimal::Instance();

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
    std::unique_ptr<expression::AbstractExpression> x_gt_50{
        new expression::ComparisonExpression(
            ExpressionType::COMPARE_GREATERTHAN, x_exp, const_50)};

    // 6) Finally, the aggregation node
    std::shared_ptr<planner::AggregatePlan> agg_plan{new planner::AggregatePlan(
        std::move(proj_info), std::move(x_gt_50), std::move(agg_terms),
        std::move(gb_cols), output_schema, AggregateType::HASH)};

    // 7) The scan that feeds the aggregation
    PlanPtr scan_plan{new planner::SeqScanPlan(&GetTestTable(TestTableId()),
                                               nullptr, {0, 1})};

    agg_plan->AddChild(std::move(scan_plan));
    return agg_plan;
  }

  std::shared_ptr<planner::NestedLoopJoinPlan> GetBlockNestedLoopJoinPlan() {
    // Output all columns
    DirectMapList direct_map_list = {{0, std::make_pair(0, 0)},
                                     {1, std::make_pair(0, 1)},
                                     {2, std::make_pair(0, 2)},
                                     {3, std::make_pair(1, 0)},
                                     {4, std::make_pair(1, 1)},
                                     {5, std::make_pair(1, 2)}};
    std::unique_ptr<planner::ProjectInfo> projection{
        new planner::ProjectInfo(TargetList{}, std::move(direct_map_list))};

    // Output schema
    auto schema = std::shared_ptr<const catalog::Schema>(new catalog::Schema(
        {GetTestColumn(0), GetTestColumn(1), GetTestColumn(2), GetTestColumn(0),
         GetTestColumn(1), GetTestColumn(2)}));

    bool left_side = true;
    auto left_a_col = ColRefExpr(type::TypeId::INTEGER, left_side, 0);
    auto right_a_col = ColRefExpr(type::TypeId::INTEGER, !left_side, 0);
    auto left_a_eq_right_a =
        CmpGtExpr(std::move(left_a_col), std::move(right_a_col));

    std::vector<oid_t> left_join_cols = {0};
    std::vector<oid_t> right_join_cols = {0};
    std::shared_ptr<planner::NestedLoopJoinPlan> nlj_plan{
        new planner::NestedLoopJoinPlan(
            JoinType::INNER, std::move(left_a_eq_right_a),
            std::move(projection), schema, left_join_cols, right_join_cols)};

    PlanPtr left_scan{new planner::SeqScanPlan(&GetTestTable(TestTableId()),
                                               nullptr, {0, 1, 2})};
    PlanPtr right_scan{new planner::SeqScanPlan(&GetTestTable(RightTableId()),
                                                nullptr, {0, 1, 2})};

    nlj_plan->AddChild(std::move(left_scan));
    nlj_plan->AddChild(std::move(right_scan));

    return nlj_plan;
  }

 private:
  uint32_t num_rows_to_insert = 64;
};

TEST_F(QueryCacheTest, SimpleCache) {
  // SELECT b FROM table where a >= 40;
  std::shared_ptr<planner::SeqScanPlan> scan1 = GetSeqScanPlan();
  std::shared_ptr<planner::SeqScanPlan> scan2 = GetSeqScanPlan();

  planner::BindingContext context_1;
  scan1->PerformBinding(context_1);
  planner::BindingContext context_2;
  scan2->PerformBinding(context_2);

  // Check the plan is the same
  auto hash_equal = (scan1->Hash() == scan2->Hash());
  EXPECT_TRUE(hash_equal);

  // Check the plan is the same
  auto is_equal = (*scan1.get() == *scan2.get());
  EXPECT_TRUE(is_equal);

  // Execute a new query
  codegen::BufferingConsumer buffer_1{{0}, context_1};
  bool cached;
  CompileAndExecuteCache(scan1, buffer_1, cached);
  const auto &results_1 = buffer_1.GetOutputTuples();
  EXPECT_EQ(NumRowsInTestTable() - 4, results_1.size());
  EXPECT_FALSE(cached);
  EXPECT_EQ(1, codegen::QueryCache::Instance().GetCount());

  // Execute a query cached
  codegen::BufferingConsumer buffer_2{{0}, context_2};
  CompileAndExecuteCache(scan2, buffer_2, cached);
  const auto &results_2 = buffer_2.GetOutputTuples();
  EXPECT_TRUE(cached);
  EXPECT_EQ(NumRowsInTestTable() - 4, results_2.size());

  EXPECT_EQ(1, codegen::QueryCache::Instance().GetCount());

  // PelotonCodeTest dies after each TEST_F()
  // So, we delete the cache
  codegen::QueryCache::Instance().Clear();
  EXPECT_EQ(0, codegen::QueryCache::Instance().GetCount());
}

TEST_F(QueryCacheTest, CacheSeqScanPlan) {
  // SELECT a, b, c FROM table where a >= 20 and b = 21;
  auto scan1 = GetSeqScanPlanWithPredicate();
  auto scan2 = GetSeqScanPlanWithPredicate();

  // Do binding
  planner::BindingContext context_1;
  scan1->PerformBinding(context_1);
  planner::BindingContext context_2;
  scan2->PerformBinding(context_2);

  auto hash_equal = (scan1->Hash() == scan2->Hash());
  EXPECT_TRUE(hash_equal);

  auto is_equal = (*scan1.get() == *scan2.get());
  EXPECT_TRUE(is_equal);

  codegen::BufferingConsumer buffer_1{{0, 1, 2}, context_1};

  bool cached;
  CompileAndExecuteCache(scan1, buffer_1, cached);

  // Check that we got all the results
  const auto &results_1 = buffer_1.GetOutputTuples();
  EXPECT_EQ(1, results_1.size());
  EXPECT_EQ(CmpBool::TRUE, results_1[0].GetValue(0).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(20)));
  EXPECT_EQ(CmpBool::TRUE, results_1[0].GetValue(1).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(21)));
  EXPECT_FALSE(cached);

  codegen::BufferingConsumer buffer_2{{0, 1, 2}, context_2};
  CompileAndExecuteCache(scan2, buffer_2, cached);

  const auto &results_2 = buffer_2.GetOutputTuples();
  EXPECT_EQ(1, results_2.size());
  EXPECT_EQ(CmpBool::TRUE, results_2[0].GetValue(0).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(20)));
  EXPECT_EQ(CmpBool::TRUE, results_2[0].GetValue(1).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(21)));
  EXPECT_TRUE(cached);
  EXPECT_EQ(1, codegen::QueryCache::Instance().GetCount());

  // PelotonCodeTest dies after each TEST_F()
  // So, we delete the cache
  codegen::QueryCache::Instance().Clear();
  EXPECT_EQ(0, codegen::QueryCache::Instance().GetCount());
}

TEST_F(QueryCacheTest, CacheHashJoinPlan) {
  auto hj_plan1 = GetHashJoinPlan();
  auto hj_plan_2 = GetHashJoinPlan();

  // Do binding
  planner::BindingContext context_1, context_2;
  hj_plan1->PerformBinding(context_1);
  hj_plan_2->PerformBinding(context_2);

  auto hash_equal = (hj_plan1->Hash() == hj_plan_2->Hash());
  EXPECT_TRUE(hash_equal);

  bool is_equal = (*hj_plan1.get() == *hj_plan_2.get());
  EXPECT_TRUE(is_equal);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer_1{{0, 1, 2, 3}, context_1};

  bool cached;
  CompileAndExecuteCache(hj_plan1, buffer_1, cached);

  // Check results
  const auto &results_1 = buffer_1.GetOutputTuples();
  EXPECT_EQ(64, results_1.size());

  // The output has the join columns (that should match) in positions 0 and 1
  for (const auto &tuple : results_1) {
    type::Value v0 = tuple.GetValue(0);
    EXPECT_EQ(v0.GetTypeId(), type::TypeId::INTEGER);

    // Check that the joins keys are actually equal
    EXPECT_EQ(tuple.GetValue(0).CompareEquals(tuple.GetValue(1)),
              CmpBool::TRUE);
  }

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer_2{{0, 1, 2, 3}, context_2};
  CompileAndExecuteCache(hj_plan_2, buffer_2, cached);
  EXPECT_TRUE(cached);
  // Check results
  const auto &results_2 = buffer_2.GetOutputTuples();
  EXPECT_EQ(64, results_2.size());
  for (const auto &tuple : results_2) {
    type::Value v0 = tuple.GetValue(0);
    EXPECT_EQ(v0.GetTypeId(), type::TypeId::INTEGER);

    // Check that the joins keys are actually equal
    EXPECT_EQ(tuple.GetValue(0).CompareEquals(tuple.GetValue(1)),
              CmpBool::TRUE);
  }
  EXPECT_EQ(1, codegen::QueryCache::Instance().GetCount());

  // PelotonCodeTest dies after each TEST_F()
  // So, we delete the cache
  codegen::QueryCache::Instance().Clear();
  EXPECT_EQ(0, codegen::QueryCache::Instance().GetCount());
}

TEST_F(QueryCacheTest, CacheOrderByPlan) {
  // plan 1, 2: SELECT * FROM test_table ORDER BY b DESC a ASC;
  // plan 3: SELECT * FROM test_table ORDER BY b ASC a DESC;
  std::shared_ptr<planner::OrderByPlan> order_by_plan_1{
      new planner::OrderByPlan({1, 0}, {true, false}, {0, 1, 2, 3})};
  std::shared_ptr<planner::OrderByPlan> order_by_plan_2{
      new planner::OrderByPlan({1, 0}, {true, false}, {0, 1, 2, 3})};
  std::shared_ptr<planner::OrderByPlan> order_by_plan_3{
      new planner::OrderByPlan({1, 0}, {false, true}, {0, 1, 2, 3})};

  std::unique_ptr<planner::SeqScanPlan> seq_scan_plan_1{
      new planner::SeqScanPlan(&GetTestTable(TestTableId()), nullptr,
                               {0, 1, 2, 3})};
  std::unique_ptr<planner::SeqScanPlan> seq_scan_plan_2{
      new planner::SeqScanPlan(&GetTestTable(TestTableId()), nullptr,
                               {0, 1, 2, 3})};
  std::unique_ptr<planner::SeqScanPlan> seq_scan_plan_3{
      new planner::SeqScanPlan(&GetTestTable(TestTableId()), nullptr,
                               {0, 1, 2, 3})};
  order_by_plan_1->AddChild(std::move(seq_scan_plan_1));
  order_by_plan_2->AddChild(std::move(seq_scan_plan_2));
  order_by_plan_3->AddChild(std::move(seq_scan_plan_3));

  planner::BindingContext context_1, context_2, context_3;
  order_by_plan_1->PerformBinding(context_1);
  order_by_plan_2->PerformBinding(context_2);
  order_by_plan_3->PerformBinding(context_3);

  auto hash_equal = (order_by_plan_1->Hash() == order_by_plan_2->Hash());
  EXPECT_TRUE(hash_equal);

  hash_equal = (order_by_plan_2->Hash() == order_by_plan_3->Hash());
  EXPECT_FALSE(hash_equal);

  auto is_equal = (*order_by_plan_1.get() == *order_by_plan_2.get());
  EXPECT_TRUE(is_equal);

  is_equal = (*order_by_plan_1.get() == *order_by_plan_3.get());
  EXPECT_FALSE(is_equal);

  codegen::BufferingConsumer buffer_1{{0, 1}, context_1};
  codegen::BufferingConsumer buffer_2{{0, 1}, context_2};

  bool cached;
  CompileAndExecuteCache(order_by_plan_1, buffer_1, cached);
  EXPECT_FALSE(cached);
  auto &results_1 = buffer_1.GetOutputTuples();
  EXPECT_EQ(results_1.size(), NumRowsInTestTable());
  EXPECT_TRUE(std::is_sorted(
      results_1.begin(), results_1.end(),
      [](const codegen::WrappedTuple &t1, const codegen::WrappedTuple &t2) {
        auto is_gte = t1.GetValue(0).CompareGreaterThanEquals(t2.GetValue(0));
        return is_gte == CmpBool::TRUE;
      }));

  CompileAndExecuteCache(order_by_plan_2, buffer_2, cached);
  EXPECT_TRUE(cached);
  auto &results_2 = buffer_2.GetOutputTuples();
  EXPECT_EQ(results_2.size(), NumRowsInTestTable());
  EXPECT_TRUE(std::is_sorted(
      results_2.begin(), results_2.end(),
      [](const codegen::WrappedTuple &t1, const codegen::WrappedTuple &t2) {
        auto is_gte = t1.GetValue(0).CompareGreaterThanEquals(t2.GetValue(0));
        return is_gte == CmpBool::TRUE;
      }));

  auto found = (codegen::QueryCache::Instance().Find(
                    std::move(order_by_plan_3)) == nullptr);
  EXPECT_EQ(found, 1);

  EXPECT_EQ(1, codegen::QueryCache::Instance().GetCount());

  // PelotonCodeTest dies after each TEST_F()
  // So, we delete the cache
  codegen::QueryCache::Instance().Clear();
  EXPECT_EQ(0, codegen::QueryCache::Instance().GetCount());
}

TEST_F(QueryCacheTest, CacheAggregatePlan) {
  auto agg_plan1 = GetAggregatePlan();
  auto agg_plan_2 = GetAggregatePlan();

  planner::BindingContext context_1, context_2;
  agg_plan1->PerformBinding(context_1);
  agg_plan_2->PerformBinding(context_2);

  auto hash_equal = (agg_plan1->Hash() == agg_plan_2->Hash());
  EXPECT_TRUE(hash_equal);

  auto is_equal = (*agg_plan1.get() == *agg_plan_2.get());
  EXPECT_TRUE(is_equal);
  EXPECT_EQ(0, codegen::QueryCache::Instance().GetCount());

  codegen::BufferingConsumer buffer_1{{0, 1}, context_1};
  codegen::BufferingConsumer buffer_2{{0, 1}, context_2};

  // Compile and execute
  bool cached;
  CompileAndExecuteCache(agg_plan1, buffer_1, cached);
  // Check results
  const auto &results_1 = buffer_1.GetOutputTuples();
  EXPECT_EQ(results_1.size(), 59);
  EXPECT_FALSE(cached);
  EXPECT_EQ(1, codegen::QueryCache::Instance().GetCount());

  // Compile and execute with the cached query
  CompileAndExecuteCache(agg_plan_2, buffer_2, cached);

  const auto &results_2 = buffer_2.GetOutputTuples();
  EXPECT_EQ(results_2.size(), 59);
  EXPECT_TRUE(cached);

  // Clean the query cache and leaves only one query
  EXPECT_EQ(1, codegen::QueryCache::Instance().GetCount());
  codegen::QueryCache::Instance().Clear();
  EXPECT_EQ(0, codegen::QueryCache::Instance().GetCount());

  // Check the correctness of LRU
  auto agg_plan_3 = GetAggregatePlan();
  planner::BindingContext context_3;
  agg_plan_3->PerformBinding(context_3);
  auto found = (codegen::QueryCache::Instance().Find(agg_plan_3) != nullptr);
  EXPECT_FALSE(found);
}

TEST_F(QueryCacheTest, CacheNestedLoopJoinPlan) {
  auto nlj_plan_1 = GetBlockNestedLoopJoinPlan();
  auto nlj_plan_2 = GetBlockNestedLoopJoinPlan();

  planner::BindingContext context_1, context_2;
  nlj_plan_1->PerformBinding(context_1);
  nlj_plan_2->PerformBinding(context_2);

  auto hash_equal = (nlj_plan_1->Hash() == nlj_plan_2->Hash());
  EXPECT_TRUE(hash_equal);

  auto is_equal = (*nlj_plan_1.get() == *nlj_plan_2.get());
  EXPECT_TRUE(is_equal);
  EXPECT_EQ(0, codegen::QueryCache::Instance().GetCount());

  codegen::BufferingConsumer buffer_1{{0, 1}, context_1};
  codegen::BufferingConsumer buffer_2{{0, 1}, context_2};

  // Compile and execute
  bool cached;
  CompileAndExecuteCache(nlj_plan_1, buffer_1, cached);
  EXPECT_FALSE(cached);
  EXPECT_EQ(1, codegen::QueryCache::Instance().GetCount());

  // Compile and execute with the cached query
  CompileAndExecuteCache(nlj_plan_2, buffer_2, cached);
  EXPECT_TRUE(cached);

  // Clean the query cache and leaves only one query
  EXPECT_EQ(1, codegen::QueryCache::Instance().GetCount());
  codegen::QueryCache::Instance().Clear();
  EXPECT_EQ(0, codegen::QueryCache::Instance().GetCount());

  // Check the correctness of LRU
  auto nlj_plan_3 = GetBlockNestedLoopJoinPlan();
  planner::BindingContext context_3;
  nlj_plan_3->PerformBinding(context_3);
  auto found = (codegen::QueryCache::Instance().Find(nlj_plan_3) != nullptr);
  EXPECT_FALSE(found);
}

TEST_F(QueryCacheTest, PerformanceBenchmark) {
  codegen::QueryCache::Instance().Clear();
  Timer<std::ratio<1, 1000>> timer1, timer2;

  timer1.Start();
  for (int i = 0; i < 10; i++) {
    auto plan = GetHashJoinPlan();
    // Do binding
    planner::BindingContext context;
    plan->PerformBinding(context);
    // We collect the results of the query into an in-memory buffer
    codegen::BufferingConsumer buffer{{0, 1, 2, 3}, context};
    // COMPILE and run without cache
    CompileAndExecute(*plan, buffer);
  }
  timer1.Stop();

  // Add one more for the caching effect
  bool cached;
  for (int i = 0; i < 11; i++) {
    if (i == 1) {
      timer2.Start();
    }
    auto plan = GetHashJoinPlan();
    // Do binding
    planner::BindingContext context;
    plan->PerformBinding(context);
    // We collect the results of the query into an in-memory buffer
    codegen::BufferingConsumer buffer{{0, 1, 2, 3}, context};
    // COMPILE and execute with cache
    CompileAndExecuteCache(plan, buffer, cached);
    if (i == 0) {
      EXPECT_FALSE(cached);
    } else {
      EXPECT_TRUE(cached);
    }
  }
  timer2.Stop();

  LOG_INFO("Time spent w/ codegen w/o cache is %f ms", timer1.GetDuration());
  LOG_INFO("Time spent w/ codegen & cache is %f ms", timer2.GetDuration());
}

}  // namespace test
}  // namespace peloton
