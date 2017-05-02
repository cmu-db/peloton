//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// plan_comparator_test.cpp
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
#include "codegen/query_cache.h"

namespace peloton {
namespace test {

//===----------------------------------------------------------------------===//
// This class contains code to test the correctness of plan comparator and query
// cache by generating pairs of same/different plans of different kinds and testing
// the comparison result and whether the same plan can successfully find cache. 
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
  uint32_t NumRowsInTestTable() const { return num_rows_to_insert; }
  uint32_t TestTableId() { return test_table1_id; }
  uint32_t RightTableId() const { return test_table2_id; }

  std::shared_ptr<planner::SeqScanPlan> GetSeqScanPlan() {
    //
    // SELECT b FROM table where a >= 40;
    //
    auto* a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
    auto* const_40_exp = CodegenTestUtils::ConstIntExpression(40);
    auto* a_gt_40 = new expression::ComparisonExpression(
        ExpressionType::COMPARE_GREATERTHANOREQUALTO, a_col_exp, const_40_exp);
    return std::shared_ptr<planner::SeqScanPlan>(new planner::SeqScanPlan(&GetTestTable(TestTableId()), a_gt_40, {0, 1}));
  }

  std::shared_ptr<planner::SeqScanPlan> GetSeqScanPlanWithPlan() {
    //
    // SELECT a, b, c FROM table where a >= 20 and b = 21;
    //
    auto* a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
    auto* const_20_exp = CodegenTestUtils::ConstIntExpression(20);
    auto* a_gt_20 = new expression::ComparisonExpression(
        ExpressionType::COMPARE_GREATERTHANOREQUALTO, a_col_exp, const_20_exp);

    auto* b_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 1);
    auto* const_21_exp = CodegenTestUtils::ConstIntExpression(21);
    auto* b_eq_21 = new expression::ComparisonExpression(
        ExpressionType::COMPARE_EQUAL, b_col_exp, const_21_exp);

    auto* conj_eq = new expression::ConjunctionExpression(
      ExpressionType::CONJUNCTION_AND, b_eq_21, a_gt_20);

    return std::shared_ptr<planner::SeqScanPlan>(new planner::SeqScanPlan(&GetTestTable(TestTableId()), conj_eq, {0, 1, 2}));
  }

  std::shared_ptr<planner::HashJoinPlan> GetHashJoinPlan() {
    //
    // SELECT
    //   left_table.a, right_table.a, left_table.b, right_table.c,
    // FROM
    //   left_table
    // JOIN
    //   right_table ON left_table.a = right_table.a
    //
    DirectMap dm1 = std::make_pair(0, std::make_pair(0, 0));
    DirectMap dm2 = std::make_pair(1, std::make_pair(1, 0));
    DirectMap dm3 = std::make_pair(2, std::make_pair(0, 1));
    DirectMap dm4 = std::make_pair(3, std::make_pair(1, 2));
    DirectMapList direct_map_list = { dm1, dm2, dm3, dm4 };
    std::unique_ptr<planner::ProjectInfo> projection{
      new planner::ProjectInfo(TargetList{}, std::move(direct_map_list))
    };

    // Output schema
    auto schema = std::shared_ptr<const catalog::Schema>(
    new catalog::Schema({
      TestingExecutorUtil::GetColumnInfo(0),
        TestingExecutorUtil::GetColumnInfo(0),
        TestingExecutorUtil::GetColumnInfo(1),
        TestingExecutorUtil::GetColumnInfo(2)
    }));
    // Left and right hash keys
    std::vector<AbstractExprPtr> left_hash_keys;
    left_hash_keys.emplace_back(
    new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0));

    std::vector<AbstractExprPtr> right_hash_keys;
    right_hash_keys.emplace_back(
    new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0));

    std::vector<AbstractExprPtr> hash_keys;
    hash_keys.emplace_back(
    new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0));

    // Finally, the fucking join node
    std::shared_ptr<planner::HashJoinPlan> hj_plan{
      new planner::HashJoinPlan(JoinType::INNER, nullptr, std::move(projection),
                                schema, left_hash_keys, right_hash_keys)
    };
    std::unique_ptr<planner::HashPlan> hash_plan{
      new planner::HashPlan(hash_keys)
    };

    std::unique_ptr<planner::AbstractPlan> left_scan{
      new planner::SeqScanPlan(&GetTestTable(TestTableId()), nullptr, {0, 1, 2})
    };
    std::unique_ptr<planner::AbstractPlan> right_scan{
      new planner::SeqScanPlan(&GetTestTable(RightTableId()), nullptr, {0, 1, 2})
    };

    hash_plan->AddChild(std::move(right_scan));
    hj_plan->AddChild(std::move(left_scan));
    hj_plan->AddChild(std::move(hash_plan));
    
    return hj_plan;
  }

  std::shared_ptr<planner::AggregatePlan> GetAggregatePlan() {
    //
    // SELECT a, avg(b) as x FROM table GROUP BY a WHERE x > 50;
    //
    DirectMapList direct_map_list = {{0, {0, 0}}, {1, {1, 0}}};
    std::unique_ptr<planner::ProjectInfo> proj_info{
        new planner::ProjectInfo(TargetList(), std::move(direct_map_list))};

    // 2) Setup the average over 'b'
    auto* tve_expr =
        new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 1);
    std::vector<planner::AggregatePlan::AggTerm> agg_terms = {
        {ExpressionType::AGGREGATE_AVG, tve_expr}};
    agg_terms[0].agg_ai.type = type::Type::TypeId::DECIMAL;

    // 3) The grouping column
    std::vector<oid_t> gb_cols = {0};

    // 4) The output schema
    std::shared_ptr<const catalog::Schema> output_schema{
        new catalog::Schema({{type::Type::TypeId::INTEGER, 4, "COL_A"},
                             {type::Type::TypeId::DECIMAL, 8, "AVG(COL_B)"}})};

    // 5) The predicate on the average aggregate
    auto* x_exp =
        new expression::TupleValueExpression(type::Type::TypeId::DECIMAL, 1, 0);
    auto* const_50 = new expression::ConstantValueExpression(
        type::ValueFactory::GetDecimalValue(50.0));
    std::unique_ptr<expression::AbstractExpression> x_gt_50{
        new expression::ComparisonExpression(ExpressionType::COMPARE_GREATERTHAN,
                                             x_exp, const_50)};

    // 6) Finally, the aggregation node
    std::shared_ptr<planner::AggregatePlan> agg_plan{new planner::AggregatePlan(
        std::move(proj_info), std::move(x_gt_50), std::move(agg_terms),
        std::move(gb_cols), output_schema, AggregateType::HASH)};

    // 7) The scan that feeds the aggregation
    std::unique_ptr<planner::AbstractPlan> scan_plan{
        new planner::SeqScanPlan(&GetTestTable(TestTableId()), nullptr, {0, 1})};

    agg_plan->AddChild(std::move(scan_plan));
    return agg_plan;
  }
  
private:
  uint32_t num_rows_to_insert = 64;
};


TEST_F(PlanComparatorTest, SimpleCacheCheck) {
  
  std::shared_ptr<planner::SeqScanPlan> scan =GetSeqScanPlan();
  std::shared_ptr<planner::SeqScanPlan> scan2 = GetSeqScanPlan();
  // Do binding
  planner::BindingContext context;
  scan->PerformBinding(context);
  planner::BindingContext context2;
  scan2->PerformBinding(context2);

  int ret = codegen::PlanComparator::Compare(*scan, *scan2);
  EXPECT_EQ(ret, 0);
  // Printing consumer
  codegen::BufferingConsumer buffer{{0}, context};

  // COMPILE and execute
  CompileAndExecuteWithCache(scan, buffer, reinterpret_cast<char*>(buffer.GetState()));
  // Check that we got all the results
  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(NumRowsInTestTable() - 4, results.size());

  codegen::BufferingConsumer buffer2{{0}, context2};
  // codegen::QueryCache::Instance().FindPlan(std::move(scan2));
  CompileAndExecuteWithCache(scan2, buffer2, reinterpret_cast<char*>(buffer2.GetState()));
  const auto &results2 = buffer2.GetOutputTuples();
  EXPECT_EQ(NumRowsInTestTable() - 4, results2.size());
  codegen::QueryCache::Instance().GetSize();
}

TEST_F(PlanComparatorTest, SeqScanCacheCheck) {
 //
 // SELECT a, b, c FROM table where a >= 20 and b = 21;
 //
 std::shared_ptr<planner::SeqScanPlan> scan = GetSeqScanPlanWithPlan();
 std::shared_ptr<planner::SeqScanPlan> scan2 = GetSeqScanPlanWithPlan();
 // Do binding
 planner::BindingContext context;
 scan->PerformBinding(context);
 planner::BindingContext context2;
 scan2->PerformBinding(context2);

 int ret = codegen::PlanComparator::Compare(*scan, *scan2);
 EXPECT_EQ(ret, 0);

 codegen::BufferingConsumer buffer{{0, 1, 2}, context};

 // COMPILE and execute
 CompileAndExecuteWithCache(scan, buffer, reinterpret_cast<char*>(buffer.GetState()));

 // Check that we got all the results
 const auto &results = buffer.GetOutputTuples();
 ASSERT_EQ(1, results.size());
 EXPECT_EQ(type::CMP_TRUE, results[0].GetValue(0).CompareEquals(
                               type::ValueFactory::GetIntegerValue(20)));
 EXPECT_EQ(type::CMP_TRUE, results[0].GetValue(1).CompareEquals(
                               type::ValueFactory::GetIntegerValue(21)));

 codegen::BufferingConsumer buffer2{{0, 1, 2}, context2};
 CompileAndExecuteWithCache(scan2, buffer2, reinterpret_cast<char*>(buffer2.GetState()));

 const auto &results2 = buffer2.GetOutputTuples();
 ASSERT_EQ(1, results2.size());
 EXPECT_EQ(type::CMP_TRUE, results2[0].GetValue(0).CompareEquals(
                               type::ValueFactory::GetIntegerValue(20)));
 EXPECT_EQ(type::CMP_TRUE, results2[0].GetValue(1).CompareEquals(
                               type::ValueFactory::GetIntegerValue(21)));
 codegen::QueryCache::Instance().GetSize();
}

TEST_F(PlanComparatorTest, HashJoinPlanCacheCheck) {

 std::shared_ptr<planner::HashJoinPlan> hj_plan = GetHashJoinPlan();
 std::shared_ptr<planner::HashJoinPlan> hj_plan2 = GetHashJoinPlan();

 // Do binding
 planner::BindingContext context, context2;
 hj_plan->PerformBinding(context);
 hj_plan2->PerformBinding(context2);
 int ret = codegen::PlanComparator::Compare(*hj_plan2, *hj_plan);
 EXPECT_EQ(ret, 0);

 // We collect the results of the query into an in-memory buffer
 codegen::BufferingConsumer buffer{{0, 1, 2, 3}, context};

 // COMPILE and run
 CompileAndExecuteWithCache(hj_plan, buffer,
                 reinterpret_cast<char*>(buffer.GetState()));

// Check results
 const auto& results = buffer.GetOutputTuples();

 EXPECT_EQ(64, results.size());
  // The output has the join columns (that should match) in positions 0 and 1
 for (const auto& tuple : results) {
   type::Value v0 = tuple.GetValue(0);
   EXPECT_EQ(v0.GetTypeId(), type::Type::TypeId::INTEGER);

   // Check that the joins keys are actually equal
   EXPECT_EQ(tuple.GetValue(0).CompareEquals(tuple.GetValue(1)),
             type::CMP_TRUE);
 }

 codegen::BufferingConsumer buffer2{{0, 1, 2, 3}, context2};

 // COMPILE and run
 CompileAndExecuteWithCache(hj_plan2, buffer2,
                   reinterpret_cast<char*>(buffer2.GetState()));

 // Check results
 const auto& results2 = buffer2.GetOutputTuples();

 EXPECT_EQ(64, results2.size());
 for (const auto& tuple : results2) {
   type::Value v0 = tuple.GetValue(0);
   EXPECT_EQ(v0.GetTypeId(), type::Type::TypeId::INTEGER);

   // Check that the joins keys are actually equal
   EXPECT_EQ(tuple.GetValue(0).CompareEquals(tuple.GetValue(1)),
             type::CMP_TRUE);
 }
 codegen::QueryCache::Instance().GetSize();
}



TEST_F(PlanComparatorTest, OrderByCacheCheck) {
 //
 // SELECT * FROM test_table ORDER BY a DESC;
 //
 std::shared_ptr<planner::OrderByPlan> order_by_plan{
     new planner::OrderByPlan({1, 0}, {true, false}, {0, 1, 2, 3})};
 std::shared_ptr<planner::OrderByPlan> order_by_plan_2{
     new planner::OrderByPlan({1, 0}, {true, false}, {0, 1, 2, 3})};
 std::shared_ptr<planner::OrderByPlan> order_by_plan_3{
     new planner::OrderByPlan({1, 0}, {false, true}, {0, 1, 2, 3})};
 std::unique_ptr<planner::SeqScanPlan> seq_scan_plan{new planner::SeqScanPlan(
     &GetTestTable(TestTableId()), nullptr, {0, 1, 2, 3})};
 std::unique_ptr<planner::SeqScanPlan> seq_scan_plan_2{new planner::SeqScanPlan(
     &GetTestTable(TestTableId()), nullptr, {0, 1, 2, 3})};
 std::unique_ptr<planner::SeqScanPlan> seq_scan_plan_3{new planner::SeqScanPlan(
     &GetTestTable(TestTableId()), nullptr, {0, 1, 2, 3})};
 order_by_plan->AddChild(std::move(seq_scan_plan));
 order_by_plan_2->AddChild(std::move(seq_scan_plan_2));
 order_by_plan_3->AddChild(std::move(seq_scan_plan_3));

 planner::BindingContext context, context2, context3;
 order_by_plan->PerformBinding(context);
 order_by_plan_2->PerformBinding(context2);
 order_by_plan_3->PerformBinding(context3);

 int ret = codegen::PlanComparator::Compare(*order_by_plan.get(), *order_by_plan_2.get());
 EXPECT_EQ(ret, 0);

 ret = (codegen::PlanComparator::Compare(*order_by_plan.get(), *order_by_plan_3.get()) == 0);
 EXPECT_EQ(ret, 0);


 codegen::BufferingConsumer buffer{{0, 1}, context};
 codegen::BufferingConsumer buffer2{{0, 1}, context2};

 CompileAndExecuteWithCache(order_by_plan, buffer,
                   reinterpret_cast<char *>(buffer.GetState()));

 auto &results = buffer.GetOutputTuples();
 EXPECT_EQ(results.size(), NumRowsInTestTable());
 EXPECT_TRUE(std::is_sorted(
     results.begin(), results.end(),
     [](const codegen::WrappedTuple &t1, const codegen::WrappedTuple &t2) {
       auto is_gte = t1.GetValue(0).CompareGreaterThanEquals(t2.GetValue(0));
       return is_gte == type::CMP_TRUE;
     }));

 CompileAndExecuteWithCache(order_by_plan_2, buffer2,
                   reinterpret_cast<char *>(buffer2.GetState()));
 auto &results2 = buffer2.GetOutputTuples();
 EXPECT_EQ(results2.size(), NumRowsInTestTable());
 EXPECT_TRUE(std::is_sorted(
     results2.begin(), results2.end(),
     [](const codegen::WrappedTuple &t1, const codegen::WrappedTuple &t2) {
       auto is_gte = t1.GetValue(0).CompareGreaterThanEquals(t2.GetValue(0));
       return is_gte == type::CMP_TRUE;
     }));

 ret = (codegen::QueryCache::Instance().FindPlan(std::move(order_by_plan_3)) == nullptr);
 EXPECT_EQ(ret, 1);
 codegen::QueryCache::Instance().GetSize();
}


TEST_F(PlanComparatorTest, AggregatePlanCacheCheck) {

 std::shared_ptr<planner::AggregatePlan> agg_plan = GetAggregatePlan();
 std::shared_ptr<planner::AggregatePlan> agg_plan2 = GetAggregatePlan();
 planner::BindingContext context, context2;
 agg_plan->PerformBinding(context);
 agg_plan2->PerformBinding(context2);
 int ret = codegen::PlanComparator::Compare(*agg_plan, *agg_plan2);
 EXPECT_EQ(ret, 0);

 codegen::BufferingConsumer buffer{{0, 1}, context};
 codegen::BufferingConsumer buffer2{{0, 1}, context};

 // Compile it all
 CompileAndExecuteWithCache(agg_plan, buffer,
                   reinterpret_cast<char*>(buffer.GetState()));

 // Check results
 const auto& results = buffer.GetOutputTuples();
 EXPECT_EQ(results.size(), 59);

 CompileAndExecuteWithCache(agg_plan2, buffer2,
                   reinterpret_cast<char*>(buffer2.GetState()));

 const auto& results2 = buffer2.GetOutputTuples();
 EXPECT_EQ(results2.size(), 59);
 codegen::QueryCache::Instance().GetSize();
 codegen::QueryCache::Instance().CleanCache(0);
 codegen::QueryCache::Instance().GetSize();

}

}
}