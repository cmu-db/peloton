//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// block_nested_loop_join_translator_test.cpp
//
// Identification: test/codegen/block_nested_loop_join_translator_test.cpp
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/query_compiler.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "planner/nested_loop_join_plan.h"
#include "planner/seq_scan_plan.h"

#include "codegen/testing_codegen_util.h"

namespace peloton {
namespace test {

class BlockNestedLoopJoinTranslatorTest : public PelotonCodeGenTest {
 public:
  BlockNestedLoopJoinTranslatorTest() : PelotonCodeGenTest() {
    // Load the test table
    uint32_t num_rows = 10;
    LoadTestTable(LeftTableId(), 2 * num_rows);
    LoadTestTable(RightTableId(), 8 * num_rows);
  }

  enum class JoinOutputColPos : oid_t {
    Table1_ColA = 0,
    Table1_ColB = 1,
    Table1_ColC = 2,
    Table2_ColA = 3,
    Table2_ColB = 4,
    Table2_ColC = 5,
  };

  oid_t LeftTableId() const { return test_table_oids[0]; }

  oid_t RightTableId() const { return test_table_oids[1]; }

  storage::DataTable &GetLeftTable() const {
    return GetTestTable(LeftTableId());
  }

  storage::DataTable &GetRightTable() const {
    return GetTestTable(RightTableId());
  }

  void PerformTest(ExpressionPtr &&predicate,
                   const std::vector<oid_t> &left_join_cols,
                   const std::vector<oid_t> &right_join_cols,
                   std::vector<codegen::WrappedTuple> &results);

  type::Value GetCol(const AbstractTuple &t, JoinOutputColPos p);
};

void BlockNestedLoopJoinTranslatorTest::PerformTest(
    ExpressionPtr &&predicate, const std::vector<oid_t> &left_join_cols,
    const std::vector<oid_t> &right_join_cols,
    std::vector<codegen::WrappedTuple> &results) {
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

  PlanPtr nlj_plan{new planner::NestedLoopJoinPlan(
      JoinType::INNER, std::move(predicate), std::move(projection), schema,
      left_join_cols, right_join_cols)};

  PlanPtr left_scan{
      new planner::SeqScanPlan(&GetLeftTable(), nullptr, {0, 1, 2})};
  PlanPtr right_scan{
      new planner::SeqScanPlan(&GetRightTable(), nullptr, {0, 1, 2})};

  nlj_plan->AddChild(std::move(left_scan));
  nlj_plan->AddChild(std::move(right_scan));

  // Do binding
  planner::BindingContext context;
  nlj_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1, 2, 3, 4, 5}, context};

  // COMPILE and run
  CompileAndExecute(*nlj_plan, buffer);

  // Return results
  results = buffer.GetOutputTuples();
}

type::Value BlockNestedLoopJoinTranslatorTest::GetCol(const AbstractTuple &t,
                                                      JoinOutputColPos p) {
  return t.GetValue(static_cast<oid_t>(p));
}

TEST_F(BlockNestedLoopJoinTranslatorTest, SingleColumnEqualityJoin) {
  {
    LOG_INFO(
        "Testing: "
        "SELECT A,B FROM table1 INNER JOIN table2 ON table1.A = table2.A");

    bool left_side = true;
    auto left_a_col = ColRefExpr(type::TypeId::INTEGER, left_side, 0);
    auto right_a_col = ColRefExpr(type::TypeId::INTEGER, !left_side, 0);
    auto left_a_eq_right_a =
        CmpEqExpr(std::move(left_a_col), std::move(right_a_col));

    std::vector<codegen::WrappedTuple> results;
    PerformTest(std::move(left_a_eq_right_a), {0}, {0}, results);

    // Check results
    EXPECT_EQ(20, results.size());
    for (const auto &t : results) {
      auto a1 = GetCol(t, JoinOutputColPos::Table1_ColA);
      auto a2 = GetCol(t, JoinOutputColPos::Table1_ColA);
      EXPECT_TRUE(a1.CompareEquals(a2) == CmpBool::TRUE);
    }
  }

  {
    LOG_INFO(
        "Testing: "
        "SELECT A,B FROM table1 INNER JOIN table2 ON table1.A = table2.B");
    bool left_side = true;
    auto left_a_col = ColRefExpr(type::TypeId::INTEGER, left_side, 0);
    auto right_b_col = ColRefExpr(type::TypeId::INTEGER, !left_side, 1);
    auto left_a_eq_right_b =
        CmpEqExpr(std::move(left_a_col), std::move(right_b_col));

    std::vector<codegen::WrappedTuple> results;
    PerformTest(std::move(left_a_eq_right_b), {0}, {1}, results);

    // Check results
    EXPECT_EQ(0, results.size());
  }

  {
    //
    // Join condition: table1.A == table2.B + 1
    //

    bool left_side = true;
    auto left_a_col = ColRefExpr(type::TypeId::INTEGER, left_side, 0);
    auto right_b_col = ColRefExpr(type::TypeId::INTEGER, !left_side, 1);
    auto b_col_minus_1 =
        OpExpr(ExpressionType::OPERATOR_MINUS, type::TypeId::INTEGER,
               std::move(right_b_col), ConstIntExpr(1));
    auto left_a_eq_right_b =
        CmpEqExpr(std::move(left_a_col), std::move(b_col_minus_1));

    std::vector<codegen::WrappedTuple> results;
    PerformTest(std::move(left_a_eq_right_b), {0}, {1}, results);

    // Check results
    EXPECT_EQ(20, results.size());
  }
}

TEST_F(BlockNestedLoopJoinTranslatorTest, NonEqualityJoin) {
  // The left and right input tables have the same schema and data distribution.
  // The test table has four columns: A, B, D, E. The values in column B, D, E
  // are 1, 2, and 3 more than the values in the A column, respectively. Values
  // in the A column increase by 10.
  {
    LOG_INFO(
        "Testing: "
        "SELECT A,B FROM table1 INNER JOIN table2 ON table1.A > table2.B");
    bool left_side = true;
    auto left_a_col = ColRefExpr(type::TypeId::INTEGER, left_side, 0);
    auto right_a_col = ColRefExpr(type::TypeId::INTEGER, !left_side, 0);
    auto left_a_eq_right_a =
        CmpGtExpr(std::move(left_a_col), std::move(right_a_col));

    std::vector<codegen::WrappedTuple> results;
    PerformTest(std::move(left_a_eq_right_a), {0}, {1}, results);

    // Check results
    //
    // The cross-product would have 20 x 80 = 1600 results total, but many are
    // removed by the join predicate. The first left tuple doesn't match with
    // any tuples from the right side because its A value is 0, less than all
    // B values from the right side. The second left tuple matches only one -
    // the first tuple from the right side whose B value is 1. The # of matches
    // is thus: 0, 1, 2, 3, ... , n where n is the number of tuples in the left
    // table. Then, the total number of matches is ((n-1)*n)/2. For 20 tuples,
    // there should be 190 matches.
    EXPECT_EQ(190, results.size());
    for (const auto &t : results) {
      auto a = GetCol(t, JoinOutputColPos::Table1_ColA);
      auto b = GetCol(t, JoinOutputColPos::Table2_ColB);
      EXPECT_TRUE(a.CompareGreaterThan(b) == CmpBool::TRUE);
    }
  }

  {
    LOG_INFO(
        "Testing: "
        "SELECT A,B FROM table1 INNER JOIN table2 ON table1.A <= table2.B");
    bool left_side = true;
    auto left_a_col = ColRefExpr(type::TypeId::INTEGER, left_side, 0);
    auto right_a_col = ColRefExpr(type::TypeId::INTEGER, !left_side, 0);
    auto left_a_eq_right_a =
        CmpLteExpr(std::move(left_a_col), std::move(right_a_col));

    std::vector<codegen::WrappedTuple> results;
    PerformTest(std::move(left_a_eq_right_a), {0}, {1}, results);

    // Check results
    // The number of matches follow logic similar to the previous test. The
    // first left tuple matches 80 tuples from the right side. The second row
    // matches 79, etc. The progression for matched rows is
    // s,(s-1),(s-2),...,(s-r), where s is the number of right tuples and
    // r is the number of left tuples. Then, the total number of matches is
    // (s*(s+1))/2 - ((s-r)*(s-r+1))/2). For r = 20 and s = 80, the number of
    // matches is
    EXPECT_EQ(1410, results.size());
    for (const auto &t : results) {
      auto a = GetCol(t, JoinOutputColPos::Table1_ColA);
      auto b = GetCol(t, JoinOutputColPos::Table2_ColB);
      EXPECT_TRUE(a.CompareLessThanEquals(b) == CmpBool::TRUE);
    }
  }

  {
    LOG_INFO(
        "Testing: "
        "SELECT A,B FROM table1 INNER JOIN table2 ON table1.A + table1.B > "
        "table2.A");
    bool left_side = true;
    auto left_a_col = ColRefExpr(type::TypeId::INTEGER, left_side, 0);
    auto left_b_col = ColRefExpr(type::TypeId::INTEGER, left_side, 1);
    auto left_a_pl_b =
        OpExpr(ExpressionType::OPERATOR_PLUS, type::TypeId::INTEGER,
               std::move(left_a_col), std::move(left_b_col));

    auto right_a_col = ColRefExpr(type::TypeId::INTEGER, !left_side, 0);
    auto left_a_eq_right_a =
        CmpGtExpr(std::move(left_a_pl_b), std::move(right_a_col));

    std::vector<codegen::WrappedTuple> results;
    PerformTest(std::move(left_a_eq_right_a), {0, 1}, {0}, results);

    // Check results
    for (const auto &t : results) {
      auto a1 = GetCol(t, JoinOutputColPos::Table1_ColA);
      auto b1 = GetCol(t, JoinOutputColPos::Table1_ColB);
      auto a1_pl_b1 = a1.Add(b1);

      auto a2 = GetCol(t, JoinOutputColPos::Table2_ColA);
      EXPECT_TRUE(a1_pl_b1.CompareGreaterThan(a2) == CmpBool::TRUE);
    }
  }
}

}  // namespace test
}  // namespace peloton
