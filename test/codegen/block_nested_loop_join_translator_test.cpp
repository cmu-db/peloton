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
};

void BlockNestedLoopJoinTranslatorTest::PerformTest(
    ExpressionPtr &&predicate, const std::vector<oid_t> &left_join_cols,
    const std::vector<oid_t> &right_join_cols,
    std::vector<codegen::WrappedTuple> &results) {
  DirectMap dm1 = std::make_pair(0, std::make_pair(0, 0));
  DirectMap dm2 = std::make_pair(1, std::make_pair(0, 1));
  DirectMap dm3 = std::make_pair(2, std::make_pair(1, 0));
  DirectMap dm4 = std::make_pair(3, std::make_pair(1, 1));
  DirectMapList direct_map_list = {dm1, dm2, dm3, dm4};
  std::unique_ptr<planner::ProjectInfo> projection{
      new planner::ProjectInfo(TargetList{}, std::move(direct_map_list))};

  // Output schema
  auto schema = std::shared_ptr<const catalog::Schema>(
      new catalog::Schema({GetTestColumn(0), GetTestColumn(1), GetTestColumn(0),
                           GetTestColumn(1)}));

  AbstractPlanPtr nlj_plan{new planner::NestedLoopJoinPlan(
      JoinType::INNER, std::move(predicate), std::move(projection), schema,
      left_join_cols, right_join_cols)};

  AbstractPlanPtr left_scan{
      new planner::SeqScanPlan(&GetLeftTable(), nullptr, {0, 1, 2})};
  AbstractPlanPtr right_scan{
      new planner::SeqScanPlan(&GetRightTable(), nullptr, {0, 1, 2})};

  nlj_plan->AddChild(std::move(left_scan));
  nlj_plan->AddChild(std::move(right_scan));

  // Do binding
  planner::BindingContext context;
  nlj_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1}, context};

  // COMPILE and run
  CompileAndExecute(*nlj_plan, buffer);

  // Return results
  results = buffer.GetOutputTuples();
}

TEST_F(BlockNestedLoopJoinTranslatorTest, SingleColumnEqualityJoin) {
  {
    //
    // Join condition: table1.A == table2.A
    //

    bool left_side = true;
    auto left_a_col = ColRefExpr(type::TypeId::INTEGER, left_side, 0);
    auto right_a_col = ColRefExpr(type::TypeId::INTEGER, !left_side, 0);
    auto left_a_eq_right_a =
        CmpEqExpr(std::move(left_a_col), std::move(right_a_col));

    std::vector<codegen::WrappedTuple> results;
    PerformTest(std::move(left_a_eq_right_a), {0}, {0}, results);

    // Check results
    EXPECT_EQ(20, results.size());
  }

  {
    //
    // Join condition: table1.A == table2.B
    //

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
    auto b_col_pl_1 =
        OpExpr(ExpressionType::OPERATOR_PLUS, type::TypeId::INTEGER,
               std::move(right_b_col), ConstIntExpr(1));
    auto left_a_eq_right_b =
        CmpEqExpr(std::move(left_a_col), std::move(b_col_pl_1));

    std::vector<codegen::WrappedTuple> results;
    PerformTest(std::move(left_a_eq_right_b), {0}, {1}, results);

    // Check results
    EXPECT_EQ(20, results.size());
  }
}

}  // namespace test
}  // namespace peloton
