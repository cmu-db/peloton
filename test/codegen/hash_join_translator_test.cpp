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

#include "codegen/query_compiler.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "expression/comparison_expression.h"
#include "storage/table_factory.h"
#include "planner/hash_join_plan.h"
#include "planner/hash_plan.h"
#include "planner/seq_scan_plan.h"

#include "codegen/codegen_test_util.h"

namespace peloton {
namespace test {

//===----------------------------------------------------------------------===//
// This class contains code to test code generation and compilation of hash
// join query plans. All the tests use two test tables we create and load in the
// SetUp() method. The left and right tables have the following schema:
//
// +---------+---------+---------+-------------+
// | A (int) | B (int) | C (int) | D (varchar) |
// +---------+---------+---------+-------------+
//
// The database and tables are created in CreateDatabase() and
// CreateTestTables(), respectively.
//
// By default, the left table is loaded with 20 rows of random values, and
// the second table is loaded with 80 rows of random values.  The scale
// factor and size difference between the two tables can be controlled
// through the LoadTestTables() method that individual tests can invoke.
//===----------------------------------------------------------------------===//

typedef std::unique_ptr<const expression::AbstractExpression> AbstractExprPtr;

class HashJoinTranslatorTest : public PelotonCodeGenTest {
 public:
  HashJoinTranslatorTest() : PelotonCodeGenTest() {
    // Load the test table
    uint32_t num_rows = 10;
    LoadTestTable(LeftTableId(), 2 * num_rows);
    LoadTestTable(RightTableId(), 8 * num_rows);
  }

  uint32_t LeftTableId() const { return test_table1_id; }

  uint32_t RightTableId() const { return test_table2_id; }

  storage::DataTable& GetLeftTable() const {
    return GetTestTable(LeftTableId());
  }

  storage::DataTable& GetRightTable() const {
    return GetTestTable(RightTableId());
  }
};

TEST_F(HashJoinTranslatorTest, SingleHashJoinColumnTest) {
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
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0));

  std::vector<AbstractExprPtr> hash_keys;
  hash_keys.emplace_back(
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0));

  // Finally, the fucking join node
  std::unique_ptr<planner::HashJoinPlan> hj_plan{
      new planner::HashJoinPlan(JoinType::INNER, nullptr, std::move(projection),
                                schema, left_hash_keys, right_hash_keys)};
  std::unique_ptr<planner::HashPlan> hash_plan{
      new planner::HashPlan(hash_keys)};

  std::unique_ptr<planner::AbstractPlan> left_scan{
      new planner::SeqScanPlan(&GetLeftTable(), nullptr, {0, 1, 2})};
  std::unique_ptr<planner::AbstractPlan> right_scan{
      new planner::SeqScanPlan(&GetRightTable(), nullptr, {0, 1, 2})};

  hash_plan->AddChild(std::move(right_scan));
  hj_plan->AddChild(std::move(left_scan));
  hj_plan->AddChild(std::move(hash_plan));

  // Do binding
  planner::BindingContext context;
  hj_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1, 2, 3}, context};

  // COMPILE and run
  CompileAndExecute(*hj_plan, buffer,
                    reinterpret_cast<char*>(buffer.GetState()));

  // Check results
  const auto& results = buffer.GetOutputTuples();
  // The left table has 20 columns, the right has 80, all of them match
  EXPECT_EQ(20, results.size());
  // The output has the join columns (that should match) in positions 0 and 1
  for (const auto& tuple : results) {
    type::Value v0 = tuple.GetValue(0);
    EXPECT_EQ(v0.GetTypeId(), type::Type::TypeId::INTEGER);

    // Check that the joins keys are actually equal
    EXPECT_EQ(tuple.GetValue(0).CompareEquals(tuple.GetValue(1)),
              type::CMP_TRUE);
  }
}

}  // namespace test
}  // namespace peloton
