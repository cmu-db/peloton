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
#include "codegen/runtime_functions_proxy.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "expression/comparison_expression.h"
#include "storage/table_factory.h"
#include "planner/hash_join_plan.h"
#include "planner/hash_plan.h"
#include "planner/seq_scan_plan.h"

#include "codegen/codegen_test_util.h"
#include "executor/executor_tests_util.h"

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

class HashJoinTranslatorTest : public PelotonTest {
 public:
  typedef std::unique_ptr<const expression::AbstractExpression> AbstractExprPtr;

  HashJoinTranslatorTest() {
    CreateDatabase();
    CreateTestTables();
    LoadTestTables();
  }

  ~HashJoinTranslatorTest() override {
    catalog::Manager::GetInstance().DropDatabaseWithOid(database->GetOid());
  }

  storage::Database& GetDatabase() const { return *database; }

  storage::DataTable& GetLeftTable() const {
    return *GetDatabase().GetTableWithOid(0);
  }
  storage::DataTable& GetRightTable() const {
    return *GetDatabase().GetTableWithOid(1);
  }

 private:
  void CreateDatabase() {
    database = new storage::Database(0);
    catalog::Manager::GetInstance().AddDatabase(database);
  }

  void CreateTestTables() {
    const int tuples_per_tilegroup_count = 5;
    const bool adapt_table = false;
    const bool own_schema = true;

    auto* left_table_schema =
        new catalog::Schema({ExecutorTestsUtil::GetColumnInfo(0),
                             ExecutorTestsUtil::GetColumnInfo(1),
                             ExecutorTestsUtil::GetColumnInfo(2),
                             ExecutorTestsUtil::GetColumnInfo(3)});
    auto* left_table = storage::TableFactory::GetDataTable(
        GetDatabase().GetOid(), 0, left_table_schema, "left-table",
        tuples_per_tilegroup_count, own_schema, adapt_table);

    auto* right_table_schema =
        new catalog::Schema({ExecutorTestsUtil::GetColumnInfo(0),
                             ExecutorTestsUtil::GetColumnInfo(1),
                             ExecutorTestsUtil::GetColumnInfo(2),
                             ExecutorTestsUtil::GetColumnInfo(3)});
    auto* right_table = storage::TableFactory::GetDataTable(
        GetDatabase().GetOid(), 1, right_table_schema, "right-table",
        tuples_per_tilegroup_count, own_schema, adapt_table);

    GetDatabase().AddTable(left_table);
    GetDatabase().AddTable(right_table);
  }

  void LoadTestTables(uint32_t num_rows = 10) {
    auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    txn_manager.BeginTransaction();
    // Load left
    auto* left_table = GetDatabase().GetTableWithOid(0);
    ExecutorTestsUtil::PopulateTable(left_table, 2 * num_rows, false,
                                     false, false);

    // Load right
    auto* right_table = GetDatabase().GetTableWithOid(1);
    ExecutorTestsUtil::PopulateTable(right_table, 8 * num_rows, false,
                                     false, false);

    txn_manager.CommitTransaction();
  }

 private:
  storage::Database *database;
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

  // Construct join predicate: left_table.a = right_table.a
  auto* left_a = new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 0);
  auto* right_a =
      new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 0);
  AbstractExprPtr left_a_eq_right_a{
      new expression::ComparisonExpression<expression::CmpEq>(
          EXPRESSION_TYPE_COMPARE_EQUAL, left_a, right_a)};

  // Projection:  [left_table.a, right_table.a, left_table.b, right_table.c]
  planner::ProjectInfo::DirectMap dm1 = std::make_pair(0, std::make_pair(0, 0));
  planner::ProjectInfo::DirectMap dm2 = std::make_pair(1, std::make_pair(1, 0));
  planner::ProjectInfo::DirectMap dm3 = std::make_pair(2, std::make_pair(0, 1));
  planner::ProjectInfo::DirectMap dm4 = std::make_pair(3, std::make_pair(1, 2));
  planner::ProjectInfo::DirectMapList direct_map_list = {dm1, dm2, dm3, dm4};
  std::unique_ptr<planner::ProjectInfo> projection{new planner::ProjectInfo(
      planner::ProjectInfo::TargetList{}, std::move(direct_map_list))};

  // Output schema
  auto schema = std::shared_ptr<const catalog::Schema>(new catalog::Schema(
      {ExecutorTestsUtil::GetColumnInfo(0), ExecutorTestsUtil::GetColumnInfo(0),
       ExecutorTestsUtil::GetColumnInfo(1),
       ExecutorTestsUtil::GetColumnInfo(2)}));

  // Left and right hash keys
  std::vector<AbstractExprPtr> left_hash_keys;
  left_hash_keys.emplace_back(
      new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 0));

  std::vector<AbstractExprPtr> right_hash_keys;
  right_hash_keys.emplace_back(
      new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 1, 0));

  std::vector<AbstractExprPtr> hash_keys;
  hash_keys.emplace_back(
      new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 1, 0));

  // Finally, the fucking join node
  std::unique_ptr<planner::HashJoinPlan> hj_plan{new planner::HashJoinPlan(
      JOIN_TYPE_INNER, std::move(left_a_eq_right_a), std::move(projection),
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
  BufferingConsumer buffer{{0, 1, 2, 3}, context};

  // COMPILE
  codegen::QueryCompiler compiler;
  auto query_statement = compiler.Compile(*hj_plan, buffer);
  query_statement->Execute(*catalog::Catalog::GetInstance(),
                           reinterpret_cast<char*>(buffer.GetState()));

  // Check results
  const auto& results = buffer.GetOutputTuples();
  // The left table has 20 columns, the right has 80, all of them match
  EXPECT_EQ(results.size(), 20);
  // The output has the join columns (that should match) in positions 0 and 1
  for (const auto& tuple : results) {
    Value v0 = tuple.GetValue(0);
    EXPECT_EQ(v0.GetValueType(), VALUE_TYPE_INTEGER);
    //EXPECT_TRUE(tuple.GetValue(0).OpEquals(tuple.GetValue(1)).IsTrue());
  }
}

}  // namespace test
}  // namespace peloton
