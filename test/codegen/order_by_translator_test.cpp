//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// order_by_translator_test.cpp
//
// Identification: test/codegen/order_by_translator_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/query_compiler.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "planner/order_by_plan.h"
#include "planner/seq_scan_plan.h"

#include "codegen/codegen_test_util.h"
#include "executor/testing_executor_util.h"

namespace peloton {
namespace test {

//===----------------------------------------------------------------------===//
// This class contains code to test code generation and compilation of order-by
// (sort) plans. All tests use a test table with the following schema:
//
// +---------+---------+---------+-------------+
// | A (int) | B (int) | C (int) | D (varchar) |
// +---------+---------+---------+-------------+
//
// Each test may choose to sort on a different column.
//===----------------------------------------------------------------------===//

class OrderByTranslatorTest : public PelotonTest {
 public:
  OrderByTranslatorTest()
      : test_db(new storage::Database(CodegenTestUtils::kTestDbOid)) {
    // Create test table
    auto *test_table = CreateTestTable();

    // Add table to test DB
    test_db->AddTable(test_table, false);

    // Add DB to catalog
    catalog::Catalog::GetInstance()->AddDatabase(test_db);
  }

  ~OrderByTranslatorTest() {
    catalog::Catalog::GetInstance()->DropDatabaseWithOid(
        CodegenTestUtils::kTestDbOid);
  }

  storage::DataTable *CreateTestTable() {
    const int tuples_per_tilegroup = 32;
    return TestingExecutorUtil::CreateTable(tuples_per_tilegroup, false,
                                            CodegenTestUtils::kTestTable1Oid);
  }

  void LoadTestTable(uint32_t num_rows) {
    auto &test_table = GetTestTable();

    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto *txn = txn_manager.BeginTransaction();

    TestingExecutorUtil::PopulateTable(&test_table, num_rows, false, false,
                                       false, txn);
    txn_manager.CommitTransaction(txn);
  }

  storage::DataTable &GetTestTable() {
    return *test_db->GetTableWithOid(CodegenTestUtils::kTestTable1Oid);
  }

 private:
  storage::Database *test_db;
};

TEST_F(OrderByTranslatorTest, SingleIntColAscTest) {
  //
  // SELECT * FROM test_table ORDER BY a;
  //

  // Load table with 20 rows
  uint32_t num_test_rows = 20;
  LoadTestTable(num_test_rows);

  std::unique_ptr<planner::OrderByPlan> order_by_plan{
      new planner::OrderByPlan({0}, {false}, {0, 1, 2, 3})};
  std::unique_ptr<planner::SeqScanPlan> seq_scan_plan{
      new planner::SeqScanPlan(&GetTestTable(), nullptr, {0, 1, 2, 3})};

  order_by_plan->AddChild(std::move(seq_scan_plan));

  // Do binding
  planner::BindingContext context;
  order_by_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  BufferingConsumer buffer{{0, 1}, context};

  // COMPILE and execute
  codegen::QueryCompiler compiler;
  auto query_statement = compiler.Compile(*order_by_plan, buffer);
  query_statement->Execute(*catalog::Catalog::GetInstance(),
                           reinterpret_cast<char *>(buffer.GetState()));

  // The results should be sorted in ascending order
  auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(results.size(), num_test_rows);
  EXPECT_TRUE(std::is_sorted(
      results.begin(), results.end(),
      [](const WrappedTuple &t1, const WrappedTuple &t2) {
        auto is_lte = t1.GetValue(0).CompareLessThanEquals(t2.GetValue(0));
        return is_lte == type::CMP_TRUE;
      }));
}

TEST_F(OrderByTranslatorTest, SingleIntColDescTest) {
  //
  // SELECT * FROM test_table ORDER BY a DESC;
  //

  // Load table with 20 rows
  uint32_t num_test_rows = 20;
  LoadTestTable(num_test_rows);

  std::unique_ptr<planner::OrderByPlan> order_by_plan{
      new planner::OrderByPlan({0}, {true}, {0, 1, 2, 3})};
  std::unique_ptr<planner::SeqScanPlan> seq_scan_plan{
      new planner::SeqScanPlan(&GetTestTable(), nullptr, {0, 1, 2, 3})};

  order_by_plan->AddChild(std::move(seq_scan_plan));

  // Do binding
  planner::BindingContext context;
  order_by_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  BufferingConsumer buffer{{0, 1}, context};

  // COMPILE and execute
  codegen::QueryCompiler compiler;
  auto query_statement = compiler.Compile(*order_by_plan, buffer);
  query_statement->Execute(*catalog::Catalog::GetInstance(),
                           reinterpret_cast<char *>(buffer.GetState()));

  // The results should be sorted in descending order
  auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(results.size(), num_test_rows);
  EXPECT_TRUE(std::is_sorted(
      results.begin(), results.end(),
      [](const WrappedTuple &t1, const WrappedTuple &t2) {
        auto is_gte = t1.GetValue(0).CompareGreaterThanEquals(t2.GetValue(0));
        return is_gte == type::CMP_TRUE;
      }));
}

TEST_F(OrderByTranslatorTest, MultiIntColAscTest) {
  //
  // SELECT * FROM test_table ORDER BY b, a ASC;
  //

  // Load table with 20 rows
  uint32_t num_test_rows = 20;
  LoadTestTable(num_test_rows);

  std::unique_ptr<planner::OrderByPlan> order_by_plan{
      new planner::OrderByPlan({1, 0}, {false, false}, {0, 1, 2, 3})};
  std::unique_ptr<planner::SeqScanPlan> seq_scan_plan{
      new planner::SeqScanPlan(&GetTestTable(), nullptr, {0, 1, 2, 3})};

  order_by_plan->AddChild(std::move(seq_scan_plan));

  // Do binding
  planner::BindingContext context;
  order_by_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  BufferingConsumer buffer{{0, 1}, context};

  // COMPILE and execute
  codegen::QueryCompiler compiler;
  auto query_statement = compiler.Compile(*order_by_plan, buffer);
  query_statement->Execute(*catalog::Catalog::GetInstance(),
                           reinterpret_cast<char *>(buffer.GetState()));

  // The results should be sorted in ascending order
  auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(results.size(), num_test_rows);

  EXPECT_TRUE(std::is_sorted(
      results.begin(), results.end(),
      [](const WrappedTuple &t1, const WrappedTuple &t2) {
        if (t1.GetValue(1).CompareEquals(t2.GetValue(0)) == type::CMP_TRUE) {
          // t1.b == t2.b => t1.a <= t2.a
          return t1.GetValue(0).CompareLessThanEquals(t2.GetValue(0)) ==
                 type::CMP_TRUE;
        } else {
          // t1.b != t2.b => t1.b < t2.b
          return t1.GetValue(1).CompareLessThan(t2.GetValue(1)) ==
                 type::CMP_TRUE;
        }
      }));
}

TEST_F(OrderByTranslatorTest, MultiIntColMixedTest) {
  //
  // SELECT * FROM test_table ORDER BY b DESC a ASC;
  //

  // Load table with 20 rows
  uint32_t num_test_rows = 20;
  LoadTestTable(num_test_rows);

  std::unique_ptr<planner::OrderByPlan> order_by_plan{
      new planner::OrderByPlan({1, 0}, {true, false}, {0, 1, 2, 3})};
  std::unique_ptr<planner::SeqScanPlan> seq_scan_plan{
      new planner::SeqScanPlan(&GetTestTable(), nullptr, {0, 1, 2, 3})};

  order_by_plan->AddChild(std::move(seq_scan_plan));

  // Do binding
  planner::BindingContext context;
  order_by_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  BufferingConsumer buffer{{0, 1}, context};

  // COMPILE and execute
  codegen::QueryCompiler compiler;
  auto query_statement = compiler.Compile(*order_by_plan, buffer);
  query_statement->Execute(*catalog::Catalog::GetInstance(),
                           reinterpret_cast<char *>(buffer.GetState()));

  // The results should be sorted in ascending order
  auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(results.size(), num_test_rows);

  EXPECT_TRUE(std::is_sorted(
      results.begin(), results.end(),
      [](const WrappedTuple &t1, const WrappedTuple &t2) {
        if (t1.GetValue(1).CompareEquals(t2.GetValue(1)) == type::CMP_TRUE) {
          // t1.b == t2.b => t1.a <= t2.a
          return t1.GetValue(0).CompareLessThanEquals(t2.GetValue(0)) ==
                 type::CMP_TRUE;
        } else {
          // t1.b != t2.b => t1.b > t2.b
          return t1.GetValue(1).CompareGreaterThan(t2.GetValue(1)) ==
                 type::CMP_TRUE;
        }
      }));
}

}  // namespace test
}  // namespace peloton