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

#include "codegen/codegen_test_utils.h"
#include "executor/executor_tests_util.h"

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
  OrderByTranslatorTest() {
    CreateDatabase();
    CreateTestTable();
    // LoadTestTable();
  }

  ~OrderByTranslatorTest() override {
    catalog::Manager::GetInstance().DropDatabaseWithOid(database->GetOid());
  }

  void CreateDatabase() {
    database = new storage::Database(INVALID_OID);
    catalog::Manager::GetInstance().AddDatabase(database);
  }

  void CreateTestTable() {
    const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;
    auto *data_table = ExecutorTestsUtil::CreateTable(tuple_count, false);
    GetDatabase().AddTable(data_table);
  }

  void LoadTestTable(uint32_t num_rows = 10) {
    auto &data_table = GetTestTable();
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    txn_manager.BeginTransaction();
    ExecutorTestsUtil::PopulateTable(&data_table, num_rows, false, true,
                                     false);
    txn_manager.CommitTransaction();
  }

  storage::Database &GetDatabase() { return *database; }

  storage::DataTable &GetTestTable() {
    return *GetDatabase().GetTableWithOid(INVALID_OID);
  }

 private:
  storage::Database *database;
};

TEST_F(OrderByTranslatorTest, SortAscInt) {
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
  query_statement->Execute(catalog::Manager::GetInstance(),
                           reinterpret_cast<char *>(buffer.GetState()));

  // The results should be sorted in ascending order
  auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(results.size(), num_test_rows);
  EXPECT_TRUE(std::is_sorted(
      results.begin(), results.end(),
      [](const WrappedTuple &t1, const WrappedTuple &t2) {
        return t1.GetValue(0).OpLessThanOrEqual(t2.GetValue(0)).IsTrue();
      }));
}

TEST_F(OrderByTranslatorTest, SortDescInt) {
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
  query_statement->Execute(catalog::Manager::GetInstance(),
                           reinterpret_cast<char *>(buffer.GetState()));

  // The results should be sorted in ascending order
  auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(results.size(), num_test_rows);
  EXPECT_TRUE(std::is_sorted(
      results.begin(), results.end(),
      [](const WrappedTuple &t1, const WrappedTuple &t2) {
        return t1.GetValue(0).OpGreaterThanOrEqual(t2.GetValue(0)).IsTrue();
      }));
}

TEST_F(OrderByTranslatorTest, SortAllAscInt) {
  //
  // SELECT * FROM test_table ORDER BY a DESC;
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
  query_statement->Execute(catalog::Manager::GetInstance(),
                           reinterpret_cast<char *>(buffer.GetState()));

  // The results should be sorted in ascending order
  auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(results.size(), num_test_rows);

  for (const auto& t : results) {
    auto v0 = t.GetValue(0);
    auto v1 = t.GetValue(1);
    std::stringstream ss;
    ss << "[" << ValuePeeker::PeekInteger(v0);
    ss << " (" << ValueTypeToString(v0.GetValueType()) << "), ";
    ss << "[" << ValuePeeker::PeekInteger(v1);
    ss << " (" << ValueTypeToString(v1.GetValueType()) << ")]";
    std::cerr << ss.str() << std::endl;
  }

  EXPECT_TRUE(std::is_sorted(
      results.begin(), results.end(),
      [](const WrappedTuple &t1, const WrappedTuple &t2) {
        if (t1.GetValue(1).OpEquals(t2.GetValue(1)).IsTrue()) {
          return t1.GetValue(0).OpLessThanOrEqual(t2.GetValue(0)).IsTrue();
        } else {
          return t1.GetValue(1).OpLessThan(t2.GetValue(1)).IsTrue();
        }
      }));
}

TEST_F(OrderByTranslatorTest, SortAscIntDescInt) {
  //
  // SELECT * FROM test_table ORDER BY a DESC;
  //

  // Load table with 20 rows
  uint32_t num_test_rows = 20;
  LoadTestTable(num_test_rows);

  std::unique_ptr<planner::OrderByPlan> order_by_plan{
      new planner::OrderByPlan({1, 0}, {false, true}, {0, 1, 2, 3})};
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
  query_statement->Execute(catalog::Manager::GetInstance(),
                           reinterpret_cast<char *>(buffer.GetState()));

  // The results should be sorted in ascending order
  auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(results.size(), num_test_rows);

  EXPECT_TRUE(std::is_sorted(
      results.begin(), results.end(),
      [](const WrappedTuple &t1, const WrappedTuple &t2) {
        if (t1.GetValue(1).OpEquals(t2.GetValue(1)).IsTrue()) {
          return t1.GetValue(0).OpGreaterThanOrEqual(t2.GetValue(0)).IsTrue();
        } else {
          return t1.GetValue(1).OpLessThan(t2.GetValue(1)).IsTrue();
        }
      }));
}

}  // namespace test
}  // namespace peloton