//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// planner_equality_test.cpp
//
// Identification: test/planner/planner_equality_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/catalog.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "optimizer/optimizer.h"
#include "parser/postgresparser.h"
#include "sql/testing_sql_util.h"

namespace peloton {
namespace test {

class PlannerEqualityTest : public PelotonTest {
 protected:
  virtual void SetUp() override {
    // Call parent virtual function first
    PelotonTest::SetUp();

    // Create test database
    CreateAndLoadTable();
  }

  virtual void TearDown() override {
    // Destroy test database
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
    txn_manager.CommitTransaction(txn);

    // Call parent virtual function
    PelotonTest::TearDown();
  }

  void CreateAndLoadTable() {
    // Create database
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
    txn_manager.CommitTransaction(txn);

    // Create a table 'test'
    TestingSQLUtil::ExecuteSQLQuery("CREATE TABLE test(a INT, b INT, c INT);");

    // Create a table 'test2'
    TestingSQLUtil::ExecuteSQLQuery("CREATE TABLE test2(a INT, b INT, c INT);");

    // Create a table 'test3'
    TestingSQLUtil::ExecuteSQLQuery("CREATE TABLE test3(a INT, b INT, c INT);");
  }

  std::shared_ptr<planner::AbstractPlan> GeneratePlanWithOptimizer(
      std::unique_ptr<optimizer::AbstractOptimizer> &optimizer,
      const std::string query, concurrency::TransactionContext *txn) {
    auto &peloton_parser = parser::PostgresParser::GetInstance();

    auto parsed_stmt = peloton_parser.BuildParseTree(query);
    auto return_value =
        optimizer->BuildPelotonPlanTree(parsed_stmt, DEFAULT_DB_NAME, txn);
    return return_value;
  }

  class TestItem {
   public:
    std::string q1;
    std::string q2;
    bool hash_equal;
    bool is_equal;
  };

 protected:
  std::unique_ptr<optimizer::AbstractOptimizer> optimizer;
  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;
};

TEST_F(PlannerEqualityTest, Select) {
  // set up optimizer for every test
  optimizer.reset(new optimizer::Optimizer());

  // clang-format off
  std::vector<TestItem> items{
      {"SELECT * from test", "SELECT * from test", true, true},
      {"SELECT * from test", "SELECT * from test2", false, false},
      {"SELECT * from test", "SELECT * from test where a = 0", false, false},
      {"SELECT * from test where a = 1", "SELECT * from test where a = 0", true,
       true},
      {"SELECT * from test where b = 1", "SELECT * from test where b > 0",
       false, false},
      {"SELECT * from test where a = 1", "SELECT * from test where c = 0",
       false, false},
      {"SELECT a from test where b = 1", "SELECT c from test where b = 0",
       false, false},
      {"SELECT a,b from test where b = 1", "SELECT b,a from test where b = 0",
       false, false},
      {"SELECT a,b from test where b = 1", "SELECT a,b from test where b = $1",
       true, true},
      {"SELECT a,b from test where b = $1", "SELECT a,b from test where b = 9",
       true, true},
      {"SELECT * from test where b = 1 order by c",
       "SELECT * from test where b = 0 order by a", false, false},
      {"SELECT * from test where b = 1 order by c DESC",
       "SELECT * from test where b = 0 order by c ASC", false, false},
      {"SELECT * from test where b = 1 order by c+a",
       "SELECT * from test where b = 0 order by a+c", false, false},
      {"SELECT a,b from test where b = $1", "SELECT a,b from test where b = $1",
       true, true},
      {"SELECT a,b from test where b = $1",
       "SELECT a,b from test where b = null", false, false},
      {"SELECT a,b from test where b = $1", "SELECT a,b from test where b = 1",
       true, true},
      {"SELECT a,b from test where b = $1", "SELECT a,b from test where b = $1", true, true},
      {"SELECT a,b from test where b = $1",
       "SELECT a,b from test where b = null", false, false},
      {"SELECT a,b from test where b = $1", "SELECT a,b from test where b = 1", true, true},
      {"SELECT a,b from test where b = null",
       "SELECT a,b from test where b = 1", false, false},
      {"SELECT DISTINCT a from test where b = 1",
       "SELECT a from test where b = 0", false, false},
      {"SELECT 1 from test", "SELECT 1 from test", false, false},
      {"SELECT 1", "SELECT 2", false, false},
      {"SELECT * FROM test, test2 WHERE test.a = 1 AND test2.b = 0",
       "SELECT * FROM test, test2 WHERE test.a = 1 AND test2.b = 0", true, true},
      {"SELECT test.a, test.b, test3.b, test2.c FROM test2, test, test3 "
       "WHERE test.b = test2.b AND test2.c = test3.c",
       "SELECT test.a, test.b, test2.c, test3.b FROM test2, test, test3 "
       "WHERE test.b = test2.b AND test2.c = test3.c", false, false},
      {"SELECT * from test inner join test2 on test.a < test2.a",
       "SELECT * from test inner join test2 on test.a < test2.a", true, true},
      {"SELECT * from test inner join test2 on test.a < test2.a",
       "SELECT * from test inner join test2 on test.a > test2.a", false, false},
      {"SELECT * from test inner join test2 on test.a < 1 + test2.a",
       "SELECT * from test inner join test2 on test.a > 1 - test2.a", false, false},
  };
  // clang-format on
  for (uint32_t i = 0; i < items.size(); i++) {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();

    TestItem &item = items[i];
    LOG_DEBUG("Checking equality between (%s) and (%s)", item.q1.c_str(),
              item.q2.c_str());
    auto plan_1 = GeneratePlanWithOptimizer(optimizer, item.q1, txn);
    auto plan_2 = GeneratePlanWithOptimizer(optimizer, item.q2, txn);
    txn_manager.CommitTransaction(txn);

    planner::BindingContext context_1;
    plan_1->PerformBinding(context_1);

    planner::BindingContext context_2;
    plan_2->PerformBinding(context_2);

    auto hash_equal = (plan_1->Hash() == plan_2->Hash());
    EXPECT_EQ(item.hash_equal, hash_equal);

    auto is_equal = (*plan_1 == *plan_2);
    EXPECT_EQ(item.is_equal, is_equal);
  }
}

TEST_F(PlannerEqualityTest, Insert) {
  // set up optimizer for every test
  optimizer.reset(new optimizer::Optimizer());

  std::vector<TestItem> items{
      {"INSERT into test values(1,1)", "INSERT into test values(1,2)", true,
       true},
      {"INSERT into test values(1,1)", "INSERT into test2 values(1,2)", false,
       false},
      {"INSERT into test values(1,1)", "INSERT into test values(1,2),(3,4)",
       false, false},
      {"INSERT into test values(1,1),(4,5)",
       "INSERT into test values(1,2),(3,4)", true, true},
      {"INSERT into test values(1,1,2),(4,5)",
       "INSERT into test values(1,2),(3,4)", true, true},
      {"INSERT into test values(1,1,2),(4,5)",
       "INSERT into test select * from test2", false, false},
      {"INSERT into test select * from test2",
       "INSERT into test select * from test3", false, false},
      {"INSERT into test select * from test3",
       "INSERT into test select * from test3 where a=1", false, false},
  };

  for (uint32_t i = 0; i < items.size(); i++) {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();

    TestItem &item = items[i];
    auto plan_1 = GeneratePlanWithOptimizer(optimizer, item.q1, txn);
    auto plan_2 = GeneratePlanWithOptimizer(optimizer, item.q2, txn);
    txn_manager.CommitTransaction(txn);

    planner::BindingContext context_1;
    plan_1->PerformBinding(context_1);

    planner::BindingContext context_2;
    plan_2->PerformBinding(context_2);

    auto hash_equal = (plan_1->Hash() == plan_2->Hash());
    EXPECT_EQ(item.hash_equal, hash_equal);

    auto is_equal = (*plan_1 == *plan_2);
    EXPECT_EQ(item.is_equal, is_equal);
  }
}

TEST_F(PlannerEqualityTest, Delete) {
  optimizer.reset(new optimizer::Optimizer());
  std::vector<TestItem> items{
      {"DELETE from test where a=1", "DELETE from test where a=1", true, true},
      {"DELETE from test where a=1", "DELETE from test where a=2", true, true},
      {"DELETE from test where a=1", "DELETE from test2 where a=1", false,
       false},
      {"DELETE from test", "DELETE from test", true, true},
  };

  for (uint32_t i = 0; i < items.size(); i++) {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();

    TestItem &item = items[i];
    auto plan_1 = GeneratePlanWithOptimizer(optimizer, item.q1, txn);
    auto plan_2 = GeneratePlanWithOptimizer(optimizer, item.q2, txn);
    txn_manager.CommitTransaction(txn);

    planner::BindingContext context_1;
    plan_1->PerformBinding(context_1);

    planner::BindingContext context_2;
    plan_2->PerformBinding(context_2);

    auto hash_equal = (plan_1->Hash() == plan_2->Hash());
    EXPECT_EQ(item.hash_equal, hash_equal);

    auto is_equal = (*plan_1 == *plan_2);
    EXPECT_EQ(item.is_equal, is_equal);
  }
}

TEST_F(PlannerEqualityTest, Update) {
  // set up optimizer for every test
  optimizer.reset(new optimizer::Optimizer());

  std::vector<TestItem> items{
      {"UPDATE test SET c = b + 1 WHERE a=1",
       "UPDATE test SET c = b + 2 WHERE a=1", true, true},
      {"UPDATE test SET c = b + 1 WHERE a=1",
       "UPDATE test SET c = c + 2 WHERE a=1", false, false},
      {"UPDATE test SET c = b + 1 WHERE a=1",
       "UPDATE test2 SET c = b + 2 WHERE a=1", false, false},
      {"UPDATE test SET c = b + 1 WHERE a=1",
       "UPDATE test SET c = b + 2 WHERE a=$1", true, true},
  };

  for (uint32_t i = 0; i < items.size(); i++) {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();

    TestItem &item = items[i];
    auto plan_1 = GeneratePlanWithOptimizer(optimizer, item.q1, txn);
    auto plan_2 = GeneratePlanWithOptimizer(optimizer, item.q2, txn);
    txn_manager.CommitTransaction(txn);

    planner::BindingContext context_1;
    plan_1->PerformBinding(context_1);

    planner::BindingContext context_2;
    plan_2->PerformBinding(context_2);

    auto hash_equal = (plan_1->Hash() == plan_2->Hash());
    EXPECT_EQ(item.hash_equal, hash_equal);

    auto is_equal = (*plan_1 == *plan_2);
    EXPECT_EQ(item.is_equal, is_equal);
  }
}

}  // namespace test
}  // namespace peloton
