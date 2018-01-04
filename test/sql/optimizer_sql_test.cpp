//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// optimizer_sql_test.cpp
//
// Identification: test/sql/optimizer_sql_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "catalog/catalog.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/create_executor.h"
#include "optimizer/optimizer.h"
#include "planner/create_plan.h"
#include "planner/order_by_plan.h"
#include "sql/testing_sql_util.h"

using std::vector;
using std::unordered_set;
using std::string;
using std::unique_ptr;
using std::shared_ptr;

namespace peloton {
namespace test {

class OptimizerSQLTests : public PelotonTest {
 protected:
  virtual void SetUp() override {
    // Call parent virtual function first
    PelotonTest::SetUp();

    // Create test database
    CreateAndLoadTable();
    optimizer.reset(new optimizer::Optimizer());
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

  /*** Helper functions **/
  void CreateAndLoadTable() {
    // Create database
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
    txn_manager.CommitTransaction(txn);

    // Create a table first
    TestingSQLUtil::ExecuteSQLQuery(
        "CREATE TABLE test(a INT PRIMARY KEY, b INT, c INT);");

    // Insert tuples into table
    TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (1, 22, 333);");
    TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (2, 11, 000);");
    TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (3, 33, 444);");
    TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (4, 00, 555);");
  }

  // If the query has OrderBy, the result is deterministic. Specify ordered to
  // be true. Otherwise, specify ordered to be false
  void TestUtil(string query, vector<string> ref_result, bool ordered,
                vector<PlanNodeType> expected_plans = {}) {
    LOG_DEBUG("Running Query \"%s\"", query.c_str());

    // Check Plan Nodes are correct if provided
    if (expected_plans.size() > 0) {
      auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
      auto txn = txn_manager.BeginTransaction();
      auto plan =
          TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query, txn);
      txn_manager.CommitTransaction(txn);

      auto plan_ptr = plan.get();
      vector<PlanNodeType> actual_plans;
      while (true) {
        actual_plans.push_back(plan_ptr->GetPlanNodeType());
        if (plan_ptr->GetChildren().size() == 0) break;
        plan_ptr = plan_ptr->GetChildren()[0].get();
      }
      EXPECT_EQ(expected_plans, actual_plans);
    }
    LOG_INFO("Before Exec with Opt");
    // Check plan execution results are correct
    TestingSQLUtil::ExecuteSQLQueryWithOptimizer(optimizer, query, result,
                                                 tuple_descriptor, rows_changed,
                                                 error_message);
    LOG_INFO("After Exec with Opt");
    vector<string> actual_result;
    for (unsigned i = 0; i < result.size(); i++)
      actual_result.push_back(
          TestingSQLUtil::GetResultValueAsString(result, i));

    EXPECT_EQ(ref_result.size(), result.size());
    if (ordered) {
      // If deterministic, do comparision with expected result in order
      EXPECT_EQ(ref_result, actual_result);
    } else {
      // If non-deterministic, make sure they have the same set of value
      unordered_set<string> ref_set(ref_result.begin(), ref_result.end());
      for (auto &result_str : actual_result) {
        if (ref_set.count(result_str) == 0) {
          // Test Failed. Print both actual results and ref results
          EXPECT_EQ(ref_result, actual_result);
          break;
        }
      }
    }
  }

 protected:
  unique_ptr<optimizer::AbstractOptimizer> optimizer;
  vector<ResultValue> result;
  vector<FieldInfo> tuple_descriptor;
  string error_message;
  int rows_changed;
};

TEST_F(OptimizerSQLTests, SimpleSelectTest) {
  // Testing select star expression
  TestUtil("SELECT * from test", {"333", "22", "1", "2", "11", "0", "3", "33",
                                  "444", "4", "0", "555"},
           false);

  // Something wrong with column property.
  string query = "SELECT b from test order by c";

  // check for plan node type
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  auto select_plan =
      TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query, txn);
  txn_manager.CommitTransaction(txn);

  //  EXPECT_EQ(select_plan->GetPlanNodeType(), PlanNodeType::PROJECTION);
  //  EXPECT_EQ(select_plan->GetChildren()[0]->GetPlanNodeType(),
  //            PlanNodeType::ORDERBY);

  // test order by
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);
  // Check the return value
  // Should be: 11, 22
  EXPECT_EQ("11", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("22", TestingSQLUtil::GetResultValueAsString(result, 1));

  // Something wrong with column property.
  query = "SELECT a from test order by c desc";

  // check for plan node type
  txn = txn_manager.BeginTransaction();
  select_plan =
      TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query, txn);
  txn_manager.CommitTransaction(txn);
  //  EXPECT_EQ(select_plan->GetPlanNodeType(), PlanNodeType::PROJECTION);
  //  EXPECT_EQ(select_plan->GetChildren()[0]->GetPlanNodeType(),
  //            PlanNodeType::ORDERBY);

  // test order by
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);
  // Check the return value
  EXPECT_EQ("4", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, 1));

  // Testing predicate
  TestUtil("SELECT c, b from test where a=1", {"333", "22"}, false);

  // Something wrong with column property.
  query = "SELECT a, b, c from test order by a + c";

  // check for plan node type
  txn = txn_manager.BeginTransaction();
  select_plan =
      TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query, txn);
  txn_manager.CommitTransaction(txn);
  //  EXPECT_EQ(select_plan->GetPlanNodeType(), PlanNodeType::ORDERBY);
  //  EXPECT_EQ(select_plan->GetChildren()[0]->GetPlanNodeType(),
  //            PlanNodeType::SEQSCAN);

  // test order by
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);
  // Check the return value
  //  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (1, 22, 333);");
  //  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (2, 11, 000);");
  //  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (3, 33, 444);");
  //  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (4, 00, 555);");
  EXPECT_EQ(12, result.size());
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("11", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("0", TestingSQLUtil::GetResultValueAsString(result, 2));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, 3));
  EXPECT_EQ("22", TestingSQLUtil::GetResultValueAsString(result, 4));
  EXPECT_EQ("333", TestingSQLUtil::GetResultValueAsString(result, 5));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, 6));
  EXPECT_EQ("33", TestingSQLUtil::GetResultValueAsString(result, 7));
  EXPECT_EQ("444", TestingSQLUtil::GetResultValueAsString(result, 8));
  EXPECT_EQ("4", TestingSQLUtil::GetResultValueAsString(result, 9));
  EXPECT_EQ("0", TestingSQLUtil::GetResultValueAsString(result, 10));
  EXPECT_EQ("555", TestingSQLUtil::GetResultValueAsString(result, 11));
}

TEST_F(OptimizerSQLTests, SelectOrderByTest) {
  // Testing order by columns different from select columns
  TestUtil("SELECT b from test order by c", {"11", "22", "33", "0"}, true);

  // Testing order by desc
  TestUtil("SELECT a from test order by c desc", {"4", "3", "1", "2"}, true);

  // Testing order by complex expression
  TestUtil(
      "SELECT * from test order by a + c",
      {"2", "11", "0", "1", "22", "333", "3", "33", "444", "4", "0", "555"},
      true);

  // Testing order by * expression
  TestUtil("SELECT * from test order by a", {"1", "22", "333", "2", "11", "0",
                                             "3", "33", "444", "4", "0", "555"},
           true);
}

TEST_F(OptimizerSQLTests, SelectLimitTest) {
  // Test limit with default offset
  TestUtil("SELECT b FROM test ORDER BY b LIMIT 3", {"0", "11", "22"}, true);

  // Test limit with offset
  TestUtil("SELECT b FROM test ORDER BY b LIMIT 2 OFFSET 2", {"22", "33"},
           true);
}

TEST_F(OptimizerSQLTests, SelectProjectionTest) {
  // Test complex expression projection
  TestUtil("SELECT a * 5 + b, -1 + c from test",
           {"27", "332", "48", "443", "21", "-1", "20", "554"}, false);

  // Test complex expression in select and order by
  TestUtil("SELECT a * 5 + b - c FROM test ORDER BY a * 10 + b",
           {"21", "-306", "-535", "-396"}, true);

  // Test mixing up select simple columns with complex expression
  TestUtil("SELECT a, a + c FROM test ORDER BY a * 3 * b DESC, b + c / 5 ASC",
           {"3", "447", "2", "2", "1", "334", "4", "559"}, true);
}

TEST_F(OptimizerSQLTests, DeleteSqlTest) {
  // TODO: Test for index scan

  // Delete with predicates
  string query = "DELETE FROM test WHERE a = 1 and c = 333";
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);
  EXPECT_EQ(1, rows_changed);
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(optimizer, "SELECT * FROM test",
                                               result, tuple_descriptor,
                                               rows_changed, error_message);
  EXPECT_EQ(9, result.size());

  // Delete with predicates
  query = "DELETE FROM test WHERE b = 33";
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);
  EXPECT_EQ(1, rows_changed);
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(optimizer, "SELECT * FROM test",
                                               result, tuple_descriptor,
                                               rows_changed, error_message);
  EXPECT_EQ(6, result.size());

  // Delete with false predicates
  query = "DELETE FROM test WHERE b = 123";
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);
  EXPECT_EQ(0, rows_changed);
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(optimizer, "SELECT * FROM test",
                                               result, tuple_descriptor,
                                               rows_changed, error_message);
  EXPECT_EQ(6, result.size());

  // Full deletion
  query = "DELETE FROM test";
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);
  EXPECT_EQ(2, rows_changed);
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(optimizer, "SELECT * FROM test",
                                               result, tuple_descriptor,
                                               rows_changed, error_message);
  EXPECT_EQ(0, result.size());
}

TEST_F(OptimizerSQLTests, UpdateSqlTest) {
  // Test Update with complex expression and predicate
  string query = "UPDATE test SET c = b + 1 WHERE a = 1";
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);
  EXPECT_EQ(1, rows_changed);
  TestUtil("SELECT c FROM test WHERE a=1", {"23"}, false);
}

TEST_F(OptimizerSQLTests, InsertSqlTest) {
  string query = "INSERT INTO test VALUES (5, 55, 555);";
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);
  EXPECT_EQ(1, rows_changed);

  // Test the tuple is succesfully inserted
  TestUtil("SELECT * FROM test WHERE a=5", {"5", "55", "555"}, false);
}

TEST_F(OptimizerSQLTests, DDLSqlTest) {
  // Test creating new table
  string query = "CREATE TABLE test2(a INT PRIMARY KEY, b INT, c INT);";
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  // using transaction to get table from catalog
  auto table = catalog::Catalog::GetInstance()->GetTableWithName(
      DEFAULT_DB_NAME, "test2", txn);
  EXPECT_NE(nullptr, table);
  auto cols = table->GetSchema()->GetColumns();
  EXPECT_EQ(3, cols.size());
  EXPECT_EQ("a", cols[0].GetName());
  EXPECT_EQ(true, cols[0].IsPrimary());
  EXPECT_EQ(type::TypeId::INTEGER, cols[0].GetType());
  EXPECT_EQ("b", cols[1].GetName());
  EXPECT_EQ(type::TypeId::INTEGER, cols[1].GetType());
  EXPECT_EQ("c", cols[2].GetName());
  EXPECT_EQ(type::TypeId::INTEGER, cols[2].GetType());
  txn_manager.CommitTransaction(txn);

  // Test dropping existing table
  query = "DROP TABLE test2";
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);

  LOG_DEBUG("here");
  
  txn = txn_manager.BeginTransaction();
  EXPECT_THROW(catalog::Catalog::GetInstance()->GetTableWithName(
                   DEFAULT_DB_NAME, "test2", txn),
               peloton::Exception);
  txn_manager.CommitTransaction(txn);
}

TEST_F(OptimizerSQLTests, GroupByTest) {
  // Insert additional tuples to test group by
  //  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (1, 22, 333);");
  //  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (2, 11, 000);");
  //  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (3, 33, 444);");
  //  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (4, 00, 555);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (5, 11, 000);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (6, 22, 333);");

  // Test basic case
  TestUtil("SELECT b FROM test GROUP BY b having b=11 or b=22", {"22", "11"},
           false);

  // Test Aggregate function: COUNT(*)
  TestUtil("SELECT COUNT(*) FROM test ", {"6"}, false);
  // Test Aggregate function: COUNT(*)
  TestUtil("SELECT COUNT(*) FROM test GROUP BY b", {"1", "1", "2", "2"}, false);

  // Test Aggregate function: COUNT(a)
  TestUtil("SELECT COUNT(a) FROM test GROUP BY b", {"1", "1", "2", "2"}, false);

  // Test basic case
  TestUtil("SELECT b FROM test GROUP BY b having b=11 or b=22", {"22", "11"},
           false);

  // Test Aggregate function: COUNT(*)
  TestUtil("SELECT COUNT(*) FROM test GROUP BY b", {"1", "1", "2", "2"}, false);

  // Test Aggregate function: COUNT(a)
  TestUtil("SELECT COUNT(a) FROM test GROUP BY b", {"1", "1", "2", "2"}, false);

  // Test group by with having
  TestUtil("SELECT AVG(a), b FROM test GROUP BY b having b=22", {"3.5", "22"},
           false);

  // Test group by combined with ORDER BY
  TestUtil("SELECT b FROM test GROUP BY b ORDER BY b", {"0", "11", "22", "33"},
           true);

  // Test complex expression in aggregation
  TestUtil("SELECT b, MAX(a + c) FROM test GROUP BY b ORDER BY b",
           {"0", "559", "11", "5", "22", "339", "33", "447"}, true);

  // Test complex expression in select list and order by complex expr
  TestUtil("SELECT b + c, SUM(c * a) FROM test GROUP BY b,c ORDER BY b + c",
           {"11", "0", "355", "2331", "477", "1332", "555", "2220"}, true);

  // Test Plain aggregation without group by
  TestUtil("SELECT SUM(c * a) FROM test", {"5883"}, false);

  // Test combining aggregation function
  TestUtil("SELECT SUM(c * a) + MAX(b - 1) * 2 FROM test", {"5947"}, false);

  // Test combining aggregation function with GroupBy
  TestUtil("SELECT MIN(b + c) * SUM(a - 2) FROM test GROUP BY b,c",
           {"1110", "477", "33", "1065"}, false);
  TestUtil("SELECT MIN(c) + b FROM test GROUP BY b",
           {"355", "11", "477", "555"}, false);
  TestUtil("SELECT MIN(b + c) * SUM(a - 2) + b * c FROM test GROUP BY b,c",
           {"1110", "15129", "33", "8391"}, false);

  // Test ORDER BY columns not shown in select list
  TestUtil("SELECT a FROM test GROUP BY a,b ORDER BY a + b",
           {"4", "2", "5", "1", "6", "3"}, true);

  // Test ORDER BY columns contains all group by columns
  // In case of SortGroupBy, no additional sort should be enforced after groupby
  TestUtil("SELECT a FROM test GROUP BY a,b ORDER BY b,a, a+b",
           {"4", "2", "5", "1", "6", "3"}, true);

  // Test ORDER BY columns are a subset of group by columns
  // In case of SortGroupBy, no additional sort should be enforced after groupby
  TestUtil("SELECT a + b FROM test GROUP BY a,b ORDER BY a",
           {"23", "13", "36", "4", "16", "28"}, true);
}

TEST_F(OptimizerSQLTests, SelectDistinctTest) {
  //  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (1, 22, 333);");
  //  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (2, 11, 000);");
  //  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (3, 33, 444);");
  //  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (4, 00, 555);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (5, 00, 555);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (6, 22, 333);");

  // Test DISTINCT and GROUP BY have the same columns. Avoid additional HashPlan
  TestUtil("SELECT DISTINCT b,c FROM test GROUP BY b,c",
           {"0", "555", "33", "444", "11", "0", "22", "333"}, false);

  // Test GROUP BY cannot satisfied DISTINCT
  TestUtil("SELECT DISTINCT b FROM test GROUP BY b,c", {"22", "11", "0", "33"},
           false);

  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (7, 00, 444);");

  // Test distinct with order by
  TestUtil("SELECT DISTINCT b FROM test ORDER BY b", {"0", "11", "22", "33"},
           true);

  // Test distinct with complex order by
  TestUtil("SELECT DISTINCT b, c FROM test ORDER BY 10 * b + c",
           {"11", "0", "0", "444", "22", "333", "0", "555", "33", "444"}, true);

  // Test distinct with limit and star expression
  TestUtil("SELECT DISTINCT * FROM test ORDER BY a + 10 * b + c LIMIT 3",
           {"2", "11", "0", "7", "0", "444", "1", "22", "333"}, true);

  // Insert additional tuples to test distinct with group by
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (5, 11, 000);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (6, 22, 333);");

  // Test distinct and group by complex expression
  //  TestUtil("SELECT DISTINCT b + c FROM test GROUP BY b + c ORDER BY b + c",
  //           {"11", "355", "444", "477", "555"}, true);
}

TEST_F(OptimizerSQLTests, SelectConstantTest) {
  // Test single constant
  TestUtil("SELECT 1", {"1"}, true);

  // Test complex arithmetic
  TestUtil("SELECT 1 + 2 * (6 / 4)", {"3"}, true);

  // Test multiple constant
  TestUtil("SELECT 18 / 4, 2 / 3 * 8 - 1", {"4", "-1"}, true);
  TestUtil("SELECT 18 % 4, 2 / 3 * 8 - 1", {"2", "-1"}, true);
  TestUtil("SELECT not 1>3, 1!=1, not 1=1", {"true", "false", "false"}, true);

  // Test combination of constant and column
  TestUtil("SELECT 1, 3 * 7, a from test",
           {"1", "21", "1", "1", "21", "2", "1", "21", "3", "1", "21", "4"},
           true);
}

TEST_F(OptimizerSQLTests, JoinTest) {
  // TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (1, 22, 333);");
  // TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (2, 11, 000);");
  // TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (3, 33, 444);");
  // TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (4, 00, 555);");

  // Create another table for join
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test1(a INT PRIMARY KEY, b INT, c INT);");

  // Insert tuples into table
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test1 VALUES (1, 22, 333);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test1 VALUES (2, 11, 000);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test1 VALUES (3, 22, 444);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test1 VALUES (4, 00, 333);");

  // Create another table for join
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test2(a INT PRIMARY KEY, b INT, c INT);");

  // Insert tuples into table
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test2 VALUES (1, 22, 000);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test2 VALUES (2, 11, 333);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test2 VALUES (3, 22, 555);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test2 VALUES (4, 00, 000);");

  /************************* Basic Queries (only joins)
   * *******************************/
  // Product
  TestUtil("SELECT * FROM test1, test2 WHERE test1.a = 1 AND test2.b = 0",
           {"1", "22", "333", "4", "0", "0"}, false);
  TestUtil(
      "SELECT test.a, test1.b FROM test, test1 "
      "WHERE test1.b = 22",
      {"1", "22", "1", "22", "2", "22", "2", "22", "3", "22", "3", "22", "4",
       "22", "4", "22"},
      false);
  TestUtil(
      "SELECT A.a, B.b, C.c FROM test as A, test1 as B, test2 as C "
      "WHERE B.a = 1 AND A.b = 22 and C.a = 2",
      {"1", "22", "333"}, false);

  // Simple 2 table join
  TestUtil("SELECT test.a, test1.a FROM test JOIN test1 ON test.a = test1.a",
           {"1", "1", "2", "2", "3", "3", "4", "4"}, false);

  // Where clause to join
  TestUtil("SELECT test.a, test1.a FROM test, test1 WHERE test.a = test1.a",
           {"1", "1", "2", "2", "3", "3", "4", "4"}, false);

  TestUtil(
      "SELECT test.a, test.b, test1.b, test1.c FROM test, test1 WHERE test.b = "
      "test1.b",
      {"1", "22", "22", "333", "1", "22", "22", "444", "2", "11", "11", "0",
       "4", "0", "0", "333"},
      false);

  // 3 table join
  TestUtil(
      "SELECT test.a, test.b, test1.b, test2.c FROM test2 "
      "JOIN test ON test.b = test2.b "
      "JOIN test1 ON test2.c = test1.c",
      {"1", "22", "0", "11", "2", "11", "333", "22", "2", "11", "333", "0", "4",
       "0", "0", "11"},
      false);

  // 3 table join with where clause
  TestUtil(
      "SELECT test.a, test.b, test1.b, test2.c FROM test2, test, test1 "
      "WHERE test.b = test2.b AND test2.c = test1.c",
      {"1", "22", "11", "0", "2", "11", "22", "333", "2", "11", "0", "333", "4",
       "0", "11", "0"},
      false);

  // 3 table join with where clause
  // This one test NLJoin.
  // Currently cannot support this query because
  // the interpreted hash join is broken.
  TestUtil(
      "SELECT test.a, test.b, test1.b, test2.c FROM test, test1, test2 "
      "WHERE test.b = test2.b AND test2.c = test1.c",
      {"1", "22", "11", "0", "2", "11", "22", "333", "2", "11", "0", "333", "4",
       "0", "11", "0"},
      false);

  // 2 table join with where clause and predicate
  TestUtil(
      "SELECT test.a, test1.b FROM test, test1 "
      "WHERE test.a = test1.a AND test1.b = 22",
      {"1", "22", "3", "22"}, false);

  // 2 table join with where clause and predicate
  // predicate column not in select list
  TestUtil(
      "SELECT test.a FROM test, test1 "
      "WHERE test.a = test1.a AND test1.b = 22",
      {"1", "3"}, false);

  // Test joining same table with different alias
  TestUtil(
      "SELECT A.a, B.a FROM test1 as A , test1 as B "
      "WHERE A.a = 1 and B.a = 1",
      {"1", "1"}, false);
  TestUtil(
      "SELECT A.b, B.b FROM test1 as A, test1 as B "
      "WHERE A.a = B.a",
      {
          "22", "22", "22", "22", "11", "11", "0", "0",
      },
      false);

  // Test mixing single table predicates with join predicates
  TestUtil(
      "SELECT test.b FROM TEST, TEST1 "
      "WHERE test.a = test1.a and test.c > 333 ",
      {"33", "0"}, false);

  /************************* Complex Queries *******************************/
  // Test projection with join
  TestUtil(
      "SELECT test.a, test.b+test2.b FROM TEST, TEST2 WHERE test.a = test2.a",
      {"1", "44", "2", "22", "3", "55", "4", "0"}, false);

  // Test order by, limit, projection with join
  TestUtil(
      "SELECT test.a, test.b+test2.b FROM TEST, TEST2 "
      "WHERE test.a = test2.a "
      "ORDER BY test.c+test2.c LIMIT 3",
      {"1", "44", "2", "22", "4", "0"}, true);

  // Test group by with join
  TestUtil(
      "SELECT SUM(test2.b) FROM TEST, TEST2 "
      "WHERE test.a = test2.a "
      "GROUP BY test.a",
      {"11", "0", "22", "22"}, false);

  // Test group by, order by with join
  TestUtil(
      "SELECT SUM(test2.b), test.a FROM TEST, TEST2 "
      "WHERE test.a = test2.a "
      "GROUP BY test.a "
      "ORDER BY test.a",
      {"22", "1", "11", "2", "22", "3", "0", "4"}, true);
}

TEST_F(OptimizerSQLTests, IndexTest) {
  TestingSQLUtil::ExecuteSQLQuery(
      "create table foo(a int, b varchar(32), primary key(a, b));");

  TestingSQLUtil::ExecuteSQLQuery("create index sk0 on foo(a);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO foo VALUES (2, '323');");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO foo VALUES (2, '313');");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO foo VALUES (1, '313');");

  TestUtil("select * from foo where b = '313';", {"2", "313", "1", "313"},
           false);
}

TEST_F(OptimizerSQLTests, QueryDerivedTableTest) {
  // Create extra table
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test2(a int primary key, b int, c varchar(32))");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test2 VALUES (1, 22, '1st');");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test2 VALUES (2, 11, '2nd');");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test2 VALUES (3, 33, '3rd');");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test2 VALUES (5, 00, '4th');");
  TestUtil("select A.b from (select b from test where a = 1) as A", {"22"},
           false);
  TestUtil("select * from (select b from test where a = 1) as A", {"22"},
           false);
  TestUtil(
      "select A.b, B.b from (select b from test where a = 1) as A, (select b "
      "from test as t where a=2) as B",
      {"22", "11"}, false);
  TestUtil(
      "select B.b from (select b from test where a = 1) as A, (select b from "
      "test as t where a=2) as B",
      {"11"}, false);
  TestUtil(
      "select * from (select b from test where a = 1) as A, (select b from "
      "test as t where a=2) as B",
      {"22", "11"}, false);
  TestUtil(
      "select * from (select b from test) as A, (select b from test as t) as B "
      "where A.b = B.b",
      {"22", "22", "11", "11", "33", "33", "0", "0"}, false);
  TestUtil(
      "select * from (select b from test) as A, (select b from test) as B "
      "where A.b = B.b",
      {"22", "22", "11", "11", "33", "33", "0", "0"}, false);
  TestUtil(
      "select * from (select a+b as a, c from test) as A, (select a+b as a, c "
      "as c from test2) as B where A.a=B.a",
      {"13", "0", "13", "2nd", "23", "333", "23", "1st", "36", "444", "36",
       "3rd"},
      false);
  TestUtil(
      "select A.c, B.c from (select a+b as a, c from test) as A, (select a+b "
      "as a, c as c from test2) as B where A.a=B.a order by A.a",
      {"0", "2nd", "333", "1st", "444", "3rd"}, true);
  TestUtil(
      "select A.a, B.c from (select count(*) as a from test) as A, (select "
      "avg(a) as C from test2) as B",
      {"4", "2.75"}, false);
}

TEST_F(OptimizerSQLTests, NestedQueryTest) {
  // Create extra table
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test2(a int primary key, b int, c varchar(32))");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test2 VALUES (1, 22, '1st');");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test2 VALUES (2, 11, '2nd');");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test2 VALUES (3, 33, '3rd');");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test2 VALUES (5, 00, '4th');");
  TestUtil(
      "select B.a from test as B where exists (select b as a from test where a "
      "= B.a);",
      {"1", "2", "3", "4"}, false);
  TestUtil(
      "select b from test where a in (select a from test as t where a = "
      "test.a)",
      {"11", "22", "33", "0"}, false);
  TestUtil(
      "select B.a from test as B where exists (select b as a from test2 where "
      "a = B.a) and "
      "b in (select b from test where b > 22);",
      {"3"}, false);
  TestUtil(
      "select B.a from test as B where exists (select b as a from test2 where "
      "a = B.a) and "
      "b in (select b from test) and c > 0;",
      {"1", "3"}, false);
  TestUtil(
      "select t1.a, t2.a from test as t1 join test as t2 on t1.a=t2.a "
      "where t1.b+t2.b in (select 2*b from test2 where a > 2)",
      {"3", "3", "4", "4"}, false);
  TestUtil(
      "select B.a from test as B where exists (select b as a from test as T "
      "where a = B.a and exists (select c from test where T.c = c));",
      {"1", "2", "3", "4"}, false);
}

/*
TEST_F(OptimizerSQLTests, NestedQueryWithAggregationTest) {
  // Nested with aggregation
  TestingSQLUtil::ExecuteSQLQuery("CREATE TABLE agg(a int, b int);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO agg VALUES (1, 2);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO agg VALUES (1, 3);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO agg VALUES (2, 3);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO agg VALUES (2, 4);");
  TestUtil(
      "select B.a from test as B where exists (select count(b) from test where "
      "a "
      "= B.a);",
      {"1", "2", "3", "4"}, false);
  TestUtil(
      "select b from test where a in (select sum(a) from test as t where a = "
      "test.a group by b)",
      {"11", "22", "33", "0"}, false);
  TestUtil(
      "select b from test where a < (select avg(a)+10 from test as t where a = "
      "test.a group by b);",
      {"11", "22", "33", "0"}, false);
  TestUtil(
      "select b from test as t where b/10+2 in (select sum(b) from agg where b "
      "< 4 and a = t.a group by a);",
      {"11"}, false);
  //  TestUtil(
  //      "select b from test as t where exists (select sum(b) from agg where b
  //      < 4 and a = t.a group by a);",
  //      {"11", "22"}, false);

  TestingSQLUtil::ExecuteSQLQuery(
      "create table student(sid int primary key, name varchar(32));");
  TestingSQLUtil::ExecuteSQLQuery(
      "create table course(cid int, sid int, score double);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO student VALUES(1, 'Patrick');");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO student VALUES(2, 'David');");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO student VALUES(3, 'Alice');");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO student VALUES(4, 'Bob');");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO course VALUES(1, 1, 95);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO course VALUES(1, 2, 90.5);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO course VALUES(1, 3, 99);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO course VALUES(2, 1, 89);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO course VALUES(2, 2, 76);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO course VALUES(2, 3, 50);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO course VALUES(3, 1, 91);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO course VALUES(3, 2, 92.5);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO course VALUES(3, 3, 89);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO course VALUES(4, 1, 45);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO course VALUES(4, 2, 65);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO course VALUES(4, 3, 77);");
  TestUtil(
      "select s.name, c.cid from student as s join course as c on s.sid = "
      "c.sid "
      "where c.score = (select min(score) from course where sid = s.sid) and "
      "s.sid < 4;",
      {"Patrick", "4", "David", "4", "Alice", "2"}, false);
}
*/

}  // namespace test
}  // namespace peloton
