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

#include "sql/testing_sql_util.h"
#include "catalog/catalog.h"
#include "common/harness.h"
#include "executor/create_executor.h"
#include "optimizer/optimizer.h"
#include "optimizer/simple_optimizer.h"
#include "planner/create_plan.h"
#include "planner/order_by_plan.h"

namespace peloton {
namespace test {

class OptimizerSQLTests : public PelotonTest {};

void CreateAndLoadTable() {
  // Create a table first
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test(a INT PRIMARY KEY, b INT, c INT);");

  // Insert tuples into table
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (1, 22, 333);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (2, 11, 000);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (3, 33, 444);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (4, 00, 555);");
}

TEST_F(OptimizerSQLTests, SimpleSelectTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;
  std::unique_ptr<optimizer::AbstractOptimizer> optimizer(
      new optimizer::Optimizer());

  std::string query("SELECT * from test");
  LOG_DEBUG("Running Query %s", query.c_str());
  auto select_plan =
      TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query);

  // test small int
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);
  // Check the return value
  // Should be: 1, 22, 333
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ(12, result.size());
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("22", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("333", TestingSQLUtil::GetResultValueAsString(result, 2));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, 3));
  EXPECT_EQ("11", TestingSQLUtil::GetResultValueAsString(result, 4));
  EXPECT_EQ("0", TestingSQLUtil::GetResultValueAsString(result, 5));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, 6));
  EXPECT_EQ("33", TestingSQLUtil::GetResultValueAsString(result, 7));
  EXPECT_EQ("444", TestingSQLUtil::GetResultValueAsString(result, 8));
  EXPECT_EQ("4", TestingSQLUtil::GetResultValueAsString(result, 9));
  EXPECT_EQ("0", TestingSQLUtil::GetResultValueAsString(result, 10));
  EXPECT_EQ("555", TestingSQLUtil::GetResultValueAsString(result, 11));

  LOG_DEBUG("Running Query SELECT c, b from test where a=1");
  // test small int
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, "SELECT c, b from test where a=1", result, tuple_descriptor,
      rows_changed, error_message);
  // Check the return value
  // Should be: 333, 22
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ("333", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("22", TestingSQLUtil::GetResultValueAsString(result, 1));

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(OptimizerSQLTests, SelectOrderByTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;
  std::unique_ptr<optimizer::AbstractOptimizer> optimizer(
      new optimizer::Optimizer());

  // Something wrong with column property.
  std::string query("SELECT b from test order by c");

  // check for plan node type
  auto select_plan =
      TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query);
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

  query = "SELECT a from test order by c desc";

  // check for plan node type
  select_plan = TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query);
  //  EXPECT_EQ(select_plan->GetPlanNodeType(), PlanNodeType::PROJECTION);
  //  EXPECT_EQ(select_plan->GetChildren()[0]->GetPlanNodeType(),
  //            PlanNodeType::ORDERBY);

  // test order by
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);
  // Check the return value
  EXPECT_EQ("4", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, 1));

  // Something wrong with column property.
  query = "SELECT a, b, c from test order by a + c";

  // check for plan node type
  select_plan = TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query);
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

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(OptimizerSQLTests, SelectLimitTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;
  std::unique_ptr<optimizer::AbstractOptimizer> optimizer(
      new optimizer::Optimizer());

  // Test limit without offset
  std::string query("SELECT b FROM test ORDER BY b LIMIT 3");

  auto select_plan =
      TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query);
  //  EXPECT_EQ(select_plan->GetPlanNodeType(), PlanNodeType::LIMIT);
  //  EXPECT_EQ(select_plan->GetChildren()[0]->GetPlanNodeType(),
  //            PlanNodeType::ORDERBY);
  //  EXPECT_EQ(select_plan->GetChildren()[0]->GetChildren()[0]->GetPlanNodeType(),
  //            PlanNodeType::SEQSCAN);

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);

  EXPECT_EQ(3, result.size());
  EXPECT_EQ("0", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("11", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("22", TestingSQLUtil::GetResultValueAsString(result, 2));

  // Test limit with offset
  query = "SELECT b FROM test ORDER BY b LIMIT 2 OFFSET 2";
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);

  EXPECT_EQ(2, result.size());
  EXPECT_EQ("22", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("33", TestingSQLUtil::GetResultValueAsString(result, 1));

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(OptimizerSQLTests, SelectProjectionTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;
  std::unique_ptr<optimizer::AbstractOptimizer> optimizer(
      new optimizer::Optimizer());

  std::string query("SELECT a * 5 + b, -1 + c from test");

  // check for plan node type
  auto select_plan =
      TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query);
  //  EXPECT_EQ(select_plan->GetPlanNodeType(), PlanNodeType::PROJECTION);
  //  EXPECT_EQ(select_plan->GetChildren()[0]->GetPlanNodeType(),
  //            PlanNodeType::SEQSCAN);

  // test small int
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);
  // Check the return value
  // Should be: 27, 332
  EXPECT_EQ("27", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("332", TestingSQLUtil::GetResultValueAsString(result, 1));

  // test projection for order by
  query = "SELECT a * 5 + b - c FROM test ORDER BY a * 10 + b";
  LOG_DEBUG("%s", query.c_str());

  // check for plan node type
  select_plan = TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query);
  //  EXPECT_EQ(select_plan->GetPlanNodeType(), PlanNodeType::PROJECTION);
  //  EXPECT_EQ(select_plan->GetChildren()[0]->GetPlanNodeType(),
  //            PlanNodeType::ORDERBY);

  // test small int
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);
  // Check the return value
  // Should be: 27, 332
  EXPECT_EQ(4, result.size());
  EXPECT_EQ("21", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("-306", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("-535", TestingSQLUtil::GetResultValueAsString(result, 2));
  EXPECT_EQ("-396", TestingSQLUtil::GetResultValueAsString(result, 3));

  // test projection for order by
  query = "SELECT a, a + c FROM test ORDER BY a * 3 * b DESC, b + c / 5 ASC";
  LOG_DEBUG("%s", query.c_str());

  // check for plan node type
  select_plan = TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query);
  //  EXPECT_EQ(select_plan->GetPlanNodeType(), PlanNodeType::PROJECTION);
  //  EXPECT_EQ(select_plan->GetChildren()[0]->GetPlanNodeType(),
  //          PlanNodeType::ORDERBY);

  // test small int
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);
  // Check the return value
  // Should be: 27, 332
  EXPECT_EQ(8, result.size());
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("447", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, 2));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, 3));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, 4));
  EXPECT_EQ("334", TestingSQLUtil::GetResultValueAsString(result, 5));
  EXPECT_EQ("4", TestingSQLUtil::GetResultValueAsString(result, 6));
  EXPECT_EQ("559", TestingSQLUtil::GetResultValueAsString(result, 7));

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(OptimizerSQLTests, DeleteSqlTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;
  std::unique_ptr<optimizer::AbstractOptimizer> optimizer(
      new optimizer::Optimizer());

  // TODO: Test for index scan
  // Delete with predicates
  std::string query("DELETE FROM test WHERE a = 1 and c = 333");

  // check for plan node type
  auto delete_plan =
      TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query);
  EXPECT_EQ(delete_plan->GetPlanNodeType(), PlanNodeType::DELETE);
  EXPECT_EQ(delete_plan->GetChildren()[0]->GetPlanNodeType(),
            PlanNodeType::SEQSCAN);

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);

  EXPECT_EQ(1, rows_changed);

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(optimizer, "SELECT * FROM test",
                                               result, tuple_descriptor,
                                               rows_changed, error_message);
  EXPECT_EQ(9, result.size());

  // Delete with predicates
  query = "DELETE FROM test WHERE b = 33";

  // check for plan node type
  delete_plan = TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query);
  EXPECT_EQ(delete_plan->GetPlanNodeType(), PlanNodeType::DELETE);
  EXPECT_EQ(delete_plan->GetChildren()[0]->GetPlanNodeType(),
            PlanNodeType::SEQSCAN);

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);

  EXPECT_EQ(1, rows_changed);

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(optimizer, "SELECT * FROM test",
                                               result, tuple_descriptor,
                                               rows_changed, error_message);
  EXPECT_EQ(6, result.size());

  // Delete with false predicates
  query = "DELETE FROM test WHERE b = 123";

  // check for plan node type
  delete_plan = TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query);
  EXPECT_EQ(delete_plan->GetPlanNodeType(), PlanNodeType::DELETE);
  EXPECT_EQ(delete_plan->GetChildren()[0]->GetPlanNodeType(),
            PlanNodeType::SEQSCAN);

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);

  EXPECT_EQ(0, rows_changed);

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(optimizer, "SELECT * FROM test",
                                               result, tuple_descriptor,
                                               rows_changed, error_message);
  EXPECT_EQ(6, result.size());

  // Full deletion
  query = "DELETE FROM test";

  // check for plan node type
  delete_plan = TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query);
  EXPECT_EQ(delete_plan->GetPlanNodeType(), PlanNodeType::DELETE);
  EXPECT_EQ(delete_plan->GetChildren()[0]->GetPlanNodeType(),
            PlanNodeType::SEQSCAN);

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);

  EXPECT_EQ(2, rows_changed);

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(optimizer, "SELECT * FROM test",
                                               result, tuple_descriptor,
                                               rows_changed, error_message);
  EXPECT_EQ(0, result.size());

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(OptimizerSQLTests, UpdateSqlTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;
  std::unique_ptr<optimizer::AbstractOptimizer> optimizer(
      new optimizer::Optimizer());

  std::string query("UPDATE test SET c = b + 1 WHERE a = 1");

  // check for plan node type
  auto update_plan =
      TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query);
  EXPECT_EQ(update_plan->GetPlanNodeType(), PlanNodeType::UPDATE);

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);

  EXPECT_EQ(1, rows_changed);

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, "SELECT c FROM test WHERE a=1", result, tuple_descriptor,
      rows_changed, error_message);
  EXPECT_EQ("23", TestingSQLUtil::GetResultValueAsString(result, 0));

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(OptimizerSQLTests, InsertSqlTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;
  std::unique_ptr<optimizer::AbstractOptimizer> optimizer(
      new optimizer::Optimizer());

  std::string query("INSERT INTO test VALUES (5, 55, 555);");

  // check for plan node type
  auto plan = TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query);
  EXPECT_EQ(plan->GetPlanNodeType(), PlanNodeType::INSERT);

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);

  EXPECT_EQ(1, rows_changed);

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, "SELECT * FROM test WHERE a=5", result, tuple_descriptor,
      rows_changed, error_message);
  EXPECT_EQ(3, result.size());
  EXPECT_EQ("5", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("55", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("555", TestingSQLUtil::GetResultValueAsString(result, 2));

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(OptimizerSQLTests, DDLSqlTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;
  std::unique_ptr<optimizer::AbstractOptimizer> optimizer(
      new optimizer::Optimizer());

  std::string query("CREATE TABLE test2(a INT PRIMARY KEY, b INT, c INT);");

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);

  auto table = catalog::Catalog::GetInstance()->GetTableWithName(
      DEFAULT_DB_NAME, "test2");
  EXPECT_NE(nullptr, table);
  auto cols = table->GetSchema()->GetColumns();
  EXPECT_EQ(3, cols.size());
  EXPECT_EQ("a", cols[0].column_name);
  EXPECT_EQ(true, cols[0].is_primary_);
  EXPECT_EQ(type::Type::INTEGER, cols[0].GetType());
  EXPECT_EQ("b", cols[1].column_name);
  EXPECT_EQ(type::Type::INTEGER, cols[1].GetType());
  EXPECT_EQ("c", cols[2].column_name);
  EXPECT_EQ(type::Type::INTEGER, cols[2].GetType());

  query = "DROP TABLE test2";
  auto plan = TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query);
  EXPECT_EQ(plan->GetPlanNodeType(), PlanNodeType::DROP);

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);
  try {
    catalog::Catalog::GetInstance()->GetTableWithName(DEFAULT_DB_NAME, "test2");
    EXPECT_TRUE(false);
  } catch (Exception &e) {
    LOG_INFO("Correct! Exception(%s) catched", e.what());
  }

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(OptimizerSQLTests, GroupByTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;
  std::unique_ptr<optimizer::AbstractOptimizer> optimizer(
      new optimizer::Optimizer());

  //  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (1, 22, 333);");
  //  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (2, 11, 000);");
  //  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (3, 33, 444);");
  //  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (4, 00, 555);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (5, 11, 000);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (6, 22, 333);");

  typedef std::vector<std::string> RawTuple;
  typedef std::vector<RawTuple> ResultTuples;
  struct SortByCol {
    uint32_t col_id;
    bool operator()(const RawTuple &a, const RawTuple &b) const {
      return std::stod(a[col_id]) < std::stod(b[col_id]);
    }
  };

  // Basic case
  std::string query = "SELECT b FROM test GROUP BY b having b=11 or b=22";
  LOG_INFO("%s", query.c_str());
  auto select_plan =
      TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query);
  EXPECT_EQ(select_plan->GetPlanNodeType(), PlanNodeType::AGGREGATE_V2);
  EXPECT_EQ(select_plan->GetChildren()[0]->GetPlanNodeType(),
            PlanNodeType::SEQSCAN);
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);
  EXPECT_EQ(2, result.size());
  EXPECT_EQ("11", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("22", TestingSQLUtil::GetResultValueAsString(result, 1));

  // Aggregate function: COUNT(*)
  query = "SELECT COUNT(*) FROM test GROUP BY b";
  LOG_INFO("%s", query.c_str());
  select_plan = TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query);
  EXPECT_EQ(select_plan->GetPlanNodeType(), PlanNodeType::AGGREGATE_V2);
  EXPECT_EQ(select_plan->GetChild(0)->GetPlanNodeType(), PlanNodeType::SEQSCAN);
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);
  {
    ResultTuples expected = {{"1"}, {"1"}, {"2"}, {"2"}};
    ResultTuples actual;
    for (uint32_t i = 0; i < result.size(); i++) {
      actual.push_back({TestingSQLUtil::GetResultValueAsString(result, i)});
    }
    std::sort(actual.begin(), actual.end(), SortByCol{0});
    EXPECT_EQ(expected, actual);
  }

  // Aggregate function: COUNT(a)
  query = "SELECT COUNT(a) FROM test GROUP BY b";
  LOG_INFO("%s", query.c_str());
  select_plan = TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query);
  EXPECT_EQ(select_plan->GetPlanNodeType(), PlanNodeType::AGGREGATE_V2);
  EXPECT_EQ(select_plan->GetChild(0)->GetPlanNodeType(), PlanNodeType::SEQSCAN);
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);
  {
    ResultTuples expected = {{"1"}, {"1"}, {"2"}, {"2"}};
    ResultTuples actual;
    for (uint32_t i = 0; i < result.size(); i++) {
      actual.push_back({TestingSQLUtil::GetResultValueAsString(result, i)});
    }
    std::sort(actual.begin(), actual.end(), SortByCol{0});
    EXPECT_EQ(expected, actual);
  }

  // Aggregate function: AVG(a)
  query = "SELECT AVG(a), b FROM test GROUP BY b having b=22";
  LOG_INFO("%s", query.c_str());
  select_plan = TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query);
  EXPECT_EQ(select_plan->GetPlanNodeType(), PlanNodeType::AGGREGATE_V2);
  EXPECT_EQ(select_plan->GetChild(0)->GetPlanNodeType(), PlanNodeType::SEQSCAN);
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);
  {
    ResultTuples expected = {{"3.5", "22"}};
    ResultTuples actual;
    for (uint32_t i = 0; i < result.size(); i += 2) {
      RawTuple tuple = {TestingSQLUtil::GetResultValueAsString(result, i),
                        TestingSQLUtil::GetResultValueAsString(result, i + 1)};
      actual.push_back(tuple);
    }
    std::sort(actual.begin(), actual.end(), SortByCol{0});
    EXPECT_EQ(expected, actual);
  }

  // Aggregate function: MIN(b)
  query = "SELECT MIN(a), b FROM test GROUP BY b having b=22";
  LOG_INFO("%s", query.c_str());
  select_plan = TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query);
  EXPECT_EQ(select_plan->GetPlanNodeType(), PlanNodeType::AGGREGATE_V2);
  EXPECT_EQ(select_plan->GetChild(0)->GetPlanNodeType(), PlanNodeType::SEQSCAN);
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);
  {
    ResultTuples expected = {{"1", "22"}};
    ResultTuples actual;
    for (uint32_t i = 0; i < result.size(); i += 2) {
      RawTuple tuple = {TestingSQLUtil::GetResultValueAsString(result, i),
                        TestingSQLUtil::GetResultValueAsString(result, i + 1)};
      actual.push_back(tuple);
    }
    std::sort(actual.begin(), actual.end(), SortByCol{0});
    EXPECT_EQ(expected, actual);
  }

  // Aggregate function: MAX(b)
  query = "SELECT MAX(a), b FROM test GROUP BY b having b=22";
  LOG_INFO("%s", query.c_str());
  select_plan = TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query);
  EXPECT_EQ(select_plan->GetPlanNodeType(), PlanNodeType::AGGREGATE_V2);
  EXPECT_EQ(select_plan->GetChild(0)->GetPlanNodeType(), PlanNodeType::SEQSCAN);
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);
  {
    ResultTuples expected = {{"6", "22"}};
    ResultTuples actual;
    for (uint32_t i = 0; i < result.size(); i += 2) {
      RawTuple tuple = {TestingSQLUtil::GetResultValueAsString(result, i),
                        TestingSQLUtil::GetResultValueAsString(result, i + 1)};
      actual.push_back(tuple);
    }
    std::sort(actual.begin(), actual.end(), SortByCol{0});
    EXPECT_EQ(expected, actual);
  }

  // Combine with ORDER BY
  query = "SELECT b FROM test GROUP BY b ORDER BY b";
  LOG_INFO("%s", query.c_str());
  select_plan = TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query);
  EXPECT_EQ(select_plan->GetPlanNodeType(), PlanNodeType::ORDERBY);
  EXPECT_EQ(select_plan->GetChildren()[0]->GetPlanNodeType(),
            PlanNodeType::AGGREGATE_V2);
  EXPECT_EQ(select_plan->GetChildren()[0]->GetChildren()[0]->GetPlanNodeType(),
            PlanNodeType::SEQSCAN);
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);
  EXPECT_EQ(4, result.size());
  EXPECT_EQ("0", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("11", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("22", TestingSQLUtil::GetResultValueAsString(result, 2));
  EXPECT_EQ("33", TestingSQLUtil::GetResultValueAsString(result, 3));

  // Test complex expression in aggregation
  query = "SELECT b, MAX(a + c) FROM test GROUP BY b ORDER BY b";
  LOG_INFO("%s", query.c_str());
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);
  EXPECT_EQ(8, result.size());
  EXPECT_EQ("0", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("559", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("11", TestingSQLUtil::GetResultValueAsString(result, 2));
  EXPECT_EQ("5", TestingSQLUtil::GetResultValueAsString(result, 3));
  EXPECT_EQ("22", TestingSQLUtil::GetResultValueAsString(result, 4));
  EXPECT_EQ("339", TestingSQLUtil::GetResultValueAsString(result, 5));
  EXPECT_EQ("33", TestingSQLUtil::GetResultValueAsString(result, 6));
  EXPECT_EQ("447", TestingSQLUtil::GetResultValueAsString(result, 7));

  // Test complex expression in select list and order by complex expr
  query = "SELECT b + c, SUM(c * a) FROM test GROUP BY b,c ORDER BY b + c";
  LOG_INFO("%s", query.c_str());
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);
  EXPECT_EQ(8, result.size());
  EXPECT_EQ("11", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("0", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("355", TestingSQLUtil::GetResultValueAsString(result, 2));
  EXPECT_EQ("2331", TestingSQLUtil::GetResultValueAsString(result, 3));
  EXPECT_EQ("477", TestingSQLUtil::GetResultValueAsString(result, 4));
  EXPECT_EQ("1332", TestingSQLUtil::GetResultValueAsString(result, 5));
  EXPECT_EQ("555", TestingSQLUtil::GetResultValueAsString(result, 6));
  EXPECT_EQ("2220", TestingSQLUtil::GetResultValueAsString(result, 7));

  // Test complex expression in select list and order by complex expr
  query = "SELECT b, c, MIN(a - b) FROM test GROUP BY c, b ORDER BY b+c";
  LOG_INFO("%s", query.c_str());
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);
  EXPECT_EQ(12, result.size());
  EXPECT_EQ("11", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("0", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("-9", TestingSQLUtil::GetResultValueAsString(result, 2));
  EXPECT_EQ("22", TestingSQLUtil::GetResultValueAsString(result, 3));
  EXPECT_EQ("333", TestingSQLUtil::GetResultValueAsString(result, 4));
  EXPECT_EQ("-21", TestingSQLUtil::GetResultValueAsString(result, 5));
  EXPECT_EQ("33", TestingSQLUtil::GetResultValueAsString(result, 6));
  EXPECT_EQ("444", TestingSQLUtil::GetResultValueAsString(result, 7));
  EXPECT_EQ("-30", TestingSQLUtil::GetResultValueAsString(result, 8));
  EXPECT_EQ("0", TestingSQLUtil::GetResultValueAsString(result, 9));
  EXPECT_EQ("555", TestingSQLUtil::GetResultValueAsString(result, 10));
  EXPECT_EQ("4", TestingSQLUtil::GetResultValueAsString(result, 11));

  // Test plain aggregation
  query = "SELECT SUM(c * a) FROM test";
  LOG_INFO("%s", query.c_str());
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);
  EXPECT_EQ(1, result.size());
  EXPECT_EQ("5883", TestingSQLUtil::GetResultValueAsString(result, 0));

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(OptimizerSQLTests, SelectDistinctTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();
  //  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (1, 22, 333);");
  //  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (2, 11, 000);");
  //  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (3, 33, 444);");
  //  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (4, 00, 555);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (5, 00, 555);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (7, 00, 444);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (6, 22, 333);");

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;
  std::unique_ptr<optimizer::AbstractOptimizer> optimizer(
      new optimizer::Optimizer());

  // Test limit without offset
  std::string query("SELECT DISTINCT b FROM test ORDER BY b");

  auto select_plan =
      TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query);
  EXPECT_EQ(select_plan->GetPlanNodeType(), PlanNodeType::ORDERBY);
  EXPECT_EQ(select_plan->GetChildren()[0]->GetPlanNodeType(),
            PlanNodeType::HASH);
  EXPECT_EQ(select_plan->GetChildren()[0]->GetChildren()[0]->GetPlanNodeType(),
            PlanNodeType::SEQSCAN);

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);

  EXPECT_EQ(4, result.size());
  EXPECT_EQ("0", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("11", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("22", TestingSQLUtil::GetResultValueAsString(result, 2));
  EXPECT_EQ("33", TestingSQLUtil::GetResultValueAsString(result, 3));

  query = "SELECT DISTINCT b, c FROM test ORDER BY 10 * b + c";

  select_plan = TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query);
  EXPECT_EQ(select_plan->GetPlanNodeType(), PlanNodeType::ORDERBY);
  EXPECT_EQ(select_plan->GetChildren()[0]->GetPlanNodeType(),
            PlanNodeType::HASH);
  EXPECT_EQ(select_plan->GetChildren()[0]->GetChildren()[0]->GetPlanNodeType(),
            PlanNodeType::PROJECTION);

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);

  EXPECT_EQ(10, result.size());
  EXPECT_EQ("11", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("0", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("0", TestingSQLUtil::GetResultValueAsString(result, 2));
  EXPECT_EQ("444", TestingSQLUtil::GetResultValueAsString(result, 3));
  EXPECT_EQ("22", TestingSQLUtil::GetResultValueAsString(result, 4));
  EXPECT_EQ("333", TestingSQLUtil::GetResultValueAsString(result, 5));
  EXPECT_EQ("0", TestingSQLUtil::GetResultValueAsString(result, 6));
  EXPECT_EQ("555", TestingSQLUtil::GetResultValueAsString(result, 7));
  EXPECT_EQ("33", TestingSQLUtil::GetResultValueAsString(result, 8));
  EXPECT_EQ("444", TestingSQLUtil::GetResultValueAsString(result, 9));

  query = "SELECT DISTINCT a, b, c FROM test ORDER BY a + 10 * b + c LIMIT 3";

  select_plan = TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query);
  EXPECT_EQ(select_plan->GetPlanNodeType(), PlanNodeType::LIMIT);
  EXPECT_EQ(select_plan->GetChildren()[0]->GetPlanNodeType(),
            PlanNodeType::ORDERBY);
  EXPECT_EQ(select_plan->GetChildren()[0]->GetChildren()[0]->GetPlanNodeType(),
            PlanNodeType::HASH);

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);

  EXPECT_EQ(9, result.size());
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("11", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("0", TestingSQLUtil::GetResultValueAsString(result, 2));
  EXPECT_EQ("7", TestingSQLUtil::GetResultValueAsString(result, 3));
  EXPECT_EQ("0", TestingSQLUtil::GetResultValueAsString(result, 4));
  EXPECT_EQ("444", TestingSQLUtil::GetResultValueAsString(result, 5));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, 6));
  EXPECT_EQ("22", TestingSQLUtil::GetResultValueAsString(result, 7));
  EXPECT_EQ("333", TestingSQLUtil::GetResultValueAsString(result, 8));

  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (5, 11, 000);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (6, 22, 333);");

  // Test group by complex expression
  query = "SELECT DISTINCT b + c FROM test GROUP BY b + c ORDER BY b + c";
  LOG_INFO("%s", query.c_str());
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);
  EXPECT_EQ(5, result.size());
  EXPECT_EQ("11", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("355", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("444", TestingSQLUtil::GetResultValueAsString(result, 2));
  EXPECT_EQ("477", TestingSQLUtil::GetResultValueAsString(result, 3));
  EXPECT_EQ("555", TestingSQLUtil::GetResultValueAsString(result, 4));

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}
}  // namespace test
}  // namespace peloton
