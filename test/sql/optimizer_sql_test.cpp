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
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);
  
  CreateAndLoadTable();
  
  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;
  std::unique_ptr<optimizer::AbstractOptimizer> optimizer(
                                                          new optimizer::Optimizer());
  
  std::string query("SELECT * from test");
  auto select_plan =
  TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query);
  EXPECT_EQ(select_plan->GetPlanNodeType(), PlanNodeType::SEQSCAN);
  
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
  
  // test small int
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
                                               optimizer, "SELECT b, a, c from test where a=1", result, tuple_descriptor,
                                               rows_changed, error_message);
  // Check the return value
  // Should be: 22, 1, 333
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ("22", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, 1));
  
  // free the database just created
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(OptimizerSQLTests, SelectOrderByTest) {
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);

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
  EXPECT_EQ(select_plan->GetPlanNodeType(), PlanNodeType::ORDERBY);
  EXPECT_EQ(select_plan->GetChildren()[0]->GetPlanNodeType(),
            PlanNodeType::SEQSCAN);

  // test order by
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);
  // Check the return value
  // Should be: 11, 22
  EXPECT_EQ("11", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("22", TestingSQLUtil::GetResultValueAsString(result, 1));

  query = "SELECT a from test order by c desc";

  // check for plan node type
  select_plan =
      TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query);
  EXPECT_EQ(select_plan->GetPlanNodeType(), PlanNodeType::ORDERBY);
  EXPECT_EQ(select_plan->GetChildren()[0]->GetPlanNodeType(),
            PlanNodeType::SEQSCAN);

  // test order by
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);
  // Check the return value
  EXPECT_EQ("4", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, 1));


  // Something wrong with column property.
  query = "SELECT * from test order by c";

  // check for plan node type
  select_plan =
      TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query);
  EXPECT_EQ(select_plan->GetPlanNodeType(), PlanNodeType::ORDERBY);
  EXPECT_EQ(select_plan->GetChildren()[0]->GetPlanNodeType(),
            PlanNodeType::SEQSCAN);

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
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(OptimizerSQLTests, SelectLimitTest) {
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);

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
  EXPECT_EQ(select_plan->GetPlanNodeType(), PlanNodeType::LIMIT);
  EXPECT_EQ(select_plan->GetChildren()[0]->GetPlanNodeType(),
            PlanNodeType::ORDERBY);
  EXPECT_EQ(select_plan->GetChildren()[0]->GetChildren()[0]->GetPlanNodeType(),
            PlanNodeType::SEQSCAN);
  
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
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}
  
TEST_F(OptimizerSQLTests, SelectProjectionTest) {
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);
  
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
  EXPECT_EQ(select_plan->GetPlanNodeType(), PlanNodeType::PROJECTION);
  EXPECT_EQ(select_plan->GetChildren()[0]->GetPlanNodeType(),
            PlanNodeType::SEQSCAN);
  
  // test small int
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
                                               optimizer, query, result, tuple_descriptor, rows_changed, error_message);
  // Check the return value
  // Should be: 27, 332
  EXPECT_EQ("27", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("332", TestingSQLUtil::GetResultValueAsString(result, 1));
  
  // free the database just created
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}
  
TEST_F(OptimizerSQLTests, DeleteSqlTest) {
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);

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

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, "SELECT * FROM test", result, tuple_descriptor, rows_changed, error_message);
  EXPECT_EQ(9, result.size());

  // Delete with predicates
  query = "DELETE FROM test WHERE b = 33";
  optimizer->Reset();
  // check for plan node type
  delete_plan =
      TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query);
  EXPECT_EQ(delete_plan->GetPlanNodeType(), PlanNodeType::DELETE);
  EXPECT_EQ(delete_plan->GetChildren()[0]->GetPlanNodeType(),
            PlanNodeType::SEQSCAN);

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);

  EXPECT_EQ(1, rows_changed);

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, "SELECT * FROM test", result, tuple_descriptor, rows_changed, error_message);
  EXPECT_EQ(6, result.size());


  // Delete with false predicates
  query = "DELETE FROM test WHERE b = 123";
  optimizer->Reset();
  // check for plan node type
  delete_plan =
      TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query);
  EXPECT_EQ(delete_plan->GetPlanNodeType(), PlanNodeType::DELETE);
  EXPECT_EQ(delete_plan->GetChildren()[0]->GetPlanNodeType(),
            PlanNodeType::SEQSCAN);

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);

  EXPECT_EQ(0, rows_changed);

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, "SELECT * FROM test", result, tuple_descriptor, rows_changed, error_message);
  EXPECT_EQ(6, result.size());


  // Full deletion
  query = "DELETE FROM test";
  optimizer->Reset();
  // check for plan node type
  delete_plan =
      TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query);
  EXPECT_EQ(delete_plan->GetPlanNodeType(), PlanNodeType::DELETE);
  EXPECT_EQ(delete_plan->GetChildren()[0]->GetPlanNodeType(),
            PlanNodeType::SEQSCAN);

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);

  EXPECT_EQ(2, rows_changed);

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, "SELECT * FROM test", result, tuple_descriptor, rows_changed, error_message);
  EXPECT_EQ(0, result.size());

  // free the database just created
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}


TEST_F(OptimizerSQLTests, UpdateSqlTest) {
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);

  CreateAndLoadTable();

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;
  std::unique_ptr<optimizer::AbstractOptimizer> optimizer(
      new optimizer::Optimizer());

  std::string query("UPDATE test SET c = -333 WHERE a = 1");

  // check for plan node type
  auto update_plan =
      TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query);
  EXPECT_EQ(update_plan->GetPlanNodeType(), PlanNodeType::UPDATE);

  optimizer->Reset();

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);

  EXPECT_EQ(1, rows_changed);

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, "SELECT c FROM test WHERE a=1", result, tuple_descriptor, rows_changed, error_message);
  EXPECT_EQ("-333", TestingSQLUtil::GetResultValueAsString(result, 0));


  // free the database just created
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}


TEST_F(OptimizerSQLTests, InsertSqlTest) {
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);

  CreateAndLoadTable();

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;
  std::unique_ptr<optimizer::AbstractOptimizer> optimizer(
      new optimizer::Optimizer());

  std::string query("INSERT INTO test VALUES (5, 55, 555);");

  // check for plan node type
  auto plan =
      TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query);
  EXPECT_EQ(plan->GetPlanNodeType(), PlanNodeType::INSERT);

  optimizer->Reset();

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);

  EXPECT_EQ(1, rows_changed);

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, "SELECT * FROM test WHERE a=5", result, tuple_descriptor, rows_changed, error_message);
  EXPECT_EQ(3, result.size());
  EXPECT_EQ("5", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("55", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("555", TestingSQLUtil::GetResultValueAsString(result, 2));



  // free the database just created
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}


TEST_F(OptimizerSQLTests, DDLSqlTest) {
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);

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

  auto table = catalog::Catalog::GetInstance()->GetTableWithName(DEFAULT_DB_NAME, "test2");
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
  } catch (Exception& e) {
    LOG_INFO("Correct! Exception(%s) catched", e.what());
  }

  // free the database just created
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}


}  // namespace test
}  // namespace peloton
