//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// plan_selection_test.cpp
//
// Identification: test/optimizer/plan_selection_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "binder/bind_node_visitor.h"
#include "catalog/catalog.h"
#include "common/harness.h"
#include "common/logger.h"
#include "common/statement.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/testing_executor_util.h"
#include "expression/abstract_expression.h"
#include "expression/operator_expression.h"
#include "optimizer/operator_expression.h"
#include "optimizer/operators.h"
#include "optimizer/optimizer.h"
#include "optimizer/rule.h"
#include "optimizer/rule_impls.h"
#include "parser/postgresparser.h"
#include "planner/create_plan.h"
#include "planner/delete_plan.h"
#include "planner/insert_plan.h"
#include "planner/update_plan.h"
#include "sql/testing_sql_util.h"
#include "type/value_factory.h"


namespace peloton {
namespace test {


class PlanSelectionTest : public PelotonTest {};

TEST_F(PlanSelectionTest, SimpleJoinOrderTest) {

  // Create database
  TestingExecutorUtil::InitializeDatabase(DEFAULT_DB_NAME);

  // Create and populate tables
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test1(id INT PRIMARY KEY, b DECIMAL, c VARCHAR);");

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test2(id INT PRIMARY KEY, b DECIMAL, c VARCHAR);");

  // Populate Tables table
  int small_table_size = 1;
  int large_table_size = 100;

  for (int i = 1; i <= small_table_size; i++) {
    std::stringstream ss;
    ss << "INSERT INTO test1 VALUES (" << i << ", 1.1, 'abcd');";
    TestingSQLUtil::ExecuteSQLQuery(ss.str());
  }

  for (int i = 1; i <= large_table_size; i++) {
    std::stringstream ss;
    ss << "INSERT INTO test2 VALUES (" << i << ", 1.1, 'abcd');";
    TestingSQLUtil::ExecuteSQLQuery(ss.str());
  }
  txn_manager.CommitTransaction(txn);


  // Generate plan
  auto &peloton_parser = parser::PostgresParser::GetInstance();
  auto stmt = peloton_parser.BuilbdParseTree(
      "SELECT * FROM test1, test2 WHERE test1.a = test2.a");

  Optimizer optimizer;

  Optimizer::





  TestingExecutorUtil::DeleteDatabase(DEFAULT_DB_NAME);

  return;
}

}
}


