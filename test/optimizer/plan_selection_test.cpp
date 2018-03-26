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
#include "planner/abstract_scan_plan.h"
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
      "CREATE TABLE test1(a INT PRIMARY KEY, b DECIMAL, c VARCHAR);");

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test2(a INT PRIMARY KEY, b DECIMAL, c VARCHAR);");

  // Populate Tables table
  int test1_table_size = 1;
  int test2_table_size = 100;

  for (int i = 1; i <= test1_table_size; i++) {
    std::stringstream ss;
    ss << "INSERT INTO test1 VALUES (" << i << ", 1.1, 'abcd');";
    TestingSQLUtil::ExecuteSQLQuery(ss.str());
  }

  for (int i = 1; i <= test2_table_size; i++) {
    std::stringstream ss;
    ss << "INSERT INTO test2 VALUES (" << i << ", 1.1, 'abcd');";
    TestingSQLUtil::ExecuteSQLQuery(ss.str());
  }
  txn_manager.CommitTransaction(txn);


  // Generate plan
  auto &peloton_parser = parser::PostgresParser::GetInstance();
  auto raw_stmt = peloton_parser.BuildParseTree(
      "SELECT * FROM test1, test2 WHERE test1.a = test2.a");

  std::unique_ptr<parser::SQLStatementList> &stmt(raw_stmt);

  optimizer::Optimizer optimizer;

  txn = txn_manager.BeginTransaction();
  auto plan = optimizer.BuildPelotonPlanTree(stmt, DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
  printf("%s\n", plan->GetInfo().c_str());

//  LOG_DEBUG("Child size: %zu", plan->GetChildren().size());
//  LOG_DEBUG("Child[0]: %s", plan->GetChildren()[0]->GetInfo().c_str());
//  LOG_DEBUG("Child[1]: %s", plan->GetChildren()[1]->GetInfo().c_str());

  EXPECT_TRUE(plan->GetPlanNodeType() == PlanNodeType::NESTLOOP ||
              plan->GetPlanNodeType() == PlanNodeType::NESTLOOPINDEX ||
              plan->GetPlanNodeType() == PlanNodeType::MERGEJOIN ||
              plan->GetPlanNodeType() == PlanNodeType::HASHJOIN);

  EXPECT_EQ(2, plan->GetChildren().size());
  EXPECT_EQ(PlanNodeType::SEQSCAN, plan->GetChildren()[0]->GetPlanNodeType());
  EXPECT_EQ(PlanNodeType::HASH, plan->GetChildren()[1]->GetPlanNodeType());

  EXPECT_EQ(1, plan->GetChildren()[1]->GetChildren().size());
  EXPECT_EQ(PlanNodeType::SEQSCAN, plan->GetChildren()[1]->GetChildren()[0]->GetPlanNodeType());

  auto left_scan = dynamic_cast<planner::AbstractScan *>(plan->GetChildren()[0].get());
  auto right_scan = dynamic_cast<planner::AbstractScan *>(plan->GetChildren()[1]->GetChildren()[0].get());

  LOG_DEBUG("Left Table: %s", left_scan->GetTable()->GetName().c_str());
  LOG_DEBUG("Right Table: %s", right_scan->GetTable()->GetName().c_str());

  TestingExecutorUtil::DeleteDatabase(DEFAULT_DB_NAME);

  return;
}

}
}


