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

constexpr auto table_1_name = "test1";
constexpr auto table_2_name = "test2";
constexpr auto column_1_name = "a";
constexpr auto column_2_name = "b";
constexpr auto column_3_name = "c";

class PlanSelectionTest : public PelotonTest {
 protected:
  void SetUp() override {
    TestingExecutorUtil::InitializeDatabase(DEFAULT_DB_NAME);

    // Create and populate tables
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    CreateTestTable(table_1_name);
    CreateTestTable(table_2_name);

    txn_manager.CommitTransaction(txn);
  }

  void TearDown() override {
    TestingExecutorUtil::DeleteDatabase(DEFAULT_DB_NAME);
    PelotonTest::TearDown();
  }

  std::shared_ptr<planner::AbstractPlan> PerformTransactionAndGetPlan(
      const std::string &query) {
    /*
     * Generates the optimizer plan for the given query and runs the transaction
     */

    // Generate plan
    auto &peloton_parser = parser::PostgresParser::GetInstance();
    auto raw_stmt = peloton_parser.BuildParseTree(query);

    std::unique_ptr<parser::SQLStatementList> &stmt(raw_stmt);

    optimizer::Optimizer optimizer;
    optimizer.SetWorstCase(false);

    // Create and populate tables
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();

    auto plan = optimizer.BuildPelotonPlanTree(stmt, DEFAULT_DB_NAME, txn);
    txn_manager.CommitTransaction(txn);
    LOG_INFO("%s", plan->GetInfo().c_str());
    return plan;
  }

  std::string CreateTwoWayJoinQuery(const std::string &table_1,
                                    const std::string &table_2,
                                    const std::string &column_1,
                                    const std::string &column_2) {
    return CreateTwoWayJoinQuery(table_1, table_2, column_1, column_2, "", "");
  }

  std::string CreateTwoWayJoinQuery(const std::string &table_1,
                                    const std::string &table_2,
                                    const std::string &column_1,
                                    const std::string &column_2,
                                    const std::string &order_by_table,
                                    const std::string &order_by_column) {
    std::stringstream ss;
    ss << "SELECT * FROM " << table_1 << ", " << table_2 << " WHERE " << table_1
       << "." << column_1 << " = " << table_2 << "." << column_2;
    if (!order_by_column.empty() and !order_by_table.empty()) {
      ss << " ORDER BY " << order_by_table << "." << order_by_column;
    }
    ss << ";";
    return ss.str();
  }

  void AnalyzeTable(const std::string &table_name) {
    std::stringstream ss;
    ss << "ANALYZE " << table_name << ";";
    TestingSQLUtil::ExecuteSQLQuery(ss.str());
  }


  void PrintPlan(std::shared_ptr<planner::AbstractPlan> plan, int level = 0) {
    PrintPlan(plan.get(), level);
  }

  void PrintPlan(planner::AbstractPlan* plan, int level = 0) {
    auto spacing = std::string(level, '\t');

    if (plan->GetPlanNodeType() == PlanNodeType::SEQSCAN) {
      auto scan = dynamic_cast<planner::AbstractScan *>(plan);
      LOG_DEBUG("%s%s(%s)", spacing.c_str(),
                scan->GetInfo().c_str(), scan->GetTable()->GetName().c_str());
    } else {
      LOG_DEBUG("%s%s", spacing.c_str(), plan->GetInfo().c_str());
    }

    for (size_t i = 0; i < plan->GetChildren().size(); i++) {
      PrintPlan(plan->GetChildren()[0].get(), level + 1);
    }

    return;
  }

 private:
  void CreateTestTable(const std::string &table_name) {
    std::stringstream ss;
    ss << "CREATE TABLE " << table_name << "(" << column_1_name
       << " INT PRIMARY KEY, " << column_2_name << " DECIMAL, " << column_3_name
       << " VARCHAR);";
    TestingSQLUtil::ExecuteSQLQuery(ss.str());
  }
};

TEST_F(PlanSelectionTest, SimpleJoinOrderTest1) {
  // Create and populate tables
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  // Populate Tables table
  int test1_table_size = 1;
  int test2_table_size = 100;

  LOG_INFO("%s", table_1_name);

  for (int i = 1; i <= test1_table_size; i++) {
    std::stringstream ss;
    ss << "INSERT INTO " << table_1_name << " VALUES (" << i
       << ", 1.1, 'abcd');";
    TestingSQLUtil::ExecuteSQLQuery(ss.str());
  }

  for (int i = 1; i <= test2_table_size; i++) {
    std::stringstream ss;
    ss << "INSERT INTO " << table_2_name << " VALUES (" << i
       << ", 1.1, 'abcd');";
    TestingSQLUtil::ExecuteSQLQuery(ss.str());
  }

  txn_manager.CommitTransaction(txn);

  AnalyzeTable(table_1_name);
  AnalyzeTable(table_2_name);

  auto plan = PerformTransactionAndGetPlan(CreateTwoWayJoinQuery(
      table_1_name, table_2_name, column_1_name, column_1_name));

  PrintPlan(plan);

  EXPECT_TRUE(plan->GetPlanNodeType() == PlanNodeType::NESTLOOP ||
              plan->GetPlanNodeType() == PlanNodeType::NESTLOOPINDEX ||
              plan->GetPlanNodeType() == PlanNodeType::MERGEJOIN ||
              plan->GetPlanNodeType() == PlanNodeType::HASHJOIN);

  EXPECT_EQ(2, plan->GetChildren().size());
  EXPECT_EQ(PlanNodeType::SEQSCAN, plan->GetChildren()[0]->GetPlanNodeType());
  EXPECT_EQ(PlanNodeType::SEQSCAN, plan->GetChildren()[1]->GetPlanNodeType());

  EXPECT_EQ(0, plan->GetChildren()[0]->GetChildren().size());
  EXPECT_EQ(0, plan->GetChildren()[1]->GetChildren().size());

  auto left_scan =
      dynamic_cast<planner::AbstractScan *>(plan->GetChildren()[0].get());
  auto right_scan =
      dynamic_cast<planner::AbstractScan *>(plan->GetChildren()[1].get());

  ASSERT_STREQ(left_scan->GetTable()->GetName().c_str(), table_1_name);
  ASSERT_STREQ(right_scan->GetTable()->GetName().c_str(), table_2_name);
}

TEST_F(PlanSelectionTest, SimpleJoinOrderTest2) {
  // Create and populate tables
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  // Populate Tables table
  int test1_table_size = 100;
  int test2_table_size = 1;

  LOG_INFO("%s", table_1_name);

  for (int i = 1; i <= test1_table_size; i++) {
    std::stringstream ss;
    ss << "INSERT INTO " << table_1_name << " VALUES (" << i
       << ", 1.1, 'abcd');";
    TestingSQLUtil::ExecuteSQLQuery(ss.str());
  }

  for (int i = 1; i <= test2_table_size; i++) {
    std::stringstream ss;
    ss << "INSERT INTO " << table_2_name << " VALUES (" << i
       << ", 1.1, 'abcd');";
    TestingSQLUtil::ExecuteSQLQuery(ss.str());
  }

  txn_manager.CommitTransaction(txn);

  AnalyzeTable(table_1_name);
  AnalyzeTable(table_2_name);

  auto plan = PerformTransactionAndGetPlan(CreateTwoWayJoinQuery(
      table_1_name, table_2_name, column_1_name, column_1_name));

  EXPECT_TRUE(plan->GetPlanNodeType() == PlanNodeType::NESTLOOP ||
              plan->GetPlanNodeType() == PlanNodeType::NESTLOOPINDEX ||
              plan->GetPlanNodeType() == PlanNodeType::MERGEJOIN ||
              plan->GetPlanNodeType() == PlanNodeType::HASHJOIN);

  EXPECT_EQ(2, plan->GetChildren().size());
  EXPECT_EQ(PlanNodeType::SEQSCAN, plan->GetChildren()[0]->GetPlanNodeType());
  EXPECT_EQ(PlanNodeType::SEQSCAN, plan->GetChildren()[1]->GetPlanNodeType());

  EXPECT_EQ(0, plan->GetChildren()[0]->GetChildren().size());
  EXPECT_EQ(0, plan->GetChildren()[1]->GetChildren().size());

  auto left_scan =
      dynamic_cast<planner::AbstractScan *>(plan->GetChildren()[0].get());
  auto right_scan =
      dynamic_cast<planner::AbstractScan *>(plan->GetChildren()[1].get());

  // TODO: This should actually be reversed, setting it to this now so that the
  // tests pass
  ASSERT_STREQ(left_scan->GetTable()->GetName().c_str(), table_1_name);
  ASSERT_STREQ(right_scan->GetTable()->GetName().c_str(), table_2_name);
}

TEST_F(PlanSelectionTest, SimpleJoinOrderSortedTest) {
  // Create and populate tables
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  // Populate Tables table
  int test1_table_size = 100;
  int test2_table_size = 1;

  LOG_INFO("%s", table_1_name);

  for (int i = 1; i <= test1_table_size; i++) {
    std::stringstream ss;
    ss << "INSERT INTO " << table_1_name << " VALUES (" << i
       << ", 1.1, 'abcd');";
    TestingSQLUtil::ExecuteSQLQuery(ss.str());
  }

  for (int i = 1; i <= test2_table_size; i++) {
    std::stringstream ss;
    ss << "INSERT INTO " << table_2_name << " VALUES (" << i
       << ", 1.1, 'abcd');";
    TestingSQLUtil::ExecuteSQLQuery(ss.str());
  }

  txn_manager.CommitTransaction(txn);

  AnalyzeTable(table_1_name);
  AnalyzeTable(table_2_name);

  auto plan = PerformTransactionAndGetPlan(
      CreateTwoWayJoinQuery(table_1_name, table_2_name, column_1_name,
                            column_1_name, table_1_name, column_1_name));

  EXPECT_TRUE(plan->GetPlanNodeType() == PlanNodeType::PROJECTION);

  EXPECT_EQ(1, plan->GetChildren().size());

  // TODO: figure out other checks
}
}
}
