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
#include "common/harness.h"
#include "common/statement.h"
#include "common/timer.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/testing_executor_util.h"
#include "expression/operator_expression.h"
#include "optimizer/cost_calculator.h"
#include "optimizer/memo.h"
#include "optimizer/operator_expression.h"
#include "optimizer/operators.h"
#include "optimizer/optimizer.h"
#include "optimizer/rule_impls.h"
#include "planner/delete_plan.h"
#include "planner/insert_plan.h"
#include "planner/update_plan.h"
#include "sql/testing_sql_util.h"

namespace peloton {

namespace optimizer {

class BadCostModel : public CostCalculator {
  double CalculateCost(GroupExpression *gexpr, Memo *memo,
                       concurrency::TransactionContext *txn) override {
    return -1 * CostCalculator::CalculateCost(gexpr, memo, txn);
  }
};

}  // namespace optimizer

namespace test {

constexpr auto table_1_name = "test1";
constexpr auto table_2_name = "test2";
constexpr auto table_3_name = "test3";
constexpr auto column_1_name = "a";
constexpr auto column_2_name = "b";
constexpr auto column_3_name = "c";

using AbstractCostCalculatorUniqPtr =
    std::unique_ptr<peloton::optimizer::AbstractCostCalculator>;

class PlanSelectionTest : public PelotonTest {
 protected:
  void SetUp() override {
    TestingExecutorUtil::InitializeDatabase(DEFAULT_DB_NAME);

    // Create and populate tables
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    CreateTestTable(table_1_name);
    CreateTestTable(table_2_name);
    CreateTestTable(table_3_name);

    txn_manager.CommitTransaction(txn);

    bad_cost_calculator_ =
        AbstractCostCalculatorUniqPtr(new peloton::optimizer::BadCostModel());
  }

  void TearDown() override {
    TestingExecutorUtil::DeleteDatabase(DEFAULT_DB_NAME);
    PelotonTest::TearDown();
  }

  std::shared_ptr<planner::AbstractPlan> PerformTransactionAndGetPlan(
      const std::string &query) {
    auto cost_calculator =
        AbstractCostCalculatorUniqPtr(new peloton::optimizer::CostCalculator);
    return PerformTransactionAndGetPlan(query, std::move(cost_calculator));
  }

  std::shared_ptr<planner::AbstractPlan> PerformTransactionAndGetPlan(
      const std::string &query, AbstractCostCalculatorUniqPtr cost_calculator) {
    /*
     * Generates the optimizer plan for the given query and runs the transaction
     */

    // Generate plan
    auto &peloton_parser = parser::PostgresParser::GetInstance();
    auto raw_stmt = peloton_parser.BuildParseTree(query);

    std::unique_ptr<parser::SQLStatementList> &stmt(raw_stmt);

    optimizer::Optimizer optimizer{std::move(cost_calculator)};

    // Create and populate tables
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();

    auto plan = optimizer.BuildPelotonPlanTree(stmt, DEFAULT_DB_NAME, txn);
    timer_.Start();
    txn_manager.CommitTransaction(txn);
    timer_.Stop();
    LOG_INFO("Query Execution Duration: %f", timer_.GetDuration());
    PrintPlan(plan);

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

  std::string CreateThreeWayJoinQuery(const std::string &table_1,
                                      const std::string &table_2,
                                      const std::string &table_3,
                                      const std::string &column_1,
                                      const std::string &column_2,
                                      const std::string &column_3) {
    return CreateThreeWayJoinQuery(table_1, table_2, table_3, column_1,
                                   column_2, column_3, "", "");
  }

  std::string CreateThreeWayJoinQuery(
      const std::string &table_1, const std::string &table_2,
      const std::string &table_3, const std::string &column_1,
      const std::string &column_2, const std::string &column_3,
      const std::string &order_by_table, const std::string &order_by_column) {
    std::stringstream ss;
    ss << "SELECT * FROM " << table_1 << ", " << table_2 << "," << table_3
       << " WHERE " << table_1 << "." << column_1 << " = " << table_2 << "."
       << column_2 << " AND " << table_2 << "." << column_2 << " = " << table_3
       << "." << column_3;
    if (!order_by_column.empty() and !order_by_table.empty()) {
      ss << " ORDER BY " << order_by_table << "." << order_by_column;
    }
    ss << ";";
    return ss.str();
  }

  void InsertDataHelper(const std::string &table_name, int tuple_count) {
    int batch_size = 1000;
    std::stringstream ss;
    auto count = 0;
    if (tuple_count > batch_size) {
      for (int i = 0; i < tuple_count; i += batch_size) {
        ss.str(std::string());
        ss << "INSERT INTO " << table_name << " VALUES ";
        for (int j = 1; j <= batch_size; j++) {
          count++;
          ss << "(" << count << ", 1.1, 'abcd')";
          if (j < batch_size) {
            ss << ",";
          }
        }
        ss << ";";
        TestingSQLUtil::ExecuteSQLQuery(ss.str());
      }
    } else {
      ss << "INSERT INTO " << table_name << " VALUES ";
      for (int i = 1; i <= tuple_count; i++) {
        ss << "(" << i << ", 1.1, 'abcd')";
        if (i < tuple_count) {
          ss << ",";
        }
        count++;
      }
      ss << ";";
      TestingSQLUtil::ExecuteSQLQuery(ss.str());
    }
    LOG_INFO("COUNT: %d", count);
  }

  void AnalyzeTable(const std::string &table_name) {
    std::stringstream ss;
    ss << "ANALYZE " << table_name << ";";
    TestingSQLUtil::ExecuteSQLQuery(ss.str());
  }

  void PrintPlan(std::shared_ptr<planner::AbstractPlan> plan, int level = 0) {
    PrintPlan(plan.get(), level);
  }

  void PrintPlan(planner::AbstractPlan *plan, int level = 0) {
    auto spacing = std::string(level, '\t');

    if (plan->GetPlanNodeType() == PlanNodeType::SEQSCAN) {
      auto scan = dynamic_cast<planner::AbstractScan *>(plan);
      LOG_DEBUG("%s%s(%s)", spacing.c_str(), scan->GetInfo().c_str(),
                scan->GetTable()->GetName().c_str());
    } else {
      LOG_DEBUG("%s%s", spacing.c_str(), plan->GetInfo().c_str());
    }

    for (size_t i = 0; i < plan->GetChildren().size(); i++) {
      PrintPlan(plan->GetChildren()[i].get(), level + 1);
    }

    return;
  }

  Timer<std::milli> timer_;
  AbstractCostCalculatorUniqPtr bad_cost_calculator_;

 private:
  void CreateTestTable(const std::string &table_name) {
    std::stringstream ss;
    ss << "CREATE TABLE " << table_name << "(" << column_1_name
       << " INT PRIMARY KEY, " << column_2_name << " DECIMAL, " << column_3_name
       << " VARCHAR);";
    TestingSQLUtil::ExecuteSQLQuery(ss.str());
  }
};

TEST_F(PlanSelectionTest, SimpleJoinOrderTestSmall1) {
  // Create and populate tables
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  // Populate Tables table
  int test1_table_size = 1;
  int test2_table_size = 100;

  InsertDataHelper(table_1_name, test1_table_size);
  InsertDataHelper(table_2_name, test2_table_size);

  txn_manager.CommitTransaction(txn);

  AnalyzeTable(table_1_name);
  AnalyzeTable(table_2_name);

  auto plan = PerformTransactionAndGetPlan(CreateTwoWayJoinQuery(
      table_1_name, table_2_name, column_1_name, column_1_name));

  EXPECT_TRUE(plan->GetPlanNodeType() == PlanNodeType::NESTLOOP);

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

TEST_F(PlanSelectionTest, ThreeWayJoinTest) {
  // Create and populate tables
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  // Populate Tables table
  int test1_table_size = 2;
  int test2_table_size = 10;
  int test3_table_size = 100;

  InsertDataHelper(table_1_name, test1_table_size);
  InsertDataHelper(table_2_name, test2_table_size);
  InsertDataHelper(table_3_name, test3_table_size);

  txn_manager.CommitTransaction(txn);

  AnalyzeTable(table_1_name);
  AnalyzeTable(table_2_name);
  AnalyzeTable(table_3_name);

  auto plan = PerformTransactionAndGetPlan(
      CreateThreeWayJoinQuery(table_1_name, table_2_name, table_3_name,
                              column_1_name, column_1_name, column_1_name));

  // TODO: Add checks
}

TEST_F(PlanSelectionTest, SimpleJoinOrderTestWorstCaseSmall1) {
  // Create and populate tables
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  // Populate Tables table
  int test1_table_size = 1;
  int test2_table_size = 100;

  InsertDataHelper(table_1_name, test1_table_size);
  InsertDataHelper(table_2_name, test2_table_size);

  txn_manager.CommitTransaction(txn);

  AnalyzeTable(table_1_name);
  AnalyzeTable(table_2_name);

  auto plan = PerformTransactionAndGetPlan(
      CreateTwoWayJoinQuery(table_1_name, table_2_name, column_1_name,
                            column_1_name),
      std::move(bad_cost_calculator_));

  EXPECT_TRUE(plan->GetPlanNodeType() == PlanNodeType::HASHJOIN);

  EXPECT_EQ(2, plan->GetChildren().size());
  EXPECT_EQ(PlanNodeType::SEQSCAN, plan->GetChildren()[0]->GetPlanNodeType());
  EXPECT_EQ(PlanNodeType::HASH, plan->GetChildren()[1]->GetPlanNodeType());

  EXPECT_EQ(0, plan->GetChildren()[0]->GetChildren().size());
  EXPECT_EQ(1, plan->GetChildren()[1]->GetChildren().size());

  auto left_scan =
      dynamic_cast<planner::AbstractScan *>(plan->GetChildren()[0].get());
  auto right_scan = dynamic_cast<planner::AbstractScan *>(
      plan->GetChildren()[1]->GetChildren()[0].get());

  // TODO: This should actually be reversed, setting it to this now so that the
  // tests pass
  ASSERT_STREQ(left_scan->GetTable()->GetName().c_str(), table_1_name);
  ASSERT_STREQ(right_scan->GetTable()->GetName().c_str(), table_2_name);
}

TEST_F(PlanSelectionTest, SimpleJoinOrderSmallTest2) {
  // Create and populate tables
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  // Populate Tables table
  int test1_table_size = 100;
  int test2_table_size = 1;

  InsertDataHelper(table_1_name, test1_table_size);
  InsertDataHelper(table_2_name, test2_table_size);

  txn_manager.CommitTransaction(txn);

  AnalyzeTable(table_1_name);
  AnalyzeTable(table_2_name);

  auto plan = PerformTransactionAndGetPlan(CreateTwoWayJoinQuery(
      table_1_name, table_2_name, column_1_name, column_1_name));

  EXPECT_TRUE(plan->GetPlanNodeType() == PlanNodeType::NESTLOOP);

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

TEST_F(PlanSelectionTest, SimpleJoinOrderSmallTest3) {
  // Create and populate tables
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  // Populate Tables table
  int test1_table_size = 1;
  int test2_table_size = 100;

  InsertDataHelper(table_1_name, test1_table_size);
  InsertDataHelper(table_2_name, test2_table_size);

  txn_manager.CommitTransaction(txn);

  AnalyzeTable(table_1_name);
  AnalyzeTable(table_2_name);

  auto plan = PerformTransactionAndGetPlan(CreateTwoWayJoinQuery(
      table_2_name, table_1_name, column_1_name, column_1_name));

  EXPECT_TRUE(plan->GetPlanNodeType() == PlanNodeType::NESTLOOP);

  EXPECT_EQ(2, plan->GetChildren().size());
  EXPECT_EQ(PlanNodeType::SEQSCAN, plan->GetChildren()[0]->GetPlanNodeType());
  EXPECT_EQ(PlanNodeType::SEQSCAN, plan->GetChildren()[1]->GetPlanNodeType());

  EXPECT_EQ(0, plan->GetChildren()[0]->GetChildren().size());
  EXPECT_EQ(0, plan->GetChildren()[1]->GetChildren().size());

  auto left_scan =
      dynamic_cast<planner::AbstractScan *>(plan->GetChildren()[0].get());
  auto right_scan =
      dynamic_cast<planner::AbstractScan *>(plan->GetChildren()[1].get());

  // TODO: Join order seems to follow order of join in query. This should really
  // be swapped. Set
  // to this way so the tests pass
  ASSERT_STREQ(left_scan->GetTable()->GetName().c_str(), table_2_name);
  ASSERT_STREQ(right_scan->GetTable()->GetName().c_str(), table_1_name);
}

/* The similarity between this and the SimpleJoinOrderSmallTest2 is to ensure
 * that the optimizer does not pick
 * joins simply based on the order the tables are written in the SQL query.
 */
TEST_F(PlanSelectionTest, SimpleJoinOrderSmallTest4) {
  // Create and populate tables
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  // Populate Tables table
  int test1_table_size = 100;
  int test2_table_size = 1;

  InsertDataHelper(table_1_name, test1_table_size);
  InsertDataHelper(table_2_name, test2_table_size);

  txn_manager.CommitTransaction(txn);

  AnalyzeTable(table_1_name);
  AnalyzeTable(table_2_name);

  auto plan = PerformTransactionAndGetPlan(CreateTwoWayJoinQuery(
      table_2_name, table_1_name, column_1_name, column_1_name));

  EXPECT_TRUE(plan->GetPlanNodeType() == PlanNodeType::NESTLOOP);

  EXPECT_EQ(2, plan->GetChildren().size());
  EXPECT_EQ(PlanNodeType::SEQSCAN, plan->GetChildren()[0]->GetPlanNodeType());
  EXPECT_EQ(PlanNodeType::SEQSCAN, plan->GetChildren()[1]->GetPlanNodeType());

  EXPECT_EQ(0, plan->GetChildren()[0]->GetChildren().size());
  EXPECT_EQ(0, plan->GetChildren()[1]->GetChildren().size());

  auto left_scan =
      dynamic_cast<planner::AbstractScan *>(plan->GetChildren()[0].get());
  auto right_scan =
      dynamic_cast<planner::AbstractScan *>(plan->GetChildren()[1].get());

  // TODO: Join order seems to follow order of join in query. This should really
  // be swapped. Set
  // to this way so the tests pass
  ASSERT_STREQ(left_scan->GetTable()->GetName().c_str(), table_2_name);
  ASSERT_STREQ(right_scan->GetTable()->GetName().c_str(), table_1_name);
}

TEST_F(PlanSelectionTest, SimpleJoinOrderLargeTest1) {
  // TODO: move this to another testing framework (currently takes too long)
  // Same applies to other "Long" tests
  // Create and populate tables
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  // Populate Tables table
  int test1_table_size = 10;
  int test2_table_size = 1000;

  InsertDataHelper(table_1_name, test1_table_size);
  InsertDataHelper(table_2_name, test2_table_size);

  txn_manager.CommitTransaction(txn);

  AnalyzeTable(table_1_name);
  AnalyzeTable(table_2_name);

  auto query = "SELECT COUNT(*) from test2;";
  std::vector<ResultValue> results;
  TestingSQLUtil::ExecuteSQLQuery(query, results);
  for (auto result : results) {
    LOG_INFO("RESULT: %s", result.c_str());
  }

  auto plan = PerformTransactionAndGetPlan(CreateTwoWayJoinQuery(
      table_1_name, table_2_name, column_1_name, column_1_name));

  EXPECT_TRUE(plan->GetPlanNodeType() == PlanNodeType::HASHJOIN);

  EXPECT_EQ(2, plan->GetChildren().size());
  EXPECT_EQ(PlanNodeType::SEQSCAN, plan->GetChildren()[0]->GetPlanNodeType());
  EXPECT_EQ(PlanNodeType::HASH, plan->GetChildren()[1]->GetPlanNodeType());

  EXPECT_EQ(0, plan->GetChildren()[0]->GetChildren().size());
  EXPECT_EQ(1, plan->GetChildren()[1]->GetChildren().size());

  auto left_scan =
      dynamic_cast<planner::AbstractScan *>(plan->GetChildren()[0].get());
  auto right_scan = dynamic_cast<planner::AbstractScan *>(
      plan->GetChildren()[1]->GetChildren()[0].get());

  // TODO: This should actually be reversed, setting it to this now so that the
  // tests pass
  ASSERT_STREQ(left_scan->GetTable()->GetName().c_str(), table_1_name);
  ASSERT_STREQ(right_scan->GetTable()->GetName().c_str(), table_2_name);
}

TEST_F(PlanSelectionTest, SimpleJoinOrderLargeTest2) {
  // Create and populate tables
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  // Populate Tables table
  int test1_table_size = 1000;
  int test2_table_size = 10;

  InsertDataHelper(table_1_name, test1_table_size);
  InsertDataHelper(table_2_name, test2_table_size);

  txn_manager.CommitTransaction(txn);

  AnalyzeTable(table_1_name);
  AnalyzeTable(table_2_name);

  auto plan = PerformTransactionAndGetPlan(CreateTwoWayJoinQuery(
      table_1_name, table_2_name, column_1_name, column_1_name));

  EXPECT_TRUE(plan->GetPlanNodeType() == PlanNodeType::HASHJOIN);

  EXPECT_EQ(2, plan->GetChildren().size());
  EXPECT_EQ(PlanNodeType::SEQSCAN, plan->GetChildren()[0]->GetPlanNodeType());
  EXPECT_EQ(PlanNodeType::HASH, plan->GetChildren()[1]->GetPlanNodeType());

  EXPECT_EQ(0, plan->GetChildren()[0]->GetChildren().size());
  EXPECT_EQ(1, plan->GetChildren()[1]->GetChildren().size());

  auto left_scan =
      dynamic_cast<planner::AbstractScan *>(plan->GetChildren()[0].get());
  auto right_scan = dynamic_cast<planner::AbstractScan *>(
      plan->GetChildren()[1]->GetChildren()[0].get());

  // TODO: This should actually be reversed, setting it to this now so that the
  // tests pass
  ASSERT_STREQ(left_scan->GetTable()->GetName().c_str(), table_2_name);
  ASSERT_STREQ(right_scan->GetTable()->GetName().c_str(), table_1_name);
}

TEST_F(PlanSelectionTest, SimpleJoinOrderLargeTest3) {
  // Create and populate tables
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  // Populate Tables table
  int test1_table_size = 10;
  int test2_table_size = 1000;

  InsertDataHelper(table_1_name, test1_table_size);
  InsertDataHelper(table_2_name, test2_table_size);

  txn_manager.CommitTransaction(txn);

  AnalyzeTable(table_1_name);
  AnalyzeTable(table_2_name);

  auto plan = PerformTransactionAndGetPlan(CreateTwoWayJoinQuery(
      table_2_name, table_1_name, column_1_name, column_1_name));

  EXPECT_TRUE(plan->GetPlanNodeType() == PlanNodeType::HASHJOIN);

  EXPECT_EQ(2, plan->GetChildren().size());
  EXPECT_EQ(PlanNodeType::SEQSCAN, plan->GetChildren()[0]->GetPlanNodeType());
  EXPECT_EQ(PlanNodeType::HASH, plan->GetChildren()[1]->GetPlanNodeType());

  EXPECT_EQ(0, plan->GetChildren()[0]->GetChildren().size());
  EXPECT_EQ(1, plan->GetChildren()[1]->GetChildren().size());

  auto left_scan =
      dynamic_cast<planner::AbstractScan *>(plan->GetChildren()[0].get());
  auto right_scan = dynamic_cast<planner::AbstractScan *>(
      plan->GetChildren()[1]->GetChildren()[0].get());

  // TODO: This should actually be reversed, setting it to this now so that the
  // tests pass
  ASSERT_STREQ(left_scan->GetTable()->GetName().c_str(), table_1_name);
  ASSERT_STREQ(right_scan->GetTable()->GetName().c_str(), table_2_name);
}

TEST_F(PlanSelectionTest, SimpleJoinOrderLargeTest4) {
  // Create and populate tables
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  // Populate Tables table
  int test1_table_size = 1000;
  int test2_table_size = 10;

  InsertDataHelper(table_1_name, test1_table_size);
  InsertDataHelper(table_2_name, test2_table_size);

  txn_manager.CommitTransaction(txn);

  AnalyzeTable(table_1_name);
  AnalyzeTable(table_2_name);

  auto plan = PerformTransactionAndGetPlan(CreateTwoWayJoinQuery(
      table_2_name, table_1_name, column_1_name, column_1_name));

  EXPECT_TRUE(plan->GetPlanNodeType() == PlanNodeType::HASHJOIN);

  EXPECT_EQ(2, plan->GetChildren().size());
  EXPECT_EQ(PlanNodeType::SEQSCAN, plan->GetChildren()[0]->GetPlanNodeType());
  EXPECT_EQ(PlanNodeType::HASH, plan->GetChildren()[1]->GetPlanNodeType());

  EXPECT_EQ(0, plan->GetChildren()[0]->GetChildren().size());
  EXPECT_EQ(1, plan->GetChildren()[1]->GetChildren().size());

  auto left_scan =
      dynamic_cast<planner::AbstractScan *>(plan->GetChildren()[0].get());
  auto right_scan = dynamic_cast<planner::AbstractScan *>(
      plan->GetChildren()[1]->GetChildren()[0].get());

  // TODO: This should actually be reversed, setting it to this now so that the
  // tests pass
  ASSERT_STREQ(left_scan->GetTable()->GetName().c_str(), table_2_name);
  ASSERT_STREQ(right_scan->GetTable()->GetName().c_str(), table_1_name);
}

TEST_F(PlanSelectionTest, SimpleJoinOrderSortedTest) {
  // Create and populate tables
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  // Populate Tables table
  const unsigned int test1_table_size = 1;
  const unsigned int test2_table_size = 100;

  InsertDataHelper(table_1_name, test1_table_size);
  InsertDataHelper(table_2_name, test2_table_size);

  txn_manager.CommitTransaction(txn);

  AnalyzeTable(table_1_name);
  AnalyzeTable(table_2_name);

  auto plan = PerformTransactionAndGetPlan(
      CreateTwoWayJoinQuery(table_1_name, table_2_name, column_1_name,
                            column_1_name, table_1_name, column_1_name));

  EXPECT_TRUE(plan->GetPlanNodeType() == PlanNodeType::PROJECTION);

  EXPECT_EQ(2, plan->GetChildren().size());

  // TODO: figure out other checks
}
}
}
