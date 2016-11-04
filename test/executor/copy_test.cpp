//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// copy_test.cpp
//
// Identification: test/executor/copy_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>

#include "catalog/catalog.h"
#include "common/harness.h"
#include "common/logger.h"
#include "common/statement.h"
#include "executor/seq_scan_executor.h"
#include "optimizer/simple_optimizer.h"
#include "parser/parser.h"
#include "planner/seq_scan_plan.h"
#include "statistics/stats_tests_util.h"

#include "gtest/gtest.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Copy Tests
//===--------------------------------------------------------------------===//

class CopyTests : public PelotonTest {};

TEST_F(CopyTests, Copying) {
  auto catalog = catalog::Catalog::GetInstance();
  catalog->CreateDatabase("emp_db", nullptr);
  auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  // Create a table first
  StatsTestsUtil::CreateTable();

  // Inserting tuples end-to-end
  int num_tuples = 50;
  for (int i = 0; i < num_tuples; i++) {
    // Execute insert
    auto statement = StatsTestsUtil::GetInsertStmt(i, "short_string");
    std::vector<common::Value> params;
    std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);
    std::vector<ResultType> result;
    bridge::peloton_status status = bridge::PlanExecutor::ExecutePlan(
        statement->GetPlanTree().get(), params, result, result_format);
    EXPECT_EQ(status.m_result, peloton::RESULT_SUCCESS);
    LOG_TRACE("Statement executed. Result: %d", status.m_result);
  }
  LOG_INFO("Tuples inserted!");

  // Now Copying end-to-end
  LOG_INFO("Copying a tuple...");
  std::string copy_sql =
      "COPY emp_db.department_table TO './copy_output.csv' DELIMITER ',';";
  auto txn = txn_manager.BeginTransaction();
  LOG_INFO("Query: %s", copy_sql.c_str());
  std::unique_ptr<Statement> statement(new Statement("COPY", copy_sql));
  LOG_INFO("Building parse tree...");
  auto& peloton_parser = parser::Parser::GetInstance();
  auto copy_stmt = peloton_parser.BuildParseTree(copy_sql);
  LOG_INFO("Building plan tree...");
  statement->SetPlanTree(
      optimizer::SimpleOptimizer::BuildPelotonPlanTree(copy_stmt));
  std::vector<common::Value> params;
  std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);
  std::vector<ResultType> result;
  bridge::PlanExecutor::PrintPlan(statement->GetPlanTree().get(), "Plan");
  LOG_INFO("Executing plan...");

  bridge::peloton_status status = bridge::PlanExecutor::ExecutePlan(
      statement->GetPlanTree().get(), params, result, result_format);
  LOG_INFO("Statement executed. Result: %d", status.m_result);
  txn_manager.CommitTransaction(txn);

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog->DropDatabaseWithName("emp_db", txn);
  txn_manager.CommitTransaction(txn);
}

}  // End test namespace
}  // End peloton namespace
