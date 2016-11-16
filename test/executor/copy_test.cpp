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
#include "executor/copy_executor.h"
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

  // Create a table without primary key
  StatsTestsUtil::CreateTable(false);
  auto txn = txn_manager.BeginTransaction();
  std::string short_string = "eeeeeeeeee";
  std::string long_string =
      short_string + short_string + short_string + short_string + short_string +
      short_string + short_string + short_string + short_string + short_string +
      short_string + short_string + short_string + short_string + short_string +
      short_string + short_string + short_string;
  std::string escape_string = "eeeeeee,eeeeee,eeeeeee,";

  // Inserting tuples end-to-end
  int num_tuples = 100;
  size_t num_bytes_to_write = 0;
  size_t integer_len = 5;
  size_t default_delimiter_len = 2;
  size_t extra_delimiter_len = 6;
  for (int i = 0; i < num_tuples; i++) {
    // Choose a string and calculate the number of bytes to write
    std::string insert_str;
    if (i % 3 == 0) {
      insert_str = short_string;
    } else if (i % 3 == 1) {
      insert_str = long_string;
    } else {
      insert_str = escape_string;
      num_bytes_to_write += extra_delimiter_len;
    }
    // Including the byte to write delimiters
    num_bytes_to_write +=
        insert_str.length() + integer_len + default_delimiter_len;

    // Execute insert
    auto statement = StatsTestsUtil::GetInsertStmt(12345, insert_str);
    std::vector<common::Value> params;
    std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);
    std::vector<ResultType> result;
    bridge::peloton_status status = bridge::PlanExecutor::ExecutePlan(
        statement->GetPlanTree().get(), params, result, result_format);
    EXPECT_EQ(status.m_result, peloton::RESULT_SUCCESS);
    LOG_TRACE("Statement executed. Result: %d", status.m_result);
  }
  LOG_INFO("Tuples inserted!");
  txn_manager.CommitTransaction(txn);

  // Now Copying end-to-end
  LOG_INFO("Copying a table...");
  std::string copy_sql =
      "COPY emp_db.department_table TO './copy_output.csv' DELIMITER ',';";
  txn = txn_manager.BeginTransaction();
  LOG_INFO("Query: %s", copy_sql.c_str());
  std::unique_ptr<Statement> statement(new Statement("COPY", copy_sql));

  LOG_INFO("Building parse tree...");
  auto& peloton_parser = parser::Parser::GetInstance();
  auto copy_stmt = peloton_parser.BuildParseTree(copy_sql);

  LOG_INFO("Building plan tree...");
  auto copy_plan = optimizer::SimpleOptimizer::BuildPelotonPlanTree(copy_stmt);
  statement->SetPlanTree(copy_plan);

  LOG_INFO("Building executor tree...");
  // Build executor tree
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));
  auto copy_executor =
      new executor::CopyExecutor(copy_plan.get(), context.get());
  std::unique_ptr<executor::AbstractExecutor> root_executor(copy_executor);
  std::unique_ptr<executor::AbstractExecutor> seq_scan_executor(
      new executor::SeqScanExecutor(copy_plan->GetChildren()[0].get(),
                                    context.get()));
  copy_executor->AddChild(seq_scan_executor.get());

  LOG_INFO("Executing plan...");
  // Initialize the executor tree
  auto status = root_executor->Init();
  EXPECT_EQ(status, true);
  // Execute the tree until we get result tiles from root node
  while (status == true) {
    status = root_executor->Execute();
  }

  // Check the number of bypes written
  EXPECT_EQ(copy_executor->GetTotalBytesWritten(), num_bytes_to_write);
  txn_manager.CommitTransaction(txn);

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog->DropDatabaseWithName("emp_db", txn);
  txn_manager.CommitTransaction(txn);
}

}  // End test namespace
}  // End peloton namespace
