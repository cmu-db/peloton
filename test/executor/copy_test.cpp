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
#include "sql/testing_sql_util.h"

#include "catalog/catalog.h"
#include "common/harness.h"
#include "common/logger.h"
#include "common/statement.h"
#include "executor/copy_executor.h"
#include "executor/seq_scan_executor.h"
#include "optimizer/optimizer.h"
#include "optimizer/rule.h"
#include "parser/postgresparser.h"
#include "planner/seq_scan_plan.h"
#include "traffic_cop/traffic_cop.h"

#include "gtest/gtest.h"
#include "statistics/testing_stats_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Copy Tests
//===--------------------------------------------------------------------===//

class CopyTests : public PelotonTest {};

TEST_F(CopyTests, Copying) {
  auto catalog = catalog::Catalog::GetInstance();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog->CreateDatabase("emp_db", txn);
  txn_manager.CommitTransaction(txn);

  std::unique_ptr<optimizer::AbstractOptimizer> optimizer(
      new optimizer::Optimizer);
  auto &traffic_cop = tcop::TrafficCop::GetInstance();
  traffic_cop.SetTaskCallback(TestingSQLUtil::UtilTestTaskCallback,
                              &TestingSQLUtil::counter_);

  // Create a table without primary key
  TestingStatsUtil::CreateTable(false);
  txn = txn_manager.BeginTransaction();
  traffic_cop.SetTcopTxnState(txn);
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
    auto statement = TestingStatsUtil::GetInsertStmt(12345, insert_str);
    std::vector<type::Value> params;
    std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);
    std::vector<ResultValue> result;

    TestingSQLUtil::counter_.store(1);
    executor::ExecutionResult status = traffic_cop.ExecuteHelper(
        statement->GetPlanTree(), params, result, result_format);

    if (traffic_cop.GetQueuing()) {
      TestingSQLUtil::ContinueAfterComplete();
      traffic_cop.ExecuteStatementPlanGetResult();
      status = traffic_cop.p_status_;
      traffic_cop.SetQueuing(false);
    }

    EXPECT_EQ(status.m_result, peloton::ResultType::SUCCESS);
    LOG_TRACE("Statement executed. Result: %s",
              ResultTypeToString(status.m_result).c_str());
  }
  LOG_TRACE("Tuples inserted!");
  traffic_cop.CommitQueryHelper();

  // Now Copying end-to-end
  LOG_TRACE("Copying a table...");
  std::string copy_sql =
      "COPY emp_db.department_table TO './copy_output.csv' DELIMITER ',';";
  txn = txn_manager.BeginTransaction();
  LOG_TRACE("Query: %s", copy_sql.c_str());
  std::unique_ptr<Statement> statement(new Statement("COPY", copy_sql));

  LOG_TRACE("Building parse tree...");
  auto &peloton_parser = parser::PostgresParser::GetInstance();
  auto copy_stmt = peloton_parser.BuildParseTree(copy_sql);

  LOG_TRACE("Building plan tree...");
  auto copy_plan =
      optimizer->BuildPelotonPlanTree(copy_stmt, DEFAULT_DB_NAME, txn);
  statement->SetPlanTree(copy_plan);

  LOG_TRACE("Building executor tree...");
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

  LOG_TRACE("Executing plan...");
  // Initialize the executor tree
  auto status = root_executor->Init();
  EXPECT_TRUE(status);
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

}  // namespace test
}  // namespace peloton
