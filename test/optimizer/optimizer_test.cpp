#include "common/harness.h"

#define private public

#include "catalog/catalog.h"
#include "common/logger.h"
#include "common/statement.h"
#include "executor/create_executor.h"
#include "executor/insert_executor.h"
#include "executor/plan_executor.h"
#include "optimizer/optimizer.h"
#include "parser/postgresparser.h"
#include "planner/create_plan.h"
#include "planner/delete_plan.h"
#include "planner/insert_plan.h"
#include "planner/update_plan.h"
#include "tcop/tcop.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Binding Tests
//===--------------------------------------------------------------------===//

using namespace optimizer;

class OptimizerTests : public PelotonTest {};

// Test whether update stament will use index scan plan
// TODO: Split the tests into separate test cases.
TEST_F(OptimizerTests, HashJoinTest) {
  LOG_INFO("Bootstrapping...");
  auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
  LOG_INFO("Bootstrapping completed!");

  optimizer::Optimizer optimizer;
  auto& traffic_cop = tcop::TrafficCop::GetInstance();

  // Create a table first
  txn = txn_manager.BeginTransaction();
  LOG_INFO("Creating table");
  LOG_INFO("Query: CREATE TABLE table_a(aid INT PRIMARY KEY,value INT);");
  std::unique_ptr<Statement> statement;
  statement.reset(new Statement(
      "CREATE", "CREATE TABLE table_a(aid INT PRIMARY KEY,value INT);"));

  auto& peloton_parser = parser::PostgresParser::GetInstance();

  auto create_stmt = peloton_parser.BuildParseTree(
      "CREATE TABLE table_a(aid INT PRIMARY KEY,value INT);");

  statement->SetPlanTree(optimizer.BuildPelotonPlanTree(create_stmt));

  std::vector<type::Value> params;
  std::vector<StatementResult> result;
  std::vector<int> result_format;
  result_format =
      std::vector<int>(statement->GetTupleDescriptor().size(), 0);

  executor::ExecuteResult status = traffic_cop.ExecuteStatementPlan(
      statement->GetPlanTree().get(), params, result, result_format);
  LOG_INFO("Statement executed. Result: %s",
           ResultTypeToString(status.m_result).c_str());
  LOG_INFO("Table Created");
  txn_manager.CommitTransaction(txn);

  EXPECT_EQ(catalog::Catalog::GetInstance()
                ->GetDatabaseWithName(DEFAULT_DB_NAME)
                ->GetTableCount(),
            1);

  txn = txn_manager.BeginTransaction();
  LOG_INFO("Creating table");
  LOG_INFO("Query: CREATE TABLE table_b(bid INT PRIMARY KEY,value INT);");
  statement.reset(new Statement(
      "CREATE", "CREATE TABLE table_b(bid INT PRIMARY KEY,value INT);"));

  create_stmt = peloton_parser.BuildParseTree(
      "CREATE TABLE table_b(bid INT PRIMARY KEY,value INT);");

  statement->SetPlanTree(optimizer.BuildPelotonPlanTree(create_stmt));

  result_format =
      std::vector<int>(statement->GetTupleDescriptor().size(), 0);
  status = traffic_cop.ExecuteStatementPlan(statement->GetPlanTree().get(),
                                            params, result, result_format);
  LOG_INFO("Statement executed. Result: %s",
           ResultTypeToString(status.m_result).c_str());
  LOG_INFO("Table Created");
  txn_manager.CommitTransaction(txn);

  EXPECT_EQ(catalog::Catalog::GetInstance()
                ->GetDatabaseWithName(DEFAULT_DB_NAME)
                ->GetTableCount(),
            2);

  // Inserting a tuple to table_a
  txn = txn_manager.BeginTransaction();
  LOG_INFO("Inserting a tuple...");
  LOG_INFO("Query: INSERT INTO table_a(aid, value) VALUES (1,1);");
  statement.reset(new Statement(
      "INSERT", "INSERT INTO table_a(aid, value) VALUES (1, 1);"));

  auto insert_stmt = peloton_parser.BuildParseTree(
      "INSERT INTO table_a(aid, value) VALUES (1, 1);");

  statement->SetPlanTree(optimizer.BuildPelotonPlanTree(insert_stmt));

  result_format =
      std::vector<int>(statement->GetTupleDescriptor().size(), 0);
  status = traffic_cop.ExecuteStatementPlan(statement->GetPlanTree().get(),
                                            params, result, result_format);
  LOG_INFO("Statement executed. Result: %s",
           ResultTypeToString(status.m_result).c_str());
  LOG_INFO("Tuple inserted to table_a!");
  txn_manager.CommitTransaction(txn);

  // Inserting a tuple to table_b
  txn = txn_manager.BeginTransaction();
  LOG_INFO("Inserting a tuple...");
  LOG_INFO("Query: INSERT INTO table_b(bid, value) VALUES (1,2);");
  statement.reset(new Statement(
      "INSERT", "INSERT INTO table_b(bid, value) VALUES (1, 2);"));

  insert_stmt = peloton_parser.BuildParseTree(
      "INSERT INTO table_b(bid, value) VALUES (1, 2);");

  statement->SetPlanTree(optimizer.BuildPelotonPlanTree(insert_stmt));

  result_format =
      std::vector<int>(statement->GetTupleDescriptor().size(), 0);
  status = traffic_cop.ExecuteStatementPlan(statement->GetPlanTree().get(),
                                            params, result, result_format);
  LOG_INFO("Statement executed. Result: %s",
           ResultTypeToString(status.m_result).c_str());
  LOG_INFO("Tuple inserted to table_b!");
  txn_manager.CommitTransaction(txn);

  txn = txn_manager.BeginTransaction();
  LOG_INFO("Join ...");
  LOG_INFO("Query: SELECT * FROM table_a INNER JOIN table_b ON aid = bid;");
  statement.reset(new Statement(
      "SELECT", "SELECT * FROM table_a INNER JOIN table_b ON aid = bid;"));

  auto select_stmt = peloton_parser.BuildParseTree(
      "SELECT * FROM table_a INNER JOIN table_b ON aid = bid;");

  statement->SetPlanTree(optimizer.BuildPelotonPlanTree(select_stmt));

  result_format = std::vector<int>(4, 0);
  status = traffic_cop.ExecuteStatementPlan(statement->GetPlanTree().get(),
                                            params, result, result_format);
  LOG_INFO("Statement executed. Result: %s",
           ResultTypeToString(status.m_result).c_str());
  LOG_INFO("Join completed!");
  txn_manager.CommitTransaction(txn);

  LOG_INFO("After Join...");
  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

} /* namespace test */
} /* namespace peloton */
