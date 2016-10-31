#include "common/harness.h"

#define private public

#include "catalog/catalog.h"
#include "common/logger.h"
#include "common/statement.h"
#include "executor/create_executor.h"
#include "executor/insert_executor.h"
#include "executor/plan_executor.h"
#include "optimizer/optimizer.h"
#include "optimizer/simple_optimizer.h"
#include "parser/parser.h"
#include "planner/create_plan.h"
#include "planner/delete_plan.h"
#include "planner/insert_plan.h"
#include "planner/update_plan.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Binding Tests
//===--------------------------------------------------------------------===//

using namespace optimizer;

class HashJoinTest : public PelotonTest {};

// Test whether update stament will use index scan plan
TEST_F(HashJoinTest, SimpleHashJoin) {
  LOG_INFO("Bootstrapping...");
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);
  LOG_INFO("Bootstrapping completed!");

  // Create a table first
  auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  // Create table A
  auto txn = txn_manager.BeginTransaction();
  LOG_INFO("Creating table");
  LOG_INFO(
      "Query: CREATE TABLE table_a(id INT PRIMARY KEY, value INT);");
  std::unique_ptr<Statement> statement;
  statement.reset(new Statement("CREATE",
                                "CREATE TABLE table_a(id INT "
                                "PRIMARY KEY, value INT);"));

  auto& peloton_parser = parser::Parser::GetInstance();

  auto create_stmt = peloton_parser.BuildParseTree(
      "CREATE TABLE table_a(id INT PRIMARY KEY, value INT);");

  std::cout<<std::endl;
  std::cout<<create_stmt->GetStatements().size()<<std::endl;
  std::cout<<std::endl;

  statement->SetPlanTree(
      optimizer::SimpleOptimizer::BuildPelotonPlanTree(create_stmt));

  std::vector<common::Value*> params;
  std::vector<ResultType> result;
  bridge::PlanExecutor::PrintPlan(statement->GetPlanTree().get(), "Plan");
  std::vector<int> result_format;
  result_format =
      std::move(std::vector<int>(statement->GetTupleDescriptor().size(), 0));
  bridge::peloton_status status = bridge::PlanExecutor::ExecutePlan(
      statement->GetPlanTree().get(), params, result, result_format);
  LOG_INFO("Statement executed. Result: %d", status.m_result);
  LOG_INFO("Table table_a Created");
  txn_manager.CommitTransaction(txn);

  EXPECT_EQ(catalog::Catalog::GetInstance()
                ->GetDatabaseWithName(DEFAULT_DB_NAME)
                ->GetTableCount(),
            1);

  txn = txn_manager.BeginTransaction();
  LOG_INFO("Creating table");
  LOG_INFO(
      "Query: CREATE TABLE table_b(id INT PRIMARY KEY, value INT);");
  statement.reset(new Statement("CREATE",
                                "CREATE TABLE table_b(id INT "
                                "PRIMARY KEY, value INT);"));

  create_stmt = peloton_parser.BuildParseTree(
      "CREATE TABLE table_b(id INT PRIMARY KEY, value INT);");

  statement->SetPlanTree(
      optimizer::SimpleOptimizer::BuildPelotonPlanTree(create_stmt));

  bridge::PlanExecutor::PrintPlan(statement->GetPlanTree().get(), "Plan");
  result_format =
      std::move(std::vector<int>(statement->GetTupleDescriptor().size(), 0));
  status = bridge::PlanExecutor::ExecutePlan(
      statement->GetPlanTree().get(), params, result, result_format);
  LOG_INFO("Statement executed. Result: %d", status.m_result);
  LOG_INFO("Table table_b Created");
  txn_manager.CommitTransaction(txn);

  EXPECT_EQ(catalog::Catalog::GetInstance()
                ->GetDatabaseWithName(DEFAULT_DB_NAME)
                ->GetTableCount(),
            2);

  // Inserting a tuple end-to-end to A
  txn = txn_manager.BeginTransaction();
  LOG_INFO("Inserting a tuple...");
  LOG_INFO(
      "Query: INSERT INTO table_a(id, value) "
      "VALUES (1,42);");
  statement.reset(new Statement("INSERT",
                                "INSERT INTO table_a(id, value)"
                                "VALUES (1,42);"));

  auto insert_stmt = peloton_parser.BuildParseTree(
      "INSERT INTO table_a(id,value)"
      "VALUES (1,42);");

  statement->SetPlanTree(
      optimizer::SimpleOptimizer::BuildPelotonPlanTree(insert_stmt));

  result_format =
      std::move(std::vector<int>(statement->GetTupleDescriptor().size(), 0));
  status = bridge::PlanExecutor::ExecutePlan(statement->GetPlanTree().get(),
                                             params, result, result_format);
  LOG_INFO("Statement executed. Result: %d", status.m_result);
  LOG_INFO("Tuple inserted!");
  txn_manager.CommitTransaction(txn);
  auto target_table_ = catalog::Catalog::GetInstance()->GetTableWithName(
      DEFAULT_DB_NAME, "table_a");
  // Expected 1 , Primary key index + created index
  EXPECT_EQ(target_table_->GetTupleCount(), 1);
  std::cout<<"After first insertion in table_a\n\n\n";

  txn = txn_manager.BeginTransaction();
  LOG_INFO("Inserting a tuple...");
  LOG_INFO(
      "Query: INSERT INTO table_a(id, value) "
      "VALUES (2,42);");
  statement.reset(new Statement("INSERT",
                                "INSERT INTO table_a(id, value)"
                                "VALUES (2,42);"));

  insert_stmt = peloton_parser.BuildParseTree(
      "INSERT INTO table_a(id,value)"
      "VALUES (2,42);");

  statement->SetPlanTree(
      optimizer::SimpleOptimizer::BuildPelotonPlanTree(insert_stmt));

  result_format =
      std::move(std::vector<int>(statement->GetTupleDescriptor().size(), 0));
  status = bridge::PlanExecutor::ExecutePlan(statement->GetPlanTree().get(),
                                             params, result, result_format);
  LOG_INFO("Statement executed. Result: %d", status.m_result);
  LOG_INFO("Tuple inserted!");
  txn_manager.CommitTransaction(txn);
  target_table_ = catalog::Catalog::GetInstance()->GetTableWithName(
      DEFAULT_DB_NAME, "table_a");
  // Expected 1 , Primary key index + created index
  EXPECT_EQ(target_table_->GetTupleCount(), 2);
  std::cout<<"After second insertion in table_a\n\n\n";

  txn = txn_manager.BeginTransaction();
  LOG_INFO("Inserting a tuple...");
  LOG_INFO(
      "Query: INSERT INTO table_b(id, value) "
      "VALUES (1,42);");
  statement.reset(new Statement("INSERT",
                                "INSERT INTO table_b(id, value)"
                                "VALUES (1,42);"));

  insert_stmt = peloton_parser.BuildParseTree(
      "INSERT INTO table_b(id,value)"
      "VALUES (1,42);");

  statement->SetPlanTree(
      optimizer::SimpleOptimizer::BuildPelotonPlanTree(insert_stmt));

  result_format =
      std::move(std::vector<int>(statement->GetTupleDescriptor().size(), 0));
  status = bridge::PlanExecutor::ExecutePlan(statement->GetPlanTree().get(),
                                             params, result, result_format);
  LOG_INFO("Statement executed. Result: %d", status.m_result);
  LOG_INFO("Tuple inserted!");
  txn_manager.CommitTransaction(txn);
  target_table_ = catalog::Catalog::GetInstance()->GetTableWithName(
      DEFAULT_DB_NAME, "table_b");
  // Expected 1 , Primary key index + created index
  EXPECT_EQ(target_table_->GetTupleCount(), 1);
  std::cout<<"After first insertion in table_b\n\n\n";

  std::cout<<"end of setup\n";
  txn = txn_manager.BeginTransaction();
  LOG_INFO("Inserting a tuple...");
  LOG_INFO(
      "Query: SELECT * FROM table_a INNER JOIN table_b"
      "ON table_a.value = table_b.value;");
  statement.reset(new Statement("SELECT",
                                "SELECT * FROM table_a INNER JOIN table_b"
                                "ON table_a.value = table_b.value;"));
  auto select_stmt = peloton_parser.BuildParseTree(
      "SELECT * FROM table_a INNER JOIN table_b ON table_a.value = table_b.value;");
  // statement->SetPlanTree(
  //     optimizer::SimpleOptimizer::BuildPelotonPlanTree(select_stmt));
  
  std::cout<<select_stmt->GetStatements().size()<<std::endl;

  auto test_select_plan =
      optimizer::SimpleOptimizer::CreateJoinPlan((parser::SelectStatement*)(select_stmt->GetStatements().at(0)));

  txn_manager.CommitTransaction(txn);

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

} /* namespace test */
} /* namespace peloton */
