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

class OptimizerTests : public PelotonTest {};

// Test whether update stament will use index scan plan
// TODO: Split the tests into separate test cases.
TEST_F(OptimizerTests, UpdateDelWithIndexScanTest) {
  LOG_INFO("Bootstrapping...");
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);
  LOG_INFO("Bootstrapping completed!");

  // Create a table first
  auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto txn = txn_manager.BeginTransaction();
  LOG_INFO("Creating table");
  LOG_INFO(
      "Query: CREATE TABLE table_a(aid INT PRIMARY KEY,value INT);");
  std::unique_ptr<Statement> statement;
  statement.reset(new Statement("CREATE",
                                "CREATE TABLE table_a(aid INT PRIMARY KEY,value INT);"));

  auto& peloton_parser = parser::Parser::GetInstance();

  auto create_stmt = peloton_parser.BuildParseTree(
      "CREATE TABLE table_a(aid INT PRIMARY KEY,value INT);");

  statement->SetPlanTree(
      optimizer::SimpleOptimizer::BuildPelotonPlanTree(create_stmt));

  std::vector<common::Value> params;
  std::vector<ResultType> result;
  bridge::PlanExecutor::PrintPlan(statement->GetPlanTree().get(), "Plan");
  std::vector<int> result_format;
  result_format =
      std::move(std::vector<int>(statement->GetTupleDescriptor().size(), 0));
  bridge::peloton_status status = bridge::PlanExecutor::ExecutePlan(
      statement->GetPlanTree().get(), params, result, result_format);
  LOG_INFO("Statement executed. Result: %d", status.m_result);
  LOG_INFO("Table Created");
  txn_manager.CommitTransaction(txn);

  EXPECT_EQ(catalog::Catalog::GetInstance()
                ->GetDatabaseWithName(DEFAULT_DB_NAME)
                ->GetTableCount(),
            1);

  txn = txn_manager.BeginTransaction();
  LOG_INFO("Creating table");
  LOG_INFO(
      "Query: CREATE TABLE table_b(bid INT PRIMARY KEY,value INT);");
  statement.reset(new Statement("CREATE",
                                "CREATE TABLE table_b(bid INT PRIMARY KEY,value INT);"));

  create_stmt = peloton_parser.BuildParseTree(
      "CREATE TABLE table_b(bid INT PRIMARY KEY,value INT);");

  statement->SetPlanTree(
      optimizer::SimpleOptimizer::BuildPelotonPlanTree(create_stmt));

  bridge::PlanExecutor::PrintPlan(statement->GetPlanTree().get(), "Plan");
  result_format =
      std::move(std::vector<int>(statement->GetTupleDescriptor().size(), 0));
  status = bridge::PlanExecutor::ExecutePlan(
      statement->GetPlanTree().get(), params, result, result_format);
  LOG_INFO("Statement executed. Result: %d", status.m_result);
  LOG_INFO("Table Created");
  txn_manager.CommitTransaction(txn);

  EXPECT_EQ(catalog::Catalog::GetInstance()
                ->GetDatabaseWithName(DEFAULT_DB_NAME)
                ->GetTableCount(),
            2);

  // Inserting a tuple to table_a
  txn = txn_manager.BeginTransaction();
  LOG_INFO("Inserting a tuple...");
  LOG_INFO(
      "Query: INSERT INTO table_a(aid, value) VALUES (1,1);");
  statement.reset(new Statement("INSERT",
                                "INSERT INTO table_a(aid, value) VALUES (1, 1);"));

  auto insert_stmt = peloton_parser.BuildParseTree(
      "INSERT INTO table_a(aid, value) VALUES (1, 1);");

  statement->SetPlanTree(
      optimizer::SimpleOptimizer::BuildPelotonPlanTree(insert_stmt));

  result_format =
      std::move(std::vector<int>(statement->GetTupleDescriptor().size(), 0));
  status = bridge::PlanExecutor::ExecutePlan(statement->GetPlanTree().get(),
                                             params, result, result_format);
  LOG_INFO("Statement executed. Result: %d", status.m_result);
  LOG_INFO("Tuple inserted to table_a!");
  txn_manager.CommitTransaction(txn);

  // Inserting a tuple to table_b
  txn = txn_manager.BeginTransaction();
  LOG_INFO("Inserting a tuple...");
  LOG_INFO(
      "Query: INSERT INTO table_b(bid, value) VALUES (1,2);");
  statement.reset(new Statement("INSERT",
                                "INSERT INTO table_b(bid, value) VALUES (1, 2);"));

  insert_stmt = peloton_parser.BuildParseTree(
      "INSERT INTO table_b(bid, value) VALUES (1, 2);");

  statement->SetPlanTree(
      optimizer::SimpleOptimizer::BuildPelotonPlanTree(insert_stmt));

  result_format =
      std::move(std::vector<int>(statement->GetTupleDescriptor().size(), 0));
  status = bridge::PlanExecutor::ExecutePlan(statement->GetPlanTree().get(),
                                             params, result, result_format);
  LOG_INFO("Statement executed. Result: %d", status.m_result);
  LOG_INFO("Tuple inserted to table_b!");
  txn_manager.CommitTransaction(txn);

  // Now Create index
  // txn = txn_manager.BeginTransaction();
  // LOG_INFO("Creating and Index");
  // LOG_INFO("Query: CREATE INDEX saif ON department_table (student_id);");
  // statement.reset(new Statement(
  //     "CREATE", "CREATE INDEX saif ON department_table (student_id);"));

  // auto update_stmt = peloton_parser.BuildParseTree(
  //     "CREATE INDEX saif ON department_table (student_id);");

  // statement->SetPlanTree(
  //     optimizer::SimpleOptimizer::BuildPelotonPlanTree(update_stmt));

  // result_format =
  //     std::move(std::vector<int>(statement->GetTupleDescriptor().size(), 0));
  // status = bridge::PlanExecutor::ExecutePlan(statement->GetPlanTree().get(),
  //                                            params, result, result_format);
  // LOG_INFO("Statement executed. Result: %d", status.m_result);
  // LOG_INFO("INDEX CREATED!");
  // txn_manager.CommitTransaction(txn);

  // // Test update tuple with index scan
  // LOG_INFO("Updating a tuple...");
  // LOG_INFO(
  //     "Query: UPDATE department_table SET dept_name = 'CS' WHERE student_id = "
  //     "52");
  // update_stmt = peloton_parser.BuildParseTree(
  //     "UPDATE department_table SET dept_name = 'CS' WHERE student_id = 52");
  // auto update_plan =
  //     optimizer::SimpleOptimizer::BuildPelotonPlanTree(update_stmt);

  // // Check scan plan
  // ASSERT_FALSE(update_plan == nullptr);
  // EXPECT_EQ(update_plan->GetPlanNodeType(), PLAN_NODE_TYPE_UPDATE);
  // auto& update_scan_plan = update_plan->GetChildren().front();
  // EXPECT_EQ(update_scan_plan->GetPlanNodeType(), PLAN_NODE_TYPE_INDEXSCAN);

  // update_stmt = peloton_parser.BuildParseTree(
  //     "UPDATE department_table SET dept_name = 'CS' WHERE dept_name = 'CS'");
  // update_plan =
  //     optimizer::SimpleOptimizer::BuildPelotonPlanTree(update_stmt);
  // EXPECT_EQ(update_plan->GetChildren().front()->GetPlanNodeType(), PLAN_NODE_TYPE_SEQSCAN);

  // // Test delete tuple with index scan
  // LOG_INFO("Deleting a tuple...");
  // LOG_INFO("Query: DELETE FROM department_table WHERE student_id = 52");
  // auto delete_stmt = peloton_parser.BuildParseTree(
  //     "DELETE FROM department_table WHERE student_id = 52");
  // auto del_plan = optimizer::SimpleOptimizer::BuildPelotonPlanTree(delete_stmt);

  // // Check scan plan
  // EXPECT_EQ(del_plan->GetPlanNodeType(), PLAN_NODE_TYPE_DELETE);
  // auto& del_scan_plan = del_plan->GetChildren().front();
  // EXPECT_EQ(del_scan_plan->GetPlanNodeType(), PLAN_NODE_TYPE_INDEXSCAN);
  // del_plan = nullptr;

  // // Test delete tuple with seq scan
  // auto delete_stmt_seq = peloton_parser.BuildParseTree(
  //     "DELETE FROM department_table WHERE dept_name = 'CS'");
  // auto del_plan_seq =
  //     optimizer::SimpleOptimizer::BuildPelotonPlanTree(delete_stmt_seq);
  // auto& del_scan_plan_seq = del_plan_seq->GetChildren().front();
  // EXPECT_EQ(del_scan_plan_seq->GetPlanNodeType(), PLAN_NODE_TYPE_SEQSCAN);
  // Perform the join
  // txn = txn_manager.BeginTransaction();
  // LOG_INFO("Select * ...");
  // LOG_INFO(
  //     "Query: SELECT id FROM table_a WHERE id = 1;");
  // statement.reset(new Statement("SELECT",
  //                               "SELECT id FROM table_a WHERE id = 1;"));

  // auto select_stmt = peloton_parser.BuildParseTree(
  //     "SELECT id FROM table_a WHERE id = 1;");

  // statement->SetPlanTree(
  //     optimizer::SimpleOptimizer::BuildPelotonPlanTree(select_stmt));

  // result_format =
  //     std::move(std::vector<int>(statement->GetTupleDescriptor().size(), 0));
  // status = bridge::PlanExecutor::ExecutePlan(statement->GetPlanTree().get(),
  //                                            params, result, result_format);
  // LOG_INFO("Statement executed. Result: %d", status.m_result);
  // LOG_INFO("Select * completed!");
  // txn_manager.CommitTransaction(txn);

  // LOG_INFO("Before Join\n\n\n\n\n\n\n\n\n\n\n");

  txn = txn_manager.BeginTransaction();
  LOG_INFO("Join ...");
  LOG_INFO(
      "Query: SELECT * FROM table_a INNER JOIN table_b ON aid = bid;");
  statement.reset(new Statement("SELECT",
                                "SELECT * FROM table_a INNER JOIN table_b ON aid = bid;"));

  auto select_stmt = peloton_parser.BuildParseTree(
      "SELECT * FROM table_a INNER JOIN table_b ON aid = bid;");

  statement->SetPlanTree(
      optimizer::SimpleOptimizer::BuildPelotonPlanTree(select_stmt));

  result_format =
      std::move(std::vector<int>(4, 0));
  status = bridge::PlanExecutor::ExecutePlan(statement->GetPlanTree().get(),
                                             params, result, result_format);
  LOG_INFO("Statement executed. Result: %d", status.m_result);
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
