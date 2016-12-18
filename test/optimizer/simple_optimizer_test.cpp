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

  optimizer::SimpleOptimizer optimizer;

  // Create a table first
  auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto txn = txn_manager.BeginTransaction();
  LOG_INFO("Creating table");
  LOG_INFO(
      "Query: CREATE TABLE department_table(dept_id INT PRIMARY KEY,student_id "
      "INT, dept_name TEXT);");
  std::unique_ptr<Statement> statement;
  statement.reset(new Statement("CREATE",
                                "CREATE TABLE department_table(dept_id INT "
                                "PRIMARY KEY, student_id INT, dept_name "
                                "TEXT);"));

  auto& peloton_parser = parser::Parser::GetInstance();

  auto create_stmt = peloton_parser.BuildParseTree(
      "CREATE TABLE department_table(dept_id INT PRIMARY KEY, student_id INT, "
      "dept_name TEXT);");

  statement->SetPlanTree(optimizer.BuildPelotonPlanTree(create_stmt));

  std::vector<type::Value> params;
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

  // Inserting a tuple end-to-end
  txn = txn_manager.BeginTransaction();
  LOG_INFO("Inserting a tuple...");
  LOG_INFO(
      "Query: INSERT INTO department_table(dept_id,student_id ,dept_name) "
      "VALUES (1,52,'hello_1');");
  statement.reset(new Statement("INSERT",
                                "INSERT INTO department_table(dept_id, "
                                "student_id, dept_name) VALUES "
                                "(1,52,'hello_1');"));

  auto insert_stmt = peloton_parser.BuildParseTree(
      "INSERT INTO department_table(dept_id,student_id,dept_name) VALUES "
      "(1,52,'hello_1');");

  statement->SetPlanTree(optimizer.BuildPelotonPlanTree(insert_stmt));

  result_format =
      std::move(std::vector<int>(statement->GetTupleDescriptor().size(), 0));
  status = bridge::PlanExecutor::ExecutePlan(statement->GetPlanTree().get(),
                                             params, result, result_format);
  LOG_INFO("Statement executed. Result: %d", status.m_result);
  LOG_INFO("Tuple inserted!");
  txn_manager.CommitTransaction(txn);

  // Now Create index
  txn = txn_manager.BeginTransaction();
  LOG_INFO("Creating and Index");
  LOG_INFO("Query: CREATE INDEX saif ON department_table (student_id);");
  statement.reset(new Statement(
      "CREATE", "CREATE INDEX saif ON department_table (student_id);"));

  auto update_stmt = peloton_parser.BuildParseTree(
      "CREATE INDEX saif ON department_table (student_id);");

  statement->SetPlanTree(optimizer.BuildPelotonPlanTree(update_stmt));

  result_format =
      std::move(std::vector<int>(statement->GetTupleDescriptor().size(), 0));
  status = bridge::PlanExecutor::ExecutePlan(statement->GetPlanTree().get(),
                                             params, result, result_format);
  LOG_INFO("Statement executed. Result: %d", status.m_result);
  LOG_INFO("INDEX CREATED!");
  txn_manager.CommitTransaction(txn);

  auto target_table_ = catalog::Catalog::GetInstance()->GetTableWithName(
      DEFAULT_DB_NAME, "department_table");
  // Expected 1 , Primary key index + created index
  EXPECT_EQ(target_table_->GetIndexCount(), 2);

  // Test update tuple with index scan
  LOG_INFO("Updating a tuple...");
  LOG_INFO(
      "Query: UPDATE department_table SET dept_name = 'CS' WHERE student_id = "
      "52");
  update_stmt = peloton_parser.BuildParseTree(
      "UPDATE department_table SET dept_name = 'CS' WHERE student_id = 52");
  auto update_plan = optimizer.BuildPelotonPlanTree(update_stmt);

  // Check scan plan
  ASSERT_FALSE(update_plan == nullptr);
  EXPECT_EQ(update_plan->GetPlanNodeType(), PLAN_NODE_TYPE_UPDATE);
  auto& update_scan_plan = update_plan->GetChildren().front();
  EXPECT_EQ(update_scan_plan->GetPlanNodeType(), PLAN_NODE_TYPE_INDEXSCAN);

  update_stmt = peloton_parser.BuildParseTree(
      "UPDATE department_table SET dept_name = 'CS' WHERE dept_name = 'CS'");
  update_plan = optimizer.BuildPelotonPlanTree(update_stmt);
  EXPECT_EQ(update_plan->GetChildren().front()->GetPlanNodeType(),
            PLAN_NODE_TYPE_SEQSCAN);

  // Test delete tuple with index scan
  LOG_INFO("Deleting a tuple...");
  LOG_INFO("Query: DELETE FROM department_table WHERE student_id = 52");
  auto delete_stmt = peloton_parser.BuildParseTree(
      "DELETE FROM department_table WHERE student_id = 52");
  auto del_plan = optimizer.BuildPelotonPlanTree(delete_stmt);

  // Check scan plan
  EXPECT_EQ(del_plan->GetPlanNodeType(), PLAN_NODE_TYPE_DELETE);
  auto& del_scan_plan = del_plan->GetChildren().front();
  EXPECT_EQ(del_scan_plan->GetPlanNodeType(), PLAN_NODE_TYPE_INDEXSCAN);
  del_plan = nullptr;

  // Test delete tuple with seq scan
  auto delete_stmt_seq = peloton_parser.BuildParseTree(
      "DELETE FROM department_table WHERE dept_name = 'CS'");
  auto del_plan_seq = optimizer.BuildPelotonPlanTree(delete_stmt_seq);
  auto& del_scan_plan_seq = del_plan_seq->GetChildren().front();
  EXPECT_EQ(del_scan_plan_seq->GetPlanNodeType(), PLAN_NODE_TYPE_SEQSCAN);

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

} /* namespace test */
} /* namespace peloton */
