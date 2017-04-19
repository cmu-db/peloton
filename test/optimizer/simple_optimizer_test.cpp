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
#include "parser/postgresparser.h"
#include "planner/create_plan.h"
#include "planner/delete_plan.h"
#include "planner/insert_plan.h"
#include "planner/plan_util.h"
#include "planner/update_plan.h"
#include "tcop/tcop.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Binding Tests
//===--------------------------------------------------------------------===//

using namespace optimizer;

class SimpleOptimizerTests : public PelotonTest {};

// Test whether update stament will use index scan plan
// TODO: Split the tests into separate test cases.
TEST_F(SimpleOptimizerTests, UpdateDelWithIndexScanTest) {
  LOG_TRACE("Bootstrapping...");
  auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  LOG_TRACE("Bootstrapping completed!");

  optimizer::SimpleOptimizer optimizer;
  auto& traffic_cop = tcop::TrafficCop::GetInstance();

  // Create a table first
  txn = txn_manager.BeginTransaction();
  LOG_TRACE("Creating table");
  LOG_TRACE(
      "Query: CREATE TABLE department_table(dept_id INT PRIMARY KEY,student_id "
      "INT, dept_name TEXT);");
  std::unique_ptr<Statement> statement;
  statement.reset(new Statement("CREATE",
                                "CREATE TABLE department_table(dept_id INT "
                                "PRIMARY KEY, student_id INT, dept_name "
                                "TEXT);"));

  auto& peloton_parser = parser::PostgresParser::GetInstance();

  auto create_stmt = peloton_parser.BuildParseTree(
      "CREATE TABLE department_table(dept_id INT PRIMARY KEY, student_id INT, "
      "dept_name TEXT);");

  statement->SetPlanTree(optimizer.BuildPelotonPlanTree(create_stmt));

  std::vector<type::Value> params;
  std::vector<StatementResult> result;
  LOG_TRACE("Query Plan:\n%s",
            planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());
  std::vector<int> result_format;
  result_format =
      std::move(std::vector<int>(statement->GetTupleDescriptor().size(), 0));
  executor::ExecuteResult status = traffic_cop.ExecuteStatementPlan(
      statement->GetPlanTree().get(), params, result, result_format);
  LOG_TRACE("Statement executed. Result: %s",
            ResultTypeToString(status.m_result).c_str());
  LOG_TRACE("Table Created");
  txn_manager.CommitTransaction(txn);

  EXPECT_EQ(catalog::Catalog::GetInstance()
                ->GetDatabaseWithName(DEFAULT_DB_NAME)
                ->GetTableCount(),
            1);

  // Inserting a tuple end-to-end
  txn = txn_manager.BeginTransaction();
  LOG_TRACE("Inserting a tuple...");
  LOG_TRACE(
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
  status = traffic_cop.ExecuteStatementPlan(statement->GetPlanTree().get(),
                                            params, result, result_format);
  LOG_TRACE("Statement executed. Result: %s",
            ResultTypeToString(status.m_result).c_str());
  LOG_TRACE("Tuple inserted!");
  txn_manager.CommitTransaction(txn);

  // Now Create index
  txn = txn_manager.BeginTransaction();
  LOG_TRACE("Creating and Index");
  LOG_TRACE("Query: CREATE INDEX saif ON department_table (student_id);");
  statement.reset(new Statement(
      "CREATE", "CREATE INDEX saif ON department_table (student_id);"));

  auto update_stmt = peloton_parser.BuildParseTree(
      "CREATE INDEX saif ON department_table (student_id);");

  statement->SetPlanTree(optimizer.BuildPelotonPlanTree(update_stmt));

  result_format =
      std::move(std::vector<int>(statement->GetTupleDescriptor().size(), 0));
  status = traffic_cop.ExecuteStatementPlan(statement->GetPlanTree().get(),
                                            params, result, result_format);
  LOG_TRACE("Statement executed. Result: %s",
            ResultTypeToString(status.m_result).c_str());
  LOG_TRACE("INDEX CREATED!");
  txn_manager.CommitTransaction(txn);

  auto target_table_ = catalog::Catalog::GetInstance()->GetTableWithName(
      DEFAULT_DB_NAME, "department_table");
  // Expected 1 , Primary key index + created index
  EXPECT_EQ(target_table_->GetIndexCount(), 2);

  // Test update tuple with index scan
  LOG_TRACE("Updating a tuple...");
  LOG_TRACE(
      "Query: UPDATE department_table SET dept_name = 'CS' WHERE student_id = "
      "52");
  update_stmt = peloton_parser.BuildParseTree(
      "UPDATE department_table SET dept_name = 'CS' WHERE student_id = 52");
  auto update_plan = optimizer.BuildPelotonPlanTree(update_stmt);

  // Check scan plan
  ASSERT_FALSE(update_plan == nullptr);
  EXPECT_EQ(update_plan->GetPlanNodeType(), PlanNodeType::UPDATE);
  auto& update_scan_plan = update_plan->GetChildren().front();
  EXPECT_EQ(update_scan_plan->GetPlanNodeType(), PlanNodeType::INDEXSCAN);

  update_stmt = peloton_parser.BuildParseTree(
      "UPDATE department_table SET dept_name = 'CS' WHERE dept_name = 'CS'");
  update_plan = optimizer.BuildPelotonPlanTree(update_stmt);
  EXPECT_EQ(update_plan->GetChildren().front()->GetPlanNodeType(),
            PlanNodeType::SEQSCAN);

  // Test delete tuple with index scan
  LOG_TRACE("Deleting a tuple...");
  LOG_TRACE("Query: DELETE FROM department_table WHERE student_id = 52");
  auto delete_stmt = peloton_parser.BuildParseTree(
      "DELETE FROM department_table WHERE student_id = 52");
  auto del_plan = optimizer.BuildPelotonPlanTree(delete_stmt);

  // Check scan plan
  EXPECT_EQ(del_plan->GetPlanNodeType(), PlanNodeType::DELETE);
  auto& del_scan_plan = del_plan->GetChildren().front();
  EXPECT_EQ(del_scan_plan->GetPlanNodeType(), PlanNodeType::INDEXSCAN);
  del_plan = nullptr;

  // Test delete tuple with seq scan
  auto delete_stmt_seq = peloton_parser.BuildParseTree(
      "DELETE FROM department_table WHERE dept_name = 'CS'");
  auto del_plan_seq = optimizer.BuildPelotonPlanTree(delete_stmt_seq);
  auto& del_scan_plan_seq = del_plan_seq->GetChildren().front();
  EXPECT_EQ(del_scan_plan_seq->GetPlanNodeType(), PlanNodeType::SEQSCAN);

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

} /* namespace test */
} /* namespace peloton */
