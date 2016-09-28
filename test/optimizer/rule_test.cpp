//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rule_test.cpp
//
// Identification: test/optimizer/rule_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "common/harness.h"

#define private public

#include "optimizer/optimizer.h"
#include "optimizer/rule.h"
#include "optimizer/rule_impls.h"
#include "optimizer/op_expression.h"
#include "optimizer/operators.h"

#include "catalog/catalog.h"
#include "common/logger.h"
#include "common/statement.h"
#include "executor/create_executor.h"
#include "executor/delete_executor.h"
#include "executor/insert_executor.h"
#include "executor/plan_executor.h"
#include "executor/update_executor.h"
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

class RuleTests : public PelotonTest {};

TEST_F(RuleTests, SimpleRuleApplyTest) {
  // Build op plan node to match rule
  auto left_get = std::make_shared<OpExpression>(LogicalGet::make(0, {}));
  auto right_get = std::make_shared<OpExpression>(LogicalGet::make(0, {}));
  auto val = common::BooleanValue(1);
  auto pred =
    std::make_shared<OpExpression>(ExprConstant::make(& val));
  auto join = std::make_shared<OpExpression>(LogicalInnerJoin::make());
  join->PushChild(left_get);
  join->PushChild(right_get);
  join->PushChild(pred);

  // Setup rule
  InnerJoinCommutativity rule;

  EXPECT_TRUE(rule.Check(join));

  std::vector<std::shared_ptr<OpExpression>> outputs;
  rule.Transform(join, outputs);
  EXPECT_EQ(outputs.size(), 1);
}

// Test whether update stament will use index scan plan
TEST_F(RuleTests, UpdateDelWithIndexScanTest) {
  LOG_INFO("Bootstrapping...");
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);
  LOG_INFO("Bootstrapping completed!");

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

  statement->SetPlanTree(
      optimizer::SimpleOptimizer::BuildPelotonPlanTree(insert_stmt));

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

  statement->SetPlanTree(
      optimizer::SimpleOptimizer::BuildPelotonPlanTree(update_stmt));

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
      "Query: UPDATE department_table SET dept_name = 'CS' WHERE student_id = 52");
  update_stmt = peloton_parser.BuildParseTree(
      "UPDATE department_table SET dept_name = 'CS' WHERE student_id = 52");
  auto update_plan = optimizer::SimpleOptimizer::BuildPelotonPlanTree(update_stmt);

  // Check scan plan
  ASSERT_FALSE(update_plan == nullptr);
  EXPECT_EQ(update_plan->GetPlanNodeType(), PLAN_NODE_TYPE_UPDATE);
  auto& update_scan_plan = update_plan->GetChildren().front();
  EXPECT_EQ(update_scan_plan->GetPlanNodeType(), PLAN_NODE_TYPE_INDEXSCAN);

  // Test delete tuple with index scan
  LOG_INFO("Deleting a tuple...");
  LOG_INFO("Query: DELETE FROM department_table WHERE student_id = 52");
  auto delete_stmt = peloton_parser.BuildParseTree(
      "DELETE FROM department_table WHERE student_id = 52");
  auto del_plan = optimizer::SimpleOptimizer::BuildPelotonPlanTree(delete_stmt);

  // Check scan plan
  EXPECT_EQ(del_plan->GetPlanNodeType(), PLAN_NODE_TYPE_DELETE);
  auto& del_scan_plan = del_plan->GetChildren().front();
  EXPECT_EQ(del_scan_plan->GetPlanNodeType(), PLAN_NODE_TYPE_INDEXSCAN);

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

} /* namespace test */
} /* namespace peloton */
