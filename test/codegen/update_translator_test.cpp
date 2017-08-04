//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// update_translator_test.cpp
//
// Identification: test/codegen/update_translator_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/testing_codegen_util.h"

#include "codegen/query_compiler.h"
#include "common/harness.h"
#include "common/statement.h"
#include "executor/create_executor.h"
#include "executor/update_executor.h"
#include "expression/expression_util.h"
#include "optimizer/optimizer.h"
#include "parser/postgresparser.h"
#include "parser/sql_statement.h"
#include "planner/abstract_plan.h"
#include "planner/create_plan.h"
#include "planner/seq_scan_plan.h"
#include "planner/plan_util.h"
#include "tcop/tcop.h"

namespace peloton {
namespace test {

class UpdateTranslatorTest : public PelotonCodeGenTest {
 public:
  UpdateTranslatorTest() : PelotonCodeGenTest() {}

  TableId TestTableId1() { return TableId::_1; }
  uint32_t NumRowsInTestTable() const { return num_rows_to_insert; }

 private:
  uint32_t num_rows_to_insert = 10;
};

TEST_F(UpdateTranslatorTest, UpdateColumnsWithAConstant) {
  LoadTestTable(TestTableId1(), NumRowsInTestTable());

  // SET a = 1;
  storage::DataTable *table = &this->GetTestTable(this->TestTableId1());
  LOG_DEBUG("Table has %zu tuples", table->GetTupleCount());
  LOG_INFO("%s", table->GetInfo().c_str());

  // Pre-condition
  EXPECT_EQ(NumRowsInTestTable(), table->GetTupleCount());

  // Get the scan plan without a predicate with four columns
  std::unique_ptr<planner::SeqScanPlan> scan_plan(new planner::SeqScanPlan(
      &GetTestTable(TestTableId1()), nullptr, {0, 1, 2, 3}));

  // Transform using a projection
  // Column 0 of the updated tuple will have constant value 1
  std::unique_ptr<const planner::ProjectInfo> project_info(
      new planner::ProjectInfo(
          // Target List : [(oid_t, planner::DerivedAttribute)]
          // Specify columns that are transformed.
          {{0, planner::DerivedAttribute{
                   expression::ExpressionUtil::ConstantValueFactory(
                       type::ValueFactory::GetIntegerValue(1))}}},
          // Direct Map List : [(oid_t, (oid_t, oid_t))]
          // Specify columns that are directly pulled from the original tuple.
          {{1, {0, 1}}, {2, {0, 2}}, {3, {0, 3}}}));

  // Embed the transformation to build up an update plan.
  std::unique_ptr<planner::UpdatePlan> update_plan(new planner::UpdatePlan(
      &GetTestTable(TestTableId1()), std::move(project_info)));

  // Add the scan to the update plan
  update_plan->AddChild(std::move(scan_plan));

  // Do binding
  planner::BindingContext context;
  update_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{}, context};

  // COMPILE and execute
  CompileAndExecute(*update_plan, buffer,
                    reinterpret_cast<char *>(buffer.GetState()));

  LOG_DEBUG("Table has %zu tuples", table->GetTupleCount());
  LOG_DEBUG("%s", table->GetInfo().c_str());

  // Post-condition: The table will have twice many, since it also has old ones
  EXPECT_EQ(NumRowsInTestTable()*2, table->GetTupleCount());
}

// fuction extracted from simple optimizer
// : Avoided using optimizer - codegen currently does not support IndexScan
std::shared_ptr<planner::AbstractPlan> BuildUpdatePlanTree(
    const std::unique_ptr<parser::SQLStatementList> &parse_tree) {
  std::shared_ptr<planner::AbstractPlan> plan_tree;

  PL_ASSERT(parse_tree->GetStatements().size() == 1);

  std::unique_ptr<planner::AbstractPlan> child_plan = nullptr;

  auto parse_tree2 = parse_tree->GetStatements().at(0);

  // column predicates passing to the index
  std::vector<oid_t> key_column_ids;
  std::vector<ExpressionType> expr_types;
  std::vector<type::Value> values;

  parser::UpdateStatement *updateStmt = (parser::UpdateStatement *)parse_tree2;
  auto target_table = catalog::Catalog::GetInstance()->GetTableWithName(
      updateStmt->table->GetDatabaseName(), updateStmt->table->GetTableName());

  auto update_clauses = updateStmt->updates;

  for (auto clause : *update_clauses) {
    auto clause_expr = clause->value;
    auto columnID = target_table->GetSchema()->GetColumnID(clause->column);
    auto column = target_table->GetSchema()->GetColumn(columnID);

    if (clause_expr->GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
      auto value =
          static_cast<const expression::ConstantValueExpression *>(clause_expr)
              ->GetValue()
              .CastAs(column.GetType());
      auto value_expression = new expression::ConstantValueExpression(value);

      delete clause->value;
      clause->value = value_expression;

    } else {
      for (unsigned int child_index = 0;
           child_index < clause_expr->GetChildrenSize(); child_index++) {
        auto child = clause_expr->GetChild(child_index);

        if (child->GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
          auto value =
              static_cast<const expression::ConstantValueExpression *>(child)
                  ->GetValue()
                  .CastAs(column.GetType());
          auto value_expression =
              new expression::ConstantValueExpression(value);

          clause_expr->SetChild(child_index, value_expression);
        }
      }
    }
  }

  // Create sequential scan plan
  std::unique_ptr<planner::AbstractPlan> child_UpdatePlan(
      new planner::UpdatePlan(updateStmt));
  child_plan = std::move(child_UpdatePlan);

  if (child_plan != nullptr) {
    if (plan_tree != nullptr) {
      plan_tree->AddChild(std::move(child_plan));
    } else {
      plan_tree = std::move(child_plan);
    }
  }

  return plan_tree;
}

TEST_F(UpdateTranslatorTest, UpdateComplicated) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn1 = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn1);
  txn_manager.CommitTransaction(txn1);

  // Bootstrapping
  auto &traffic_cop = tcop::TrafficCop::GetInstance();
  auto catalog = catalog::Catalog::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  traffic_cop.SetTcopTxnState(txn);

  optimizer::Optimizer optimizer;

  //  Create a table
  auto id_column = catalog::Column(type::TypeId::INTEGER,
      type::Type::GetTypeSize(type::TypeId::INTEGER), "dept_id", true);
  catalog::Constraint constraint(ConstraintType::PRIMARY, "con_primary");
  id_column.AddConstraint(constraint);
  auto manager_id_column = catalog::Column(type::TypeId::INTEGER,
      type::Type::GetTypeSize(type::TypeId::INTEGER), "manager_id", true);
  auto name_column = catalog::Column(type::TypeId::VARCHAR, 32, "dept_name",
                                     false);

  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_column, manager_id_column, name_column}));
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));
  planner::CreatePlan node("department_table", DEFAULT_DB_NAME,
                           std::move(table_schema), CreateType::TABLE);
  executor::CreateExecutor create_executor(&node, context.get());
  create_executor.Init();
  create_executor.Execute();
  traffic_cop.CommitQueryHelper();
  EXPECT_EQ(catalog->GetDatabaseWithName(DEFAULT_DB_NAME)->GetTableCount(), 1);

  storage::DataTable *table = catalog->GetTableWithName(DEFAULT_DB_NAME,
                                                        "department_table");

  //  Inserting a tuple end-to-end
  txn = txn_manager.BeginTransaction();
  traffic_cop.SetTcopTxnState(txn);
  std::string insert_query = 
      "INSERT INTO department_table(dept_id,manager_id,dept_name) "
      "VALUES (1,12,'hello_1');";
  std::unique_ptr<Statement> statement;
  statement.reset(new Statement("INSERT", insert_query));
  auto &peloton_parser = parser::PostgresParser::GetInstance();
  auto insert_stmt = peloton_parser.BuildParseTree(insert_query);
  statement->SetPlanTree(optimizer.BuildPelotonPlanTree(insert_stmt, txn));
  LOG_INFO("Executing plan...\n%s",
           planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());

  std::vector<type::Value> params;
  std::vector<StatementResult> result;
  std::vector<int> result_format =
      std::move(std::vector<int>(statement->GetTupleDescriptor().size(), 0));
  executor::ExecuteResult status = traffic_cop.ExecuteStatementPlan(
      statement->GetPlanTree(), params, result, result_format);
  LOG_INFO("Statement executed. Result: %s",
           ResultTypeToString(status.m_result).c_str());
  EXPECT_EQ(1, status.m_processed);

  traffic_cop.CommitQueryHelper();
  LOG_INFO("%s", table->GetInfo().c_str());

  //  Now Update a tuple
  std::string update_query_1 =
      "UPDATE department_table SET dept_name = 'CS' WHERE dept_id = 1";
  statement.reset(new Statement("UPDATE", update_query_1));
  auto update_stmt = peloton_parser.BuildParseTree(update_query_1);
  statement->SetPlanTree(BuildUpdatePlanTree(update_stmt));
  LOG_INFO("Executing plan...\n%s",
           planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());

  // Now embed the scan and the transformation to build up an update plan.
  auto &update_plan_1 =
      *reinterpret_cast<planner::UpdatePlan *>(statement->GetPlanTree().get());

  planner::BindingContext ctx_1;
  update_plan_1.PerformBinding(ctx_1);
  codegen::BufferingConsumer buffer_1{{}, ctx_1};

  CompileAndExecute(update_plan_1, buffer_1,
                    reinterpret_cast<char *>(buffer_1.GetState()));
  LOG_INFO("%s", table->GetInfo().c_str());

  //  Now Update another tuple
  std::string update_query_2 =
      "UPDATE department_table SET manager_id = manager_id + 1 WHERE "
      "dept_id = 1";
  statement.reset(new Statement("UPDATE", update_query_2));
  update_stmt = peloton_parser.BuildParseTree(update_query_2);
  statement->SetPlanTree(BuildUpdatePlanTree(update_stmt));
  LOG_INFO("Executing plan...\n%s",
           planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());

  // Now embed the scan and the transformation to build up an update plan.
  auto &update_plan_2 =
      *reinterpret_cast<planner::UpdatePlan *>(statement->GetPlanTree().get());

  planner::BindingContext ctx_2;
  update_plan_2.PerformBinding(ctx_2);
  codegen::BufferingConsumer buffer_2{{}, ctx_2};

  CompileAndExecute(update_plan_2, buffer_2,
                    reinterpret_cast<char *>(buffer_2.GetState()));
  LOG_INFO("%s", table->GetInfo().c_str());

  // Updating a primary key
  std::string update_query_3 =
      "UPDATE department_table SET dept_id = 2 WHERE dept_id = 1";
  statement.reset(new Statement("UPDATE", update_query_3));
  update_stmt = peloton_parser.BuildParseTree(update_query_3);
  statement->SetPlanTree(BuildUpdatePlanTree(update_stmt));
  LOG_INFO("Executing plan...\n%s",
           planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());

  // Now embed the scan and the transformation to build up an update plan.
  auto &update_plan_3 =
      *reinterpret_cast<planner::UpdatePlan *>(statement->GetPlanTree().get());

  planner::BindingContext ctx_3;
  update_plan_3.PerformBinding(ctx_3);
  codegen::BufferingConsumer buffer_3{{}, ctx_3};

  CompileAndExecute(update_plan_3, buffer_3,
                    reinterpret_cast<char *>(buffer_3.GetState()));
  LOG_INFO("%s", table->GetInfo().c_str());

  // Deleting now
  txn = txn_manager.BeginTransaction();
  traffic_cop.SetTcopTxnState(txn);
  std::string delete_query =
      "DELETE FROM department_table WHERE dept_id = 2;";
  statement.reset(new Statement("DELETE", delete_query));
  auto delete_stmt = peloton_parser.BuildParseTree(delete_query);
  statement->SetPlanTree(optimizer.BuildPelotonPlanTree(delete_stmt, txn));
  LOG_INFO("Executing plan...\n%s",
           planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());
  result_format =
      std::move(std::vector<int>(statement->GetTupleDescriptor().size(), 0));
  status = traffic_cop.ExecuteStatementPlan(statement->GetPlanTree(),
                                            params, result, result_format);
  LOG_INFO("Statement executed. Result: %s",
           ResultTypeToString(status.m_result).c_str());
  traffic_cop.CommitQueryHelper();
  LOG_INFO("%s", table->GetInfo().c_str());
  EXPECT_EQ(1, status.m_processed);

  txn = txn_manager.BeginTransaction();
  catalog->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
