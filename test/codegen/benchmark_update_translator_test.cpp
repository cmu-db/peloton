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

#include "expression/expression_util.h"
#include "executor/update_executor.h"
#include "codegen/query_compiler.h"
#include "common/harness.h"
#include "optimizer/simple_optimizer.h"
#include "tcop/tcop.h"
#include "planner/create_plan.h"
#include "executor/create_executor.h"
#include "common/statement.h"
#include "parser/postgresparser.h"
#include "planner/plan_util.h"
#include "planner/abstract_plan.h"
#include "parser/sql_statement.h"

#include "codegen/codegen_test_util.h"

namespace peloton {
namespace test {

//===----------------------------------------------------------------------===//
// This class contains code to test code generation and compilation of table
// scan query plans. All the tests use a single table created and loaded during
// SetUp().  The schema of the table is as follows:
//
// +---------+---------+---------+-------------+
// | A (int) | B (int) | C (int) | D (varchar) |
// +---------+---------+---------+-------------+
//
// The database and tables are created in CreateDatabase() and
// CreateTestTables(), respectively.
//
// By default, the table is loaded with 64 rows of random values.
//===----------------------------------------------------------------------===//

class UpdateTranslatorTest : public PelotonCodeGenTest {
 public:
  UpdateTranslatorTest() : PelotonCodeGenTest(), num_rows_to_insert(10) {
    // Load test table
    LoadTestTable(TestTableId(), num_rows_to_insert);
  }

  uint32_t NumRowsInTestTable() const { return num_rows_to_insert; }

  uint32_t TestTableId() { return test_table1_id; }

 private:
  uint32_t num_rows_to_insert;
};

/**
 * @brief This test case uses the interpreted executor to perform an update.
 * This is an example showing what an update plan looks like.
 */
TEST_F(UpdateTranslatorTest, ToConstUpdate) {
  // UPDATE table
  // SET a = 1;

  storage::DataTable *table = &this->GetTestTable(this->TestTableId());
  LOG_DEBUG("Table has %zu tuples", table->GetTupleCount());
  LOG_INFO("%s", table->GetInfo().c_str());

  // =============
  //  Create plan
  // =============

  // Each update query needs to first scan the table.
  std::unique_ptr<planner::SeqScanPlan> scan_plan(new planner::SeqScanPlan(
      &GetTestTable(TestTableId()), /* table */
      nullptr,                      /* predicate */
      {0, 1, 2, 3}                  /* columns */
  ));

  // Then it transforms each tuple scanned.
  // Weirdly, the transformation is represented by a "projection".
  std::unique_ptr<const planner::ProjectInfo> project_info(new planner::ProjectInfo(
      // target list : [(oid_t, planner::DerivedAttribute)]
      // These specify columns that are transformed.
      {
          // Column 0 of the updated tuple will have constant value 1
          {
              0,
              planner::DerivedAttribute{
                  // I haven't figured out what this should be
                  planner::AttributeInfo{},

                  expression::ExpressionUtil::ConstantValueFactory(
                      type::ValueFactory::GetIntegerValue(1)
                  )
              }
          }
      },

      // direct map list : [(oid_t, (oid_t, oid_t))]
      // These specify columns that are directly pulled from the original tuple.
      {
          { 1, { 0, 1 } },
          { 2, { 0, 2 } },
          { 3, { 0, 3 } },
      }
  ));

  // Now embed the scan and the transformation to build up an update plan.
  std::unique_ptr<planner::UpdatePlan> update_plan(new planner::UpdatePlan(
      &GetTestTable(TestTableId()), /* table */
      std::move(project_info)       /* projection info */
  ));

  update_plan->AddChild(std::move(scan_plan));

  // Do binding
  planner::BindingContext context;
  update_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{}, context};

  // COMPILE and execute
  CompileAndExecute(*update_plan, buffer, reinterpret_cast<char*>(buffer.GetState()));

  LOG_DEBUG("Table has %zu tuples", table->GetTupleCount());
  LOG_INFO("%s", table->GetInfo().c_str());
}

/*
 * fuction extracted from simple optimizer
 * Avoid using simple optimizer since codegen currently does not support
 * IndexScan
 */
std::shared_ptr<planner::AbstractPlan> BuildUpdatePlanTree(
    const std::unique_ptr<parser::SQLStatementList>& parse_tree) {
  std::shared_ptr<planner::AbstractPlan> plan_tree;

  PL_ASSERT(parse_tree->GetStatements().size() == 1);

  std::unique_ptr<planner::AbstractPlan> child_plan = nullptr;

  auto parse_tree2 = parse_tree->GetStatements().at(0);

  LOG_TRACE("Adding Update plan...");

  // column predicates passing to the index
  std::vector<oid_t> key_column_ids;
  std::vector<ExpressionType> expr_types;
  std::vector<type::Value> values;

  parser::UpdateStatement* updateStmt =
      (parser::UpdateStatement*)parse_tree2;
  auto target_table = catalog::Catalog::GetInstance()->GetTableWithName(
      updateStmt->table->GetDatabaseName(),
      updateStmt->table->GetTableName());

  auto update_clauses = updateStmt->updates;

  for (auto clause : *update_clauses) {
    auto clause_expr = clause->value;
    auto columnID = target_table->GetSchema()->GetColumnID(clause->column);
    auto column = target_table->GetSchema()->GetColumn(columnID);

    if (clause_expr->GetExpressionType() ==
        ExpressionType::VALUE_CONSTANT) {
      auto value = static_cast<const expression::ConstantValueExpression*>(
          clause_expr)
          ->GetValue()
          .CastAs(column.GetType());
      auto value_expression =
          new expression::ConstantValueExpression(value);

      delete clause->value;
      clause->value = value_expression;

    } else {
      for (unsigned int child_index = 0;
           child_index < clause_expr->GetChildrenSize(); child_index++) {
        auto child = clause_expr->GetChild(child_index);

        if (child->GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
          auto value =
              static_cast<const expression::ConstantValueExpression*>(child)
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

TEST_F(UpdateTranslatorTest, UpdatingOld) {
  LOG_INFO("Bootstrapping...");
  auto catalog = catalog::Catalog::GetInstance();
  auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog->CreateDatabase(DEFAULT_DB_NAME, txn);
  LOG_INFO("Bootstrapping completed!");

  optimizer::SimpleOptimizer optimizer;
  auto& traffic_cop = tcop::TrafficCop::GetInstance();

  // ======================
  //  Create a table first
  // ======================

  LOG_INFO("Creating a table...");
  auto id_column = catalog::Column(type::Type::INTEGER,
                                   type::Type::GetTypeSize(type::Type::INTEGER),
                                   "dept_id", true);
  catalog::Constraint constraint(ConstraintType::PRIMARY, "con_primary");
  id_column.AddConstraint(constraint);
  auto manager_id_column = catalog::Column(
      type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
      "manager_id", true);
  auto name_column =
      catalog::Column(type::Type::VARCHAR, 32, "dept_name", false);

  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_column, manager_id_column, name_column}));
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));
  planner::CreatePlan node("department_table", DEFAULT_DB_NAME,
                           std::move(table_schema), CreateType::TABLE);
  executor::CreateExecutor create_executor(&node, context.get());
  create_executor.Init();
  create_executor.Execute();
  txn_manager.CommitTransaction(txn);
  EXPECT_EQ(catalog->GetDatabaseWithName(DEFAULT_DB_NAME)->GetTableCount(), 1);

  LOG_INFO("Table created!");

  storage::DataTable* table =
      catalog->GetTableWithName(DEFAULT_DB_NAME, "department_table");

  // ==============================
  //  Inserting a tuple end-to-end
  // ==============================

  txn = txn_manager.BeginTransaction();
  LOG_INFO("Inserting a tuple...");
  LOG_INFO(
      "Query: INSERT INTO department_table(dept_id,manager_id,dept_name) "
          "VALUES (1,12,'hello_1');");
  std::unique_ptr<Statement> statement;
  statement.reset(new Statement("INSERT",
                                "INSERT INTO "
                                    "department_table(dept_id,manager_id,dept_name)"
                                    " VALUES (1,12,'hello_1');"));
  auto& peloton_parser = parser::PostgresParser::GetInstance();
  LOG_INFO("Building parse tree...");
  auto insert_stmt = peloton_parser.BuildParseTree(
      "INSERT INTO department_table(dept_id,manager_id,dept_name) VALUES "
          "(1,12,'hello_1');");
  LOG_INFO("Building parse tree completed!");
  LOG_INFO("Building plan tree...");
  statement->SetPlanTree(optimizer.BuildPelotonPlanTree(insert_stmt));
  LOG_INFO("Building plan tree completed!");
  std::vector<type::Value> params;
  std::vector<StatementResult> result;
  LOG_INFO("Executing plan...\n%s",
           planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());

  std::vector<int> result_format;
  result_format =
      std::move(std::vector<int>(statement->GetTupleDescriptor().size(), 0));
  executor::ExecuteResult status = traffic_cop.ExecuteStatementPlan(
      statement->GetPlanTree().get(), params, result, result_format);
  LOG_INFO("Statement executed. Result: %s",
           ResultTypeToString(status.m_result).c_str());
  LOG_INFO("Tuple inserted!");
  txn_manager.CommitTransaction(txn);

  LOG_INFO("%s", table->GetInfo().c_str());

  // =========================
  //  Now Updating end-to-end
  // =========================

  LOG_INFO("Updating a tuple...");
  LOG_INFO(
      "Query: UPDATE department_table SET dept_name = 'CS' WHERE dept_id = 1");
  statement.reset(new Statement(
      "UPDATE",
      "UPDATE department_table SET dept_name = 'CS' WHERE dept_id = 1"));
  LOG_INFO("Building parse tree...");
  auto update_stmt = peloton_parser.BuildParseTree(
      "UPDATE department_table SET dept_name = 'CS' WHERE dept_id = 1");
  LOG_INFO("Building parse tree completed!");
  LOG_INFO("Building plan tree...");
  statement->SetPlanTree(BuildUpdatePlanTree(update_stmt));
  LOG_INFO("Building plan tree completed!");
  LOG_INFO("Executing plan...\n%s",
           planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());
  // Now embed the scan and the transformation to build up an update plan.
  auto &update_plan_1 = *reinterpret_cast<planner::UpdatePlan *>(statement->GetPlanTree().get());
  // Do binding
  planner::BindingContext ctx_1;
  update_plan_1.PerformBinding(ctx_1);
  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer_1{{}, ctx_1};
  // COMPILE and execute
  CompileAndExecute(update_plan_1, buffer_1, reinterpret_cast<char*>(buffer_1.GetState()));
  LOG_INFO("Tuple Updated!");

  LOG_INFO("%s", table->GetInfo().c_str());

  LOG_INFO("Updating another tuple...");
  LOG_INFO(
      "Query: UPDATE department_table SET manager_id = manager_id + 1 WHERE "
          "dept_id = 1");
  statement.reset(new Statement("UPDATE",
                                "UPDATE department_table SET manager_id = "
                                    "manager_id + 1 WHERE dept_id = 1"));
  LOG_INFO("Building parse tree...");
  update_stmt = peloton_parser.BuildParseTree(
      "UPDATE department_table SET manager_id = manager_id + 1 WHERE dept_id = "
          "1");
  LOG_INFO("Building parse tree completed!");
  LOG_INFO("Building plan tree...");
  statement->SetPlanTree(BuildUpdatePlanTree(update_stmt));
  LOG_INFO("Building plan tree completed!");
  LOG_INFO("Executing plan...\n%s",
           planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());
  // Now embed the scan and the transformation to build up an update plan.
  auto &update_plan_2 = *reinterpret_cast<planner::UpdatePlan *>(statement->GetPlanTree().get());
  // Do binding
  planner::BindingContext ctx_2;
  update_plan_2.PerformBinding(ctx_2);
  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer_2{{}, ctx_2};
  // COMPILE and execute
  CompileAndExecute(update_plan_2, buffer_2, reinterpret_cast<char*>(buffer_2.GetState()));
  LOG_INFO("Tuple Updated!");

  LOG_INFO("%s", table->GetInfo().c_str());

  LOG_INFO("Updating primary key...");
  LOG_INFO("Query: UPDATE department_table SET dept_id = 2 WHERE dept_id = 1");
  statement.reset(new Statement(
      "UPDATE", "UPDATE department_table SET dept_id = 2 WHERE dept_id = 1"));
  LOG_INFO("Building parse tree...");
  update_stmt = peloton_parser.BuildParseTree(
      "UPDATE department_table SET dept_id = 2 WHERE dept_id = 1");
  LOG_INFO("Building parse tree completed!");
  LOG_INFO("Building plan tree...");
  statement->SetPlanTree(BuildUpdatePlanTree(update_stmt));
  LOG_INFO("Building plan tree completed!");
  LOG_INFO("Executing plan...\n%s",
           planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());
  // Now embed the scan and the transformation to build up an update plan.
  auto &update_plan_3 = *reinterpret_cast<planner::UpdatePlan *>(statement->GetPlanTree().get());
  // Do binding
  planner::BindingContext ctx_3;
  update_plan_3.PerformBinding(ctx_3);
  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer_3{{}, ctx_3};
  // COMPILE and execute
  CompileAndExecute(update_plan_3, buffer_3, reinterpret_cast<char*>(buffer_3.GetState()));
  LOG_INFO("Tuple Updated!");

  LOG_INFO("%s", table->GetInfo().c_str());

  // Deleting now
  txn = txn_manager.BeginTransaction();
  LOG_INFO("Deleting a tuple...");
  LOG_INFO("Query: DELETE FROM department_table WHERE dept_name = 'CS'");
  statement.reset(new Statement(
      "DELETE", "DELETE FROM department_table WHERE dept_name = 'CS'"));
  LOG_INFO("Building parse tree...");
  auto delete_stmt = peloton_parser.BuildParseTree(
      "DELETE FROM department_table WHERE dept_name = 'CS'");
  LOG_INFO("Building parse tree completed!");
  LOG_INFO("Building plan tree...");
  statement->SetPlanTree(optimizer.BuildPelotonPlanTree(delete_stmt));
  LOG_INFO("Building plan tree completed!");
  LOG_INFO("Executing plan...\n%s",
           planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());
  result_format =
      std::move(std::vector<int>(statement->GetTupleDescriptor().size(), 0));
  status = traffic_cop.ExecuteStatementPlan(statement->GetPlanTree().get(),
                                            params, result, result_format);
  LOG_INFO("Statement executed. Result: %s",
           ResultTypeToString(status.m_result).c_str());
  LOG_INFO("Tuple deleted!");
  txn_manager.CommitTransaction(txn);

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}
}  // namespace test
}  // namespace peloton
