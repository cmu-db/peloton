//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// planner_test.cpp
//
// Identification: test/planner/planner_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "catalog/catalog.h"
#include "expression/abstract_expression.h"
#include "expression/operator_expression.h"
#include "expression/comparison_expression.h"
#include "expression/parameter_value_expression.h"
#include "parser/statements.h"
#include "planner/delete_plan.h"
#include "planner/update_plan.h"
#include "executor/plan_executor.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Planner Tests
//===--------------------------------------------------------------------===//

class PlannerTests : public PelotonTest {};

TEST_F(PlannerTests, DeletePlanTestParameter) {

  // Bootstrapping peloton
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);

  // Create table
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  auto id_column = catalog::Column(common::Type::INTEGER,
                                   common::Type::GetTypeSize(common::Type::INTEGER), "id", true);
  auto name_column = catalog::Column(common::Type::VARCHAR, 32, "name", true);

  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_column, name_column}));
  catalog::Catalog::GetInstance()->CreateTable(
      DEFAULT_DB_NAME, "department_table", std::move(table_schema), txn);

  // DELETE FROM department_table WHERE id = $0
  parser::DeleteStatement *delete_statement = new parser::DeleteStatement();
  auto name = new char[strlen("department_table") + 1]();
  strcpy(name, "department_table");
  auto table_name = new expression::ParserExpression(EXPRESSION_TYPE_TABLE_REF,
                                                     name, nullptr);
  delete_statement->table_name = table_name;
  // Value val =
  //    common::ValueFactory::GetNullValue();  // The value is not important at
  // this point

  // id = $0
  auto parameter_expr = new expression::ParameterValueExpression(0);
  auto tuple_expr =
      new expression::TupleValueExpression(common::Type::INTEGER, 0, 0);
  auto cmp_expr =
      new expression::ComparisonExpression(
          EXPRESSION_TYPE_COMPARE_EQUAL, tuple_expr, parameter_expr);

  delete_statement->expr = cmp_expr;

  auto del_plan = new planner::DeletePlan(delete_statement);
  LOG_INFO("Plan created");
  bridge::PlanExecutor::PrintPlan(del_plan, "Delete Plan");

  auto values = new std::vector<common::Value>();

  // id = 15
  LOG_INFO("Binding values");
  values->push_back(common::ValueFactory::GetIntegerValue(15).Copy());

  // bind values to parameters in plan
  del_plan->SetParameterValues(values);

  // free the database just created
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  delete values;
  delete del_plan;
  delete delete_statement;
}

TEST_F(PlannerTests, UpdatePlanTestParameter) {

  // Bootstrapping peloton
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);

  // Create table
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  auto id_column = catalog::Column(common::Type::INTEGER,
                                   common::Type::GetTypeSize(common::Type::INTEGER), "id", true);
  auto name_column = catalog::Column(common::Type::VARCHAR, 32, "name", true);

  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_column, name_column}));
  catalog::Catalog::GetInstance()->CreateTable(
      DEFAULT_DB_NAME, "department_table", std::move(table_schema), txn);

  // UPDATE department_table SET name = $0 WHERE id = $1
  parser::UpdateStatement *update_statement = new parser::UpdateStatement();
  parser::TableRef *table_ref =
      new parser::TableRef(peloton::TABLE_REFERENCE_TYPE_JOIN);

  auto name = new char[strlen("department_table") + 1]();
  strcpy(name, "department_table");
  auto table_name = new expression::ParserExpression(EXPRESSION_TYPE_TABLE_REF,
                                                     name, nullptr);
  table_ref->table_name = table_name;
  update_statement->table = table_ref;
  // Value val =
  //    common::ValueFactory::GetNullValue();  // The value is not important at
  // this point

  // name = $0
  auto update = new parser::UpdateClause();
  auto column = new char[5]();
  strcpy(column, "name");
  update->column = column;
  auto parameter_expr = new expression::ParameterValueExpression(0);
  update->value = parameter_expr;
  auto updates = new std::vector<parser::UpdateClause *>();
  updates->push_back(update);
  update_statement->updates = updates;

  // id = $1
  parameter_expr = new expression::ParameterValueExpression(1);
  auto tuple_expr =
      new expression::TupleValueExpression(common::Type::INTEGER, 0, 0);
  auto cmp_expr =
      new expression::ComparisonExpression(
          EXPRESSION_TYPE_COMPARE_EQUAL, tuple_expr, parameter_expr);

  update_statement->where = cmp_expr;

  auto update_plan = new planner::UpdatePlan(update_statement);
  LOG_INFO("Plan created");
  bridge::PlanExecutor::PrintPlan(update_plan, "Update Plan");

  auto values = new std::vector<common::Value>();

  // name = CS, id = 1
  LOG_INFO("Binding values");
  values->push_back(common::ValueFactory::GetVarcharValue("CS").Copy());
  values->push_back(common::ValueFactory::GetIntegerValue(1).Copy());

  // bind values to parameters in plan
  update_plan->SetParameterValues(values);

  // free the database just created
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  delete values;
  delete update_statement;
  delete update_plan;
}

TEST_F(PlannerTests, InsertPlanTestParameter) {
  // Bootstrapping peloton
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);

  // Create table
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  auto id_column = catalog::Column(common::Type::INTEGER,
                                   common::Type::GetTypeSize(common::Type::INTEGER), "id", true);
  auto name_column = catalog::Column(common::Type::VARCHAR, 32, "name", true);

  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_column, name_column}));
  catalog::Catalog::GetInstance()->CreateTable(
      DEFAULT_DB_NAME, "department_table", std::move(table_schema), txn);

  // INSERT INTO department_table VALUES ($0, $1)
  auto insert_statement =
      new parser::InsertStatement(peloton::INSERT_TYPE_VALUES);

  auto name = new char[strlen("department_table") + 1]();
  strcpy(name, "department_table");
  auto table_name = new expression::ParserExpression(EXPRESSION_TYPE_TABLE_REF,
                                                     name, nullptr);
  insert_statement->table_name = table_name;
  std::vector<char *> *columns = NULL;  // will not be used
  insert_statement->columns = columns;

  // Value val =
  //    common::ValueFactory::GetNullValue();  // The value is not important at
  // this point
  auto parameter_expr_1 = new expression::ParameterValueExpression(0);
  auto parameter_expr_2 = new expression::ParameterValueExpression(1);
  auto parameter_exprs = new std::vector<expression::AbstractExpression *>();
  parameter_exprs->push_back(parameter_expr_1);
  parameter_exprs->push_back(parameter_expr_2);
  insert_statement->insert_values = new std::vector<std::vector<peloton::expression::AbstractExpression*>*>();
  insert_statement->insert_values->push_back(parameter_exprs);

  auto insert_plan = new planner::InsertPlan(insert_statement);
  LOG_INFO("Plan created");
  bridge::PlanExecutor::PrintPlan(insert_plan, "Insert Plan");

  // VALUES(1, "CS")
  LOG_INFO("Binding values");
  auto values = new std::vector<common::Value>();
  values->push_back(common::ValueFactory::GetIntegerValue(1).Copy());
  values->push_back(common::ValueFactory::GetVarcharValue(
      "CS", TestingHarness::GetInstance().GetTestingPool()).Copy());
  LOG_INFO("Value 1: %s", values->at(0).GetInfo().c_str());
  LOG_INFO("Value 2: %s", values->at(1).GetInfo().c_str());
  // bind values to parameters in plan
  insert_plan->SetParameterValues(values);

  // free the database just created
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  delete values;
  delete insert_plan;
  delete insert_statement;
}

}  // End test namespace
}  // End peloton namespace
