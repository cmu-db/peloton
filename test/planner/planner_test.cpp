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

#include "catalog/catalog.h"
#include "common/harness.h"
#include "executor/plan_executor.h"
#include "expression/abstract_expression.h"
#include "expression/comparison_expression.h"
#include "expression/operator_expression.h"
#include "expression/parameter_value_expression.h"
#include "parser/statements.h"
#include "planner/delete_plan.h"
#include "planner/plan_util.h"
#include "planner/update_plan.h"
#include "planner/seq_scan_plan.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Planner Tests
//===--------------------------------------------------------------------===//

class PlannerTests : public PelotonTest {};

TEST_F(PlannerTests, DeletePlanTestParameter) {
  // Bootstrapping peloton
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  // Create table
  txn = txn_manager.BeginTransaction();
  auto id_column =
      catalog::Column(type::TypeId::INTEGER,
                      type::Type::GetTypeSize(type::TypeId::INTEGER), "id", true);
  auto name_column = catalog::Column(type::TypeId::VARCHAR, 32, "name", true);

  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_column, name_column}));
  catalog::Catalog::GetInstance()->CreateTable(
      DEFAULT_DB_NAME, "department_table", std::move(table_schema), txn);
  txn_manager.CommitTransaction(txn);

  // DELETE FROM department_table WHERE id = $0

  // id = $0
  txn = txn_manager.BeginTransaction();
  auto parameter_expr = new expression::ParameterValueExpression(0);
  auto tuple_expr =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
  auto cmp_expr = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, tuple_expr, parameter_expr);

  auto target_table = catalog::Catalog::GetInstance()->GetTableWithName(
      DEFAULT_DB_NAME, "department_table", txn);

  // Create delete plan
  planner::DeletePlan *delete_plan =
      new planner::DeletePlan(target_table, cmp_expr);

  // Create sequential scan plan
  expression::AbstractExpression *scan_expr =
      (delete_plan->GetPredicate() == nullptr
           ? nullptr
           : delete_plan->GetPredicate()->Copy());
  LOG_TRACE("Creating a sequential scan plan");
  std::unique_ptr<planner::SeqScanPlan> seq_scan_node(
      new planner::SeqScanPlan(target_table, scan_expr, {}));
  LOG_INFO("Sequential scan plan created");

  // Add seq scan plan
  delete_plan->AddChild(std::move(seq_scan_node));

  LOG_INFO("Plan created:\n%s",
           planner::PlanUtil::GetInfo(delete_plan).c_str());

  auto values = new std::vector<type::Value>();

  // id = 15
  LOG_INFO("Binding values");
  values->push_back(type::ValueFactory::GetIntegerValue(15).Copy());

  // bind values to parameters in plan
  delete_plan->SetParameterValues(values);

  // free the database just created
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  delete values;
  delete cmp_expr;
  delete delete_plan;
}

TEST_F(PlannerTests, UpdatePlanTestParameter) {
  // Bootstrapping peloton
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  // Create table
  txn = txn_manager.BeginTransaction();
  auto id_column =
      catalog::Column(type::TypeId::INTEGER,
                      type::Type::GetTypeSize(type::TypeId::INTEGER), "id", true);
  auto name_column = catalog::Column(type::TypeId::VARCHAR, 32, "name", true);

  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_column, name_column}));
  catalog::Catalog::GetInstance()->CreateTable(
      DEFAULT_DB_NAME, "department_table", std::move(table_schema), txn);
  txn_manager.CommitTransaction(txn);

  // UPDATE department_table SET name = $0 WHERE id = $1
  txn = txn_manager.BeginTransaction();
  parser::UpdateStatement *update_statement = new parser::UpdateStatement();
  parser::TableRef *table_ref =
      new parser::TableRef(peloton::TableReferenceType::JOIN);

  auto name = new char[strlen("department_table") + 1]();
  strcpy(name, "department_table");
  auto table_info = new parser::TableInfo();
  table_info->table_name = name;
  table_ref->table_info_ = table_info;
  update_statement->table = table_ref;
  // Value val =
  //    type::ValueFactory::GetNullValue();  // The value is not important
  // at this point

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
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
  auto cmp_expr = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, tuple_expr, parameter_expr);

  update_statement->where = cmp_expr;

  auto update_plan = new planner::UpdatePlan(update_statement);
  LOG_INFO("Plan created:\n%s", update_plan->GetInfo().c_str());

  auto values = new std::vector<type::Value>();

  // name = CS, id = 1
  LOG_INFO("Binding values");
  values->push_back(type::ValueFactory::GetVarcharValue("CS").Copy());
  values->push_back(type::ValueFactory::GetIntegerValue(1).Copy());

  // bind values to parameters in plan
  update_plan->SetParameterValues(values);
  txn_manager.CommitTransaction(txn);

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  delete values;
  delete update_statement;
  delete update_plan;
}

TEST_F(PlannerTests, InsertPlanTestParameter) {
  // Bootstrapping peloton
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  // Create table
  txn = txn_manager.BeginTransaction();
  auto id_column =
      catalog::Column(type::TypeId::INTEGER,
                      type::Type::GetTypeSize(type::TypeId::INTEGER), "id", true);
  auto name_column = catalog::Column(type::TypeId::VARCHAR, 32, "name", true);

  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_column, name_column}));
  auto ret = catalog::Catalog::GetInstance()->CreateTable(
      DEFAULT_DB_NAME, "department_table", std::move(table_schema), txn);
  if (ret != ResultType::SUCCESS) LOG_TRACE("create table failed");
  txn_manager.CommitTransaction(txn);

  // INSERT INTO department_table VALUES ($0, $1)
  txn = txn_manager.BeginTransaction();
  auto insert_statement =
      new parser::InsertStatement(peloton::InsertType::VALUES);

  auto name = new char[strlen("department_table") + 1]();
  strcpy(name, "department_table");
  auto table_ref = new parser::TableRef(TableReferenceType::NAME);
  table_ref->table_info_ = new parser::TableInfo();
  table_ref->table_info_->table_name = name;
  insert_statement->table_ref_ = table_ref;
  std::vector<char *> *columns = NULL;  // will not be used
  insert_statement->columns = columns;

  // Value val =
  //    type::ValueFactory::GetNullValue();  // The value is not important
  // at  this point
  auto parameter_expr_1 = new expression::ParameterValueExpression(0);
  auto parameter_expr_2 = new expression::ParameterValueExpression(1);
  auto parameter_exprs = new std::vector<expression::AbstractExpression *>();
  parameter_exprs->push_back(parameter_expr_1);
  parameter_exprs->push_back(parameter_expr_2);
  insert_statement->insert_values = new std::vector<
      std::vector<peloton::expression::AbstractExpression *> *>();
  insert_statement->insert_values->push_back(parameter_exprs);

  auto target_table = catalog::Catalog::GetInstance()->GetTableWithName(
      DEFAULT_DB_NAME, "department_table", txn);

  planner::InsertPlan *insert_plan = new planner::InsertPlan(
      target_table, insert_statement->columns, insert_statement->insert_values);
  LOG_INFO("Plan created:\n%s", insert_plan->GetInfo().c_str());

  // VALUES(1, "CS")
  LOG_INFO("Binding values");
  auto values = new std::vector<type::Value>();
  values->push_back(type::ValueFactory::GetIntegerValue(1).Copy());
  values->push_back(type::ValueFactory::GetVarcharValue(
                        (std::string) "CS",
                        TestingHarness::GetInstance().GetTestingPool()).Copy());
  LOG_INFO("Value 1: %s", values->at(0).GetInfo().c_str());
  LOG_INFO("Value 2: %s", values->at(1).GetInfo().c_str());
  // bind values to parameters in plan
  insert_plan->SetParameterValues(values);
  txn_manager.CommitTransaction(txn);

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  delete values;
  delete insert_plan;
  delete insert_statement;
}

TEST_F(PlannerTests, InsertPlanTestParameterColumns) {
  // Bootstrapping peloton
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  // Create table
  txn = txn_manager.BeginTransaction();
  auto id_column =
      catalog::Column(type::TypeId::INTEGER,
                      type::Type::GetTypeSize(type::TypeId::INTEGER), "id", true);
  auto name_column = catalog::Column(type::TypeId::VARCHAR, 32, "name", true);

  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_column, name_column}));
  catalog::Catalog::GetInstance()->CreateTable(
      DEFAULT_DB_NAME, "department_table", std::move(table_schema), txn);
  txn_manager.CommitTransaction(txn);

  // INSERT INTO department_table VALUES (1, $1)
  txn = txn_manager.BeginTransaction();
  auto insert_statement =
      new parser::InsertStatement(peloton::InsertType::VALUES);

  auto name = new char[strlen("department_table") + 1]();
  strcpy(name, "department_table");
  auto table_ref = new parser::TableRef(TableReferenceType::NAME);
  table_ref->table_info_ = new parser::TableInfo();
  table_ref->table_info_->table_name = name;

  auto id_col = new char[strlen("id") + 1], name_col = new char[strlen("name") + 1];
  strcpy(id_col, "id");
  strcpy(name_col, "name");
  insert_statement->table_ref_ = table_ref;
  insert_statement->columns = new std::vector<char *>{id_col, name_col};

  // Value val =
  //    type::ValueFactory::GetNullValue();  // The value is not important
  // at  this point
  auto constant_expr_1 = new expression::ConstantValueExpression(
    type::ValueFactory::GetIntegerValue(1).Copy());
  auto parameter_expr_2 = new expression::ParameterValueExpression(1);
  auto exprs = new std::vector<expression::AbstractExpression *>();
  exprs->push_back(constant_expr_1);
  exprs->push_back(parameter_expr_2);
  insert_statement->insert_values = new std::vector<
      std::vector<peloton::expression::AbstractExpression *> *>();
  insert_statement->insert_values->push_back(exprs);

  auto target_table = catalog::Catalog::GetInstance()->GetTableWithName(
      DEFAULT_DB_NAME, "department_table", txn);

  planner::InsertPlan *insert_plan = new planner::InsertPlan(
      target_table, insert_statement->columns, insert_statement->insert_values);
  LOG_INFO("Plan created:\n%s", insert_plan->GetInfo().c_str());

  // VALUES(1, "CS")
  LOG_INFO("Binding values");
  auto values = new std::vector<type::Value>();
  values->push_back(type::ValueFactory::GetVarcharValue(
      (std::string)"CS", TestingHarness::GetInstance().GetTestingPool()).Copy());
  LOG_INFO("Value 1: %s", values->at(0).GetInfo().c_str());
  // bind values to parameters in plan
  insert_plan->SetParameterValues(values);
  txn_manager.CommitTransaction(txn);

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  delete values;
  delete insert_plan;
  delete insert_statement;
}


}  // End test namespace
}  // End peloton namespace
