//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// planner_test.cpp
//
// Identification: test/planner/planner_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/catalog.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/plan_executor.h"
#include "expression/comparison_expression.h"
#include "expression/operator_expression.h"
#include "expression/parameter_value_expression.h"
#include "expression/tuple_value_expression.h"
#include "expression/expression_util.h"
#include "parser/statements.h"
#include "parser/postgresparser.h"

#include "planner/create_plan.h"
#include "planner/delete_plan.h"
#include "planner/drop_plan.h"
#include "planner/attribute_info.h"
#include "planner/plan_util.h"
#include "planner/project_info.h"
#include "planner/seq_scan_plan.h"
#include "planner/update_plan.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Planner Test
//===--------------------------------------------------------------------===//

class PlannerTest : public PelotonTest {};

TEST_F(PlannerTest, CreateDatabasePlanTest) {
  auto& peloton_parser = parser::PostgresParser::GetInstance();
  auto parse_tree_list = peloton_parser.BuildParseTree("CREATE DATABASE pelotondb;");
  // There should be only one statement in the statement list
  EXPECT_EQ(1, parse_tree_list->GetNumStatements());
  auto &parse_tree = parse_tree_list->GetStatements().at(0);

  std::unique_ptr<planner::CreatePlan> create_DB_plan(
    new planner::CreatePlan((parser::CreateStatement *)parse_tree.get())
  );
  EXPECT_STREQ("pelotondb", create_DB_plan->GetDatabaseName().c_str());
  EXPECT_EQ(CreateType::DB, create_DB_plan->GetCreateType());
}

TEST_F(PlannerTest, DropDatabasePlanTest) {
  auto& peloton_parser = parser::PostgresParser::GetInstance();
  auto parse_tree_list = peloton_parser.BuildParseTree("DROP DATABASE pelotondb;");
  // There should be only one statement in the statement list
  EXPECT_EQ(1, parse_tree_list->GetNumStatements());
  auto &parse_tree = parse_tree_list->GetStatements().at(0);

  std::unique_ptr<planner::DropPlan> drop_DB_plan(
    new planner::DropPlan((parser::DropStatement *)parse_tree.get())
  );
  EXPECT_STREQ("pelotondb", drop_DB_plan->GetDatabaseName().c_str());
  EXPECT_EQ(DropType::DB, drop_DB_plan->GetDropType());
}
  
TEST_F(PlannerTest, DeletePlanTestParameter) {
  // Bootstrapping peloton
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  // Create table
  txn = txn_manager.BeginTransaction();
  auto id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "id", true);
  auto name_column = catalog::Column(type::TypeId::VARCHAR, 32, "name", true);

  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_column, name_column}));
  catalog::Catalog::GetInstance()->CreateTable(
      DEFAULT_DB_NAME, "department_table", std::move(table_schema), txn);
  txn_manager.CommitTransaction(txn);

  // DELETE FROM department_table WHERE id = $0

  // id = $0
  txn = txn_manager.BeginTransaction();
  auto *parameter_expr = new expression::ParameterValueExpression(0);
  auto *tuple_expr =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
  auto *scan_expr = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, tuple_expr, parameter_expr);

  auto target_table = catalog::Catalog::GetInstance()->GetTableWithName(
      DEFAULT_DB_NAME, "department_table", txn);

  // Create delete plan
  std::unique_ptr<planner::DeletePlan> delete_plan(
      new planner::DeletePlan(target_table));

  // Create sequential scan plan
  LOG_TRACE("Creating a sequential scan plan");
  std::unique_ptr<planner::SeqScanPlan> seq_scan_node(
      new planner::SeqScanPlan(target_table, scan_expr, {}));
  LOG_INFO("Sequential scan plan created");

  // Add seq scan plan
  delete_plan->AddChild(std::move(seq_scan_node));

  LOG_INFO("Plan created:\n%s",
           planner::PlanUtil::GetInfo(delete_plan.get()).c_str());

  std::vector<type::Value> values;

  // id = 15
  LOG_INFO("Binding values");
  values.push_back(type::ValueFactory::GetIntegerValue(15).Copy());

  // bind values to parameters in plan
  delete_plan->SetParameterValues(&values);

  // free the database just created
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(PlannerTest, UpdatePlanTestParameter) {
  // Bootstrapping peloton
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  // Create table
  txn = txn_manager.BeginTransaction();
  auto id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "id", true);
  auto name_column = catalog::Column(type::TypeId::VARCHAR, 32, "name", true);

  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_column, name_column}));
  catalog::Catalog::GetInstance()->CreateTable(
      DEFAULT_DB_NAME, "department_table", std::move(table_schema), txn);
  txn_manager.CommitTransaction(txn);

  // UPDATE department_table SET name = $0 WHERE id = $1
  txn = txn_manager.BeginTransaction();

  auto table_name = std::string("department_table");
  auto database_name = DEFAULT_DB_NAME;
  auto target_table = catalog::Catalog::GetInstance()->GetTableWithName(
      database_name, table_name, txn);
  auto schema = target_table->GetSchema();

  TargetList tlist;
  DirectMapList dmlist;
  oid_t col_id;
  std::vector<oid_t> column_ids;

  col_id = schema->GetColumnID(std::string("name"));
  column_ids.push_back(col_id);
  auto *update_expr = new expression::ParameterValueExpression(0);

  planner::DerivedAttribute attribute(update_expr);
  attribute.attribute_info.type = update_expr->ResultType();
  attribute.attribute_info.name = std::string("name");
  tlist.emplace_back(col_id, attribute);

  auto *parameter_expr = new expression::ParameterValueExpression(1);
  auto *tuple_expr =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
  auto *where_expr = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, tuple_expr, parameter_expr);

  auto &schema_columns = schema->GetColumns();
  for (uint i = 0; i < schema_columns.size(); i++) {
    bool is_in_target_list = false;
    for (auto col_id : column_ids) {
      if (schema_columns[i].column_name == schema_columns[col_id].column_name) {
        is_in_target_list = true;
        break;
      }
    }
    if (is_in_target_list == false)
      dmlist.emplace_back(i, std::pair<oid_t, oid_t>(0, i));
  }

  column_ids.clear();
  for (uint i = 0; i < schema_columns.size(); i++) {
    column_ids.emplace_back(i);
  }

  std::unique_ptr<const planner::ProjectInfo> project_info(
      new planner::ProjectInfo(std::move(tlist), std::move(dmlist)));

  std::unique_ptr<planner::UpdatePlan> update_plan(
      new planner::UpdatePlan(target_table, std::move(project_info)));

  std::unique_ptr<planner::SeqScanPlan> seq_scan_node(
      new planner::SeqScanPlan(target_table, where_expr, column_ids));
  update_plan->AddChild(std::move(seq_scan_node));

  LOG_INFO("Plan created:\n%s", update_plan->GetInfo().c_str());

  std::vector<type::Value> values;

  // name = CS, id = 1
  LOG_INFO("Binding values");
  values.push_back(type::ValueFactory::GetVarcharValue("CS").Copy());
  values.push_back(type::ValueFactory::GetIntegerValue(1).Copy());

  // bind values to parameters in plan
  update_plan->SetParameterValues(&values);
  txn_manager.CommitTransaction(txn);

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(PlannerTest, InsertPlanTestParameter) {
  // Bootstrapping peloton
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  // Create table
  txn = txn_manager.BeginTransaction();
  auto id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "id", true);
  auto name_column = catalog::Column(type::TypeId::VARCHAR, 32, "name", true);

  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_column, name_column}));
  auto ret = catalog::Catalog::GetInstance()->CreateTable(
      DEFAULT_DB_NAME, "department_table", std::move(table_schema), txn);
  if (ret != ResultType::SUCCESS) LOG_TRACE("create table failed");
  txn_manager.CommitTransaction(txn);

  // INSERT INTO department_table VALUES ($0, $1)
  txn = txn_manager.BeginTransaction();
  std::unique_ptr<parser::InsertStatement> insert_statement(
      new parser::InsertStatement(peloton::InsertType::VALUES));

  std::string name = "department_table";
  auto table_ref = new parser::TableRef(TableReferenceType::NAME);
  table_ref->table_info_ .reset(new parser::TableInfo());
  table_ref->table_info_->table_name = name;
  insert_statement->table_ref_.reset(table_ref);

  // Value val =
  //    type::ValueFactory::GetNullValue();  // The value is not important
  // at  this point
  insert_statement->insert_values.push_back(
      std::vector<std::unique_ptr<peloton::expression::AbstractExpression>>());
  auto& parameter_exprs = insert_statement->insert_values[0];
  auto parameter_expr_1 = new expression::ParameterValueExpression(0);
  auto parameter_expr_2 = new expression::ParameterValueExpression(1);
  parameter_exprs.push_back(std::unique_ptr<expression::AbstractExpression>(parameter_expr_1));
  parameter_exprs.push_back(std::unique_ptr<expression::AbstractExpression>(parameter_expr_2));

  auto target_table = catalog::Catalog::GetInstance()->GetTableWithName(
      DEFAULT_DB_NAME, "department_table", txn);

  std::unique_ptr<planner::InsertPlan> insert_plan(new planner::InsertPlan(
      target_table, &insert_statement->columns,
      &insert_statement->insert_values));
  LOG_INFO("Plan created:\n%s", insert_plan->GetInfo().c_str());

  // VALUES(1, "CS")
  LOG_INFO("Binding values");
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(1).Copy());
  values.push_back(type::ValueFactory::GetVarcharValue(
                        (std::string) "CS",
                        TestingHarness::GetInstance().GetTestingPool()).Copy());
  LOG_INFO("Value 1: %s", values.at(0).GetInfo().c_str());
  LOG_INFO("Value 2: %s", values.at(1).GetInfo().c_str());
  // bind values to parameters in plan
  insert_plan->SetParameterValues(&values);
  txn_manager.CommitTransaction(txn);

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(PlannerTest, InsertPlanTestParameterColumns) {
  // Bootstrapping peloton
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  // Create table
  txn = txn_manager.BeginTransaction();
  auto id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "id", true);
  auto name_column = catalog::Column(type::TypeId::VARCHAR, 32, "name", true);

  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_column, name_column}));
  catalog::Catalog::GetInstance()->CreateTable(
      DEFAULT_DB_NAME, "department_table", std::move(table_schema), txn);
  txn_manager.CommitTransaction(txn);

  // INSERT INTO department_table VALUES (1, $1)
  txn = txn_manager.BeginTransaction();
  std::unique_ptr<parser::InsertStatement> insert_statement(
      new parser::InsertStatement(peloton::InsertType::VALUES));

  std::string name = "department_table";
  auto table_ref = new parser::TableRef(TableReferenceType::NAME);
  table_ref->table_info_.reset(new parser::TableInfo());
  table_ref->table_info_->table_name = name;

  std::string id_col = "id", name_col = "name";
  insert_statement->table_ref_.reset(table_ref);
  insert_statement->columns.push_back(id_col);
  insert_statement->columns.push_back(name_col);

  // Value val =
  //    type::ValueFactory::GetNullValue();  // The value is not important
  // at  this point
  auto constant_expr_1 = new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(1));
  auto parameter_expr_2 = new expression::ParameterValueExpression(1);
  insert_statement->insert_values.push_back(
      std::vector<std::unique_ptr<expression::AbstractExpression>>());
  auto& exprs = insert_statement->insert_values[0];
  exprs.push_back(std::unique_ptr<expression::AbstractExpression>(constant_expr_1));
  exprs.push_back(std::unique_ptr<expression::AbstractExpression>(parameter_expr_2));

  auto target_table = catalog::Catalog::GetInstance()->GetTableWithName(
      DEFAULT_DB_NAME, "department_table", txn);

  std::unique_ptr<planner::InsertPlan> insert_plan(new planner::InsertPlan(
      target_table, &insert_statement->columns,
      &insert_statement->insert_values));
  LOG_INFO("Plan created:\n%s", insert_plan->GetInfo().c_str());

  // VALUES(1, "CS")
  LOG_INFO("Binding values");
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetVarcharValue(
                        (std::string) "CS",
                        TestingHarness::GetInstance().GetTestingPool()).Copy());
  LOG_INFO("Value 1: %s", values.at(0).GetInfo().c_str());
  // bind values to parameters in plan
  insert_plan->SetParameterValues(&values);
  txn_manager.CommitTransaction(txn);

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
