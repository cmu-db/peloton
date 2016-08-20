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
#include "catalog/bootstrapper.h"
#include "expression/abstract_expression.h"
#include "expression/operator_expression.h"
#include "expression/comparison_expression.h"
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
	catalog::Bootstrapper::bootstrap();
	catalog::Bootstrapper::global_catalog->CreateDatabase(DEFAULT_DB_NAME, nullptr);

	// Create table
	auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
	auto txn = txn_manager.BeginTransaction();
	auto id_column =
	      catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
		                      "id", true);
	auto name_column =
	     catalog::Column(VALUE_TYPE_VARCHAR, 32,
	                      "name", true);

	std::unique_ptr<catalog::Schema> table_schema(new catalog::Schema({id_column, name_column}));
	catalog::Bootstrapper::global_catalog->CreateTable(DEFAULT_DB_NAME,
			"department_table", std::move(table_schema), txn);

	// DELETE FROM department_table WHERE id = $0
	parser::DeleteStatement *delete_statement = new parser::DeleteStatement();
	delete_statement->table_name = "department_table";
	Value val = ValueFactory::GetNullValue(); // The value is not important at this point

	// id = $0
	auto parameter_expr = new expression::ParameterValueExpression(0, val);
	auto tuple_expr = new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 0);
	auto cmp_expr = new expression::ComparisonExpression<peloton::expression::CmpEq>(EXPRESSION_TYPE_COMPARE_EQUAL,
			tuple_expr, parameter_expr);

	delete_statement->expr = cmp_expr;

	auto del_plan = new planner::DeletePlan(delete_statement);
	LOG_INFO("Plan created");
	bridge::PlanExecutor::PrintPlan(del_plan, "Delete Plan");


	auto values = new std::vector<Value>();

	// id = 15
	LOG_INFO("Binding values");
	values->push_back(ValueFactory::GetIntegerValue(15));

	// bind values to parameters in plan
	del_plan->SetParameterValues(values);
	txn_manager.CommitTransaction(txn);

}

TEST_F(PlannerTests, UpdatePlanTestParameter) {


	// Bootstrapping peloton
	catalog::Bootstrapper::bootstrap();
	catalog::Bootstrapper::global_catalog->CreateDatabase(DEFAULT_DB_NAME, nullptr);

	// Create table
	auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
	auto txn = txn_manager.BeginTransaction();
	auto id_column =
	      catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
		                      "id", true);
	auto name_column =
	     catalog::Column(VALUE_TYPE_VARCHAR, 32,
	                      "name", true);

	std::unique_ptr<catalog::Schema> table_schema(new catalog::Schema({id_column, name_column}));
	catalog::Bootstrapper::global_catalog->CreateTable(DEFAULT_DB_NAME,
			"department_table", std::move(table_schema), txn);

	// UPDATE department_table SET name = $0 WHERE id = $1
	parser::UpdateStatement *update_statement = new parser::UpdateStatement();
	parser::TableRef* table_ref = new parser::TableRef(peloton::TABLE_REFERENCE_TYPE_JOIN);
	table_ref->name = "department_table";
	update_statement->table = table_ref;
	Value val = ValueFactory::GetNullValue(); // The value is not important at this point

	// name = $0
	auto update = new parser::UpdateClause();
	update->column = "name";
	auto parameter_expr = new expression::ParameterValueExpression(0, val);
	update->value = parameter_expr;
	auto updates = new std::vector<parser::UpdateClause*>();
	updates->push_back(update);
	update_statement->updates = updates;

	// id = $1
	parameter_expr = new expression::ParameterValueExpression(1, val);
	auto tuple_expr = new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 0);
	auto cmp_expr = new expression::ComparisonExpression<peloton::expression::CmpEq>(EXPRESSION_TYPE_COMPARE_EQUAL,
			tuple_expr, parameter_expr);

	update_statement->where = cmp_expr;

	auto update_plan = new planner::UpdatePlan(update_statement);
	LOG_INFO("Plan created");
	bridge::PlanExecutor::PrintPlan(update_plan, "Update Plan");

	auto values = new std::vector<Value>();

	// name = CS, id = 1
	LOG_INFO("Binding values");
	values->push_back(ValueFactory::GetStringValue("CS"));
	values->push_back(ValueFactory::GetIntegerValue(1));

	// bind values to parameters in plan
	update_plan->SetParameterValues(values);
	txn_manager.CommitTransaction(txn);

}

TEST_F(PlannerTests, InsertPlanTestParameter) {
	// Bootstrapping peloton
	catalog::Bootstrapper::bootstrap();
	catalog::Bootstrapper::global_catalog->CreateDatabase(DEFAULT_DB_NAME, nullptr);

	// Create table
	auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
	auto txn = txn_manager.BeginTransaction();
	auto id_column =
	      catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
		                      "id", true);
	auto name_column =
	     catalog::Column(VALUE_TYPE_VARCHAR, 32,
	                      "name", true);

	std::unique_ptr<catalog::Schema> table_schema(new catalog::Schema({id_column, name_column}));
	catalog::Bootstrapper::global_catalog->CreateTable(DEFAULT_DB_NAME,
			"department_table", std::move(table_schema), txn);

	// INSERT INTO department_table VALUES ($0, $1)
	auto insert_statement = new parser::InsertStatement(peloton::INSERT_TYPE_VALUES);
	insert_statement->table_name = "department_table";
	std::vector<char*>* columns = NULL;  // will not be used
	insert_statement->columns = columns;

	Value val = ValueFactory::GetNullValue(); // The value is not important at this point
	auto parameter_expr_1 = new expression::ParameterValueExpression(0, val);
	auto parameter_expr_2 = new expression::ParameterValueExpression(1, val);
	auto parameter_exprs = new std::vector<expression::AbstractExpression*>();
	parameter_exprs->push_back(parameter_expr_1);
	parameter_exprs->push_back(parameter_expr_2);
	insert_statement->values = parameter_exprs;

	auto insert_plan = new planner::InsertPlan(insert_statement);
	LOG_INFO("Plan created");
	bridge::PlanExecutor::PrintPlan(insert_plan, "Insert Plan");

	// VALUES(1, "CS")
	LOG_INFO("Binding values");
	auto values = new std::vector<Value>();
	values->push_back(ValueFactory::GetIntegerValue(1));
	values->push_back(ValueFactory::GetStringValue("CS",
			TestingHarness::GetInstance().GetTestingPool()));
	LOG_INFO("Value 1: %s", values->at(0).GetInfo().c_str());
	LOG_INFO("Value 2: %s", values->at(1).GetInfo().c_str());
	// bind values to parameters in plan
	insert_plan->SetParameterValues(values);
	txn_manager.CommitTransaction(txn);

}

}  // End test namespace
}  // End peloton namespace
