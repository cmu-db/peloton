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
	catalog::Bootstrapper::global_catalog->CreateDatabase(DEFAULT_DB_NAME);

	// Create table
	auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
	txn_manager.BeginTransaction();
	auto id_column =
	      catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
		                      "id", true);
	auto name_column =
	     catalog::Column(VALUE_TYPE_VARCHAR, 32,
	                      "name", true);

	std::unique_ptr<catalog::Schema> table_schema(new catalog::Schema({id_column, name_column}));
	catalog::Bootstrapper::global_catalog->CreateTable(DEFAULT_DB_NAME, "department_table", std::move(table_schema));

	// DELETE FROM department WHERE col_0 = $0
	parser::DeleteStatement *delete_statement = new parser::DeleteStatement();
	delete_statement->table_name = "department_table";
	Value val = ValueFactory::GetNullValue(); // The value is not important at this point

	// tuple[0].colum[0] = $0
	auto parameter_expr = new expression::ParameterValueExpression(0, val);
	auto tuple_expr = new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 0);
	auto cmp_expr = new expression::ComparisonExpression<peloton::expression::CmpEq>(EXPRESSION_TYPE_COMPARE_EQUAL,
			tuple_expr, parameter_expr);

	delete_statement->expr = cmp_expr;

	auto del_plan = new planner::DeletePlan(delete_statement);


	bridge::PlanExecutor::PrintPlan(del_plan, "Delete Plan");

}

}  // End test namespace
}  // End peloton namespace
