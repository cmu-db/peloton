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


#include "planner/abstract_plan.h"

#include "common/harness.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Planner Tests
//===--------------------------------------------------------------------===//

class PlannerTests : public PelotonTest {};

TEST_F(PlannerTests, DeletePlanTest) {

	/*parser::DeleteStatement *delete_statement = new parser::DeleteStatement();
	delete_statement->table_name = "department";
	Value val = ValueFactory::GetNullValue(); // The value is not important at this point

	auto parameter_expr = new expression::ParameterValueExpression(0, val);
	auto tuple_expr = new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 0);
	auto eq_expr = new expression::CmpEq();


	delete_statement->expr = ;*/

}

}  // End test namespace
}  // End peloton namespace
