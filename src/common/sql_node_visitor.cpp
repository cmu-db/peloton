//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sql_node_visitor.h
//
// Identification: src/common/sql_node_visitor.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/sql_node_visitor.h"
#include "expression/comparison_expression.h"
#include "expression/aggregate_expression.h"
#include "expression/conjunction_expression.h"
#include "expression/function_expression.h"
#include "expression/operator_expression.h"
#include "expression/parameter_value_expression.h"
#include "expression/star_expression.h"
#include "expression/tuple_value_expression.h"
#include "expression/constant_value_expression.h"
#include "expression/case_expression.h"
#include "expression/subquery_expression.h"

namespace peloton {
void SqlNodeVisitor::Visit(expression::ComparisonExpression *expr) {
  expr->AcceptChildren(this);
}
void SqlNodeVisitor::Visit(expression::AggregateExpression *expr) {
  expr->AcceptChildren(this);
}
void SqlNodeVisitor::Visit(expression::CaseExpression *expr) {
  expr->AcceptChildren(this);
}
void SqlNodeVisitor::Visit(expression::ConjunctionExpression *expr) {
  expr->AcceptChildren(this);
}
void SqlNodeVisitor::Visit(expression::ConstantValueExpression *expr) {
  expr->AcceptChildren(this);
}
void SqlNodeVisitor::Visit(expression::FunctionExpression *expr) {
  expr->AcceptChildren(this);
}
void SqlNodeVisitor::Visit(expression::OperatorExpression *expr) {
  expr->AcceptChildren(this);
}
void SqlNodeVisitor::Visit(expression::OperatorUnaryMinusExpression *expr) {
  expr->AcceptChildren(this);
}
void SqlNodeVisitor::Visit(expression::ParameterValueExpression *expr) {
  expr->AcceptChildren(this);
}
void SqlNodeVisitor::Visit(expression::StarExpression *expr) {
  expr->AcceptChildren(this);
}
void SqlNodeVisitor::Visit(expression::TupleValueExpression *expr) {
  expr->AcceptChildren(this);
}
void SqlNodeVisitor::Visit(expression::SubqueryExpression *expr) {
  expr->AcceptChildren(this);
}

}  // peloton
