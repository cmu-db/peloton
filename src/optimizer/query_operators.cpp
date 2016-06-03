//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_operators.cpp
//
// Identification: src/optimizer/query_operators.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/query_operators.h"
#include "optimizer/query_node_visitor.h"

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// QueryExpression
//===--------------------------------------------------------------------===//

QueryExpression::QueryExpression() {}

QueryExpression::~QueryExpression() {}

const QueryExpression *QueryExpression::GetParent() const { return parent_; }

//===--------------------------------------------------------------------===//
// Variable
//===--------------------------------------------------------------------===//
Variable::Variable(oid_t base_table, oid_t column_index, catalog::Column col)
  : base_table_oid(base_table), column_index(column_index), column(col)
{
}

ExpressionType Variable::GetExpressionType() const {
  return EXPRESSION_TYPE_COLUMN_REF;
}

void Variable::accept(QueryNodeVisitor *v) const {
  v->visit(this);
}

//===--------------------------------------------------------------------===//
// Constant
//===--------------------------------------------------------------------===//
Constant::Constant(Value value)
  : value(value)
{}

ExpressionType Constant::GetExpressionType() const {
  return EXPRESSION_TYPE_VALUE_CONSTANT;
}

void Constant::accept(QueryNodeVisitor *v) const {
  v->visit(this);
}

//===--------------------------------------------------------------------===//
// OperatorExpression - matches with Peloton's operator_expression.h
//===--------------------------------------------------------------------===//
OperatorExpression::OperatorExpression(
  peloton::ExpressionType type,
  ValueType value_type,
  const std::vector<QueryExpression *>& args)
  : type(type), value_type(value_type), args(args)
{}

ExpressionType OperatorExpression::GetExpressionType() const {
  return type;
}

void OperatorExpression::accept(QueryNodeVisitor *v) const {
  v->visit(this);
}

//===--------------------------------------------------------------------===//
// Logical Operators
//===--------------------------------------------------------------------===//
AndOperator::AndOperator(const std::vector<QueryExpression *> args)
  : args(args)
{
}

ExpressionType AndOperator::GetExpressionType() const {
  return EXPRESSION_TYPE_CONJUNCTION_AND;
}

void AndOperator::accept(QueryNodeVisitor *v) const {
  v->visit(this);
}

OrOperator::OrOperator(const std::vector<QueryExpression *> args)
  : args(args)
{
}

ExpressionType OrOperator::GetExpressionType() const {
  return EXPRESSION_TYPE_CONJUNCTION_OR;
}

void OrOperator::accept(QueryNodeVisitor *v) const {
  v->visit(this);
}

NotOperator::NotOperator(QueryExpression *arg)
  : arg(arg)
{
}

ExpressionType NotOperator::GetExpressionType() const {
  return EXPRESSION_TYPE_OPERATOR_NOT;
}

void NotOperator::accept(QueryNodeVisitor *v) const {
  v->visit(this);
}

//===--------------------------------------------------------------------===//
// Attribute
//===--------------------------------------------------------------------===//
Attribute::Attribute(
  QueryExpression *expression,
  std::string name,
  bool intermediate)
  : expression(expression), name(name), intermediate(intermediate)
{}

ExpressionType Attribute::GetExpressionType() const {
  return EXPRESSION_TYPE_COLUMN_REF;
}

void Attribute::accept(QueryNodeVisitor *v) const {
  v->visit(this);
}

//===--------------------------------------------------------------------===//
// QueryJoinNode
//===--------------------------------------------------------------------===//

QueryJoinNode::QueryJoinNode() {}

QueryJoinNode::~QueryJoinNode() {}

const QueryJoinNode *QueryJoinNode::GetParent() const { return parent_; }

//===--------------------------------------------------------------------===//
// Table
//===--------------------------------------------------------------------===//
Table::Table(storage::DataTable *data_table)
  : data_table(data_table)
{}

QueryJoinNodeType Table::GetPlanNodeType() const {
  return QueryJoinNodeType::TABLE;
}

void Table::accept(QueryNodeVisitor *v) const {
  v->visit(this);
}

//===--------------------------------------------------------------------===//
// Join
//===--------------------------------------------------------------------===//
Join::Join(PelotonJoinType join_type,
           QueryJoinNode *left_node,
           QueryJoinNode *right_node,
           QueryExpression *predicate,
           const std::vector<Table *>& left_tables,
           const std::vector<Table *>& right_tables)
  : join_type(join_type),
    left_node(left_node),
    right_node(right_node),
    predicate(predicate),
    left_node_tables(left_tables),
    right_node_tables(right_tables)
{}

QueryJoinNodeType Join::GetPlanNodeType() const {
  return QueryJoinNodeType::JOIN;
}

void Join::accept(QueryNodeVisitor *v) const {
  v->visit(this);
}

//===--------------------------------------------------------------------===//
// Order By
//===--------------------------------------------------------------------===//
OrderBy::OrderBy(
  int output_list_index,
  bridge::PltFuncMetaInfo equality_fn,
  bridge::PltFuncMetaInfo sort_fn,
  bool hashable,
  bool nulls_first,
  bool reverse)
  : output_list_index(output_list_index),
    equality_fn(equality_fn),
    sort_fn(sort_fn),
    hashable(hashable),
    nulls_first(nulls_first),
    reverse(reverse)
{}

void OrderBy::accept(QueryNodeVisitor *v) const {
  v->visit(this);
}

//===--------------------------------------------------------------------===//
// Select
//===--------------------------------------------------------------------===//
Select::Select(QueryJoinNode *join_tree,
               QueryExpression *where_predicate,
               const std::vector<Attribute *>& output_list,
               const std::vector<OrderBy *>& orderings)
  : join_tree(join_tree),
    where_predicate(where_predicate),
    output_list(output_list),
    orderings(orderings)
{}

void Select::accept(QueryNodeVisitor *v) const {
  v->visit(this);
}

} /* namespace optimizer */
} /* namespace peloton */
