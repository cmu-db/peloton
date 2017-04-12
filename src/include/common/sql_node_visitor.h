//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sql_node_visitor.h
//
// Identification: src/include/common/sql_node_visitor.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

namespace peloton {

namespace parser {
class SelectStatement;
class CreateStatement;
class InsertStatement;
class DeleteStatement;
class DropStatement;
class PrepareStatement;
class ExecuteStatement;
class TransactionStatement;
class UpdateStatement;
class CopyStatement;
struct JoinDefinition;
struct TableRef;

class GroupByDescription;
class OrderDescription;
class LimitDescription;
}

namespace expression {
class AbstractExpression;
class ComparisonExpression;
class AggregateExpression;
class ConjunctionExpression;
class ConstantValueExpression;
class OperatorExpression;
class ParameterValueExpression;
class StarExpression;
class TupleValueExpression;
class FunctionExpression;
class OperatorUnaryMinusExpression;
}

//===--------------------------------------------------------------------===//
// Query Node Visitor
//===--------------------------------------------------------------------===//

class SqlNodeVisitor {
 public:
  virtual ~SqlNodeVisitor(){};

  virtual void Visit(const parser::SelectStatement *) = 0;

  // Some sub query nodes inside SelectStatement
  virtual void Visit(const parser::JoinDefinition *) = 0;
  virtual void Visit(const parser::TableRef *) = 0;
  virtual void Visit(const parser::GroupByDescription *) = 0;
  virtual void Visit(const parser::OrderDescription *) = 0;
  virtual void Visit(const parser::LimitDescription *) = 0;

  virtual void Visit(const parser::CreateStatement *) = 0;
  virtual void Visit(const parser::InsertStatement *) = 0;
  virtual void Visit(const parser::DeleteStatement *) = 0;
  virtual void Visit(const parser::DropStatement *) = 0;
  virtual void Visit(const parser::PrepareStatement *) = 0;
  virtual void Visit(const parser::ExecuteStatement *) = 0;
  virtual void Visit(const parser::TransactionStatement *) = 0;
  virtual void Visit(const parser::UpdateStatement *) = 0;
  virtual void Visit(const parser::CopyStatement *) = 0;

  virtual void Visit(expression::ComparisonExpression *expr);
  virtual void Visit(expression::AggregateExpression *expr);
  virtual void Visit(expression::ConjunctionExpression *expr);
  virtual void Visit(expression::ConstantValueExpression *expr);
  virtual void Visit(expression::FunctionExpression *expr);
  virtual void Visit(expression::OperatorExpression *expr);
  virtual void Visit(expression::OperatorUnaryMinusExpression *expr);
  virtual void Visit(expression::ParameterValueExpression *expr);
  virtual void Visit(expression::StarExpression *expr);
  virtual void Visit(expression::TupleValueExpression *expr);
};

} /* namespace peloton */
