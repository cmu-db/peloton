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
class CreateFunctionStatement;
class InsertStatement;
class DeleteStatement;
class DropStatement;
class PrepareStatement;
class ExecuteStatement;
class TransactionStatement;
class UpdateStatement;
class CopyStatement;
class AnalyzeStatement;
class VariableSetStatement;
class JoinDefinition;
struct TableRef;

class GroupByDescription;
class OrderDescription;
class LimitDescription;
}  // namespace parser

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
class CaseExpression;
class SubqueryExpression;
}  // namespace expression

//===--------------------------------------------------------------------===//
// Query Node Visitor
//===--------------------------------------------------------------------===//

class SqlNodeVisitor {
 public:
  virtual ~SqlNodeVisitor(){};

  virtual void Visit(parser::SelectStatement *) {}

  // Some sub query nodes inside SelectStatement
  virtual void Visit(parser::JoinDefinition *) {}
  virtual void Visit(parser::TableRef *) {}
  virtual void Visit(parser::GroupByDescription *) {}
  virtual void Visit(parser::OrderDescription *) {}
  virtual void Visit(parser::LimitDescription *) {}

  virtual void Visit(parser::CreateStatement *) {}
  virtual void Visit(parser::CreateFunctionStatement *) {}
  virtual void Visit(parser::InsertStatement *) {}
  virtual void Visit(parser::DeleteStatement *) {}
  virtual void Visit(parser::DropStatement *) {}
  virtual void Visit(parser::PrepareStatement *) {}
  virtual void Visit(parser::ExecuteStatement *) {}
  virtual void Visit(parser::TransactionStatement *) {}
  virtual void Visit(parser::UpdateStatement *) {}
  virtual void Visit(parser::CopyStatement *) {}
  virtual void Visit(parser::AnalyzeStatement *){};

  virtual void Visit(expression::ComparisonExpression *expr);
  virtual void Visit(expression::AggregateExpression *expr);
  virtual void Visit(expression::CaseExpression *expr);
  virtual void Visit(expression::ConjunctionExpression *expr);
  virtual void Visit(expression::ConstantValueExpression *expr);
  virtual void Visit(expression::FunctionExpression *expr);
  virtual void Visit(expression::OperatorExpression *expr);
  virtual void Visit(expression::OperatorUnaryMinusExpression *expr);
  virtual void Visit(expression::ParameterValueExpression *expr);
  virtual void Visit(expression::StarExpression *expr);
  virtual void Visit(expression::TupleValueExpression *expr);
  virtual void Visit(expression::SubqueryExpression *expr);


};

}  // namespace peloton
