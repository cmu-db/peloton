//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_node_visitor.h
//
// Identification: src/include/optimizer/query_node_visitor.h
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
}

namespace optimizer {

class Variable;
class Constant;
class OperatorExpression;
class AndOperator;
class OrOperator;
class NotOperator;
class Attribute;
class Get;
class Join;
class Table;
class OrderBy;
class Select;

//===--------------------------------------------------------------------===//
// Query Node Visitor
//===--------------------------------------------------------------------===//

class QueryNodeVisitor {
 public:
  virtual ~QueryNodeVisitor(){};

  // TODO: They're left here for compilation. Delete them later.
  virtual void visit(const Table *) = 0;
  virtual void visit(const Join *) = 0;
  virtual void visit(const OrderBy *) = 0;
  virtual void visit(const Select *) = 0;

  virtual void Visit(const parser::SelectStatement *) = 0;
  virtual void Visit(const parser::CreateStatement *) = 0;
  virtual void Visit(const parser::InsertStatement *) = 0;
  virtual void Visit(const parser::DeleteStatement *) = 0;
  virtual void Visit(const parser::DropStatement *) = 0;
  virtual void Visit(const parser::PrepareStatement *) = 0;
  virtual void Visit(const parser::ExecuteStatement *) = 0;
  virtual void Visit(const parser::TransactionStatement *) = 0;
  virtual void Visit(const parser::UpdateStatement *) = 0;
  virtual void Visit(const parser::CopyStatement *) = 0;
};

} /* namespace optimizer */
} /* namespace peloton */
