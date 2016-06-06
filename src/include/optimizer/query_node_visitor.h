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
namespace optimizer {

class Variable;
class Constant;
class OperatorExpression;
class AndOperator;
class OrOperator;
class NotOperator;
class Attribute;
class Table;
class Join;
class OrderBy;
class Select;

//===--------------------------------------------------------------------===//
// Query Node Visitor
//===--------------------------------------------------------------------===//

class QueryNodeVisitor {
 public:
  virtual ~QueryNodeVisitor(){};

  virtual void visit(const Variable*) = 0;
  virtual void visit(const Constant*) = 0;
  virtual void visit(const OperatorExpression*) = 0;
  virtual void visit(const AndOperator*) = 0;
  virtual void visit(const OrOperator*) = 0;
  virtual void visit(const NotOperator*) = 0;
  virtual void visit(const Attribute*) = 0;
  virtual void visit(const Table*) = 0;
  virtual void visit(const Join*) = 0;
  virtual void visit(const OrderBy*) = 0;
  virtual void visit(const Select*) = 0;
};

} /* namespace optimizer */
} /* namespace peloton */
