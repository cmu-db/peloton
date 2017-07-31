//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parse_node_visitor.h
//
// Identification: src/include/parser/peloton/parse_node_visitor.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

namespace peloton {
namespace parser {

class CreateParse;
class DropParse;
class InsertParse;
class SelectParse;

//===--------------------------------------------------------------------===//
// Parse Node Visitor
//===--------------------------------------------------------------------===//

class ParseNodeVisitor {
 public:
  virtual ~ParseNodeVisitor() {};

  /* TODO: implement them one by one later
  virtual void visit(const Variable *) = 0;
  virtual void visit(const Constant *) = 0;
  virtual void visit(const OperatorExpression *) = 0;
  virtual void visit(const AndOperator *) = 0;
  virtual void visit(const OrOperator *) = 0;
  virtual void visit(const NotOperator *) = 0;
  virtual void visit(const Attribute *) = 0;
  virtual void visit(const Table *) = 0;
  virtual void visit(const Join *) = 0;
  virtual void visit(const OrderBy *) = 0;
  */
  virtual void visit(const CreateParse *) = 0;
  virtual void visit(const DropParse *) = 0;
  virtual void visit(const InsertParse *) = 0;
  virtual void visit(const SelectParse *) = 0;
};

}  // namespace optimizer
}  // namespace peloton
