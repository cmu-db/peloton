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

namespace optimizer {

//===--------------------------------------------------------------------===//
// Query Node Visitor
//===--------------------------------------------------------------------===//

class QueryNodeVisitor {
 public:
  virtual ~QueryNodeVisitor(){};

  virtual void Visit(const parser::SelectStatement *) = 0;

  // Some sub query nodes inside SelectStatement
  virtual void Visit(const parser::JoinDefinition *) = 0;
  virtual void Visit(const parser::TableRef *) = 0;
  virtual void Visit(const parser::GroupByDescription *) = 0;
  virtual void Visit(const parser::OrderDescription *) = 0;
  virtual void Visit(const parser::LimitDescription *) = 0;

  virtual void Visit(const parser::CreateStatement *) = 0;
  virtual void Visit(const parser::CreateFunctionStatement *) = 0;
  virtual void Visit(const parser::InsertStatement *) = 0;
  virtual void Visit(const parser::DeleteStatement *) = 0;
  virtual void Visit(const parser::DropStatement *) = 0;
  virtual void Visit(const parser::PrepareStatement *) = 0;
  virtual void Visit(const parser::ExecuteStatement *) = 0;
  virtual void Visit(const parser::TransactionStatement *) = 0;
  virtual void Visit(const parser::UpdateStatement *) = 0;
  virtual void Visit(const parser::CopyStatement *) = 0;
  virtual void Visit(const parser::AnalyzeStatement *) = 0;
};

}  // namespace optimizer
}  // namespace peloton
