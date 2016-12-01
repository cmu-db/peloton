//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_to_operator_transformer.h
//
// Identification: src/include/optimizer/query_to_operator_transformer.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/query_node_visitor.h"

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
class Join;
class Table;
class OrderBy;
class Select;

class OperatorExpression;
class ColumnManager;
}

namespace optimizer {

// Transform a query from parsed statement to operator expressions.
class QueryToOperatorTransformer : public QueryNodeVisitor {
 public:
  QueryToOperatorTransformer(ColumnManager &manager);

  std::shared_ptr<OperatorExpression> ConvertToOpExpression(
      parser::SQLStatement *op);

  void visit(const Table *op) override;

  void visit(const Join *op) override;

  void visit(const OrderBy *op) override;

  void Visit(const parser::SelectStatement *op) override;
  void Visit(const parser::CreateStatement *op) override;
  void Visit(const parser::InsertStatement *op) override;
  void Visit(const parser::DeleteStatement *op) override;
  void Visit(const parser::DropStatement *op) override;
  void Visit(const parser::PrepareStatement *op) override;
  void Visit(const parser::ExecuteStatement *op) override;
  void Visit(const parser::TransactionStatement *op) override;
  void Visit(const parser::UpdateStatement *op) override;
  void Visit(const parser::CopyStatement *op) override;

 private:
  ColumnManager &manager;

  std::shared_ptr<OperatorExpression> output_expr;
  // For expr nodes
  common::Type::TypeId output_type;
  int output_size;
  bool output_inlined;
};

} /* namespace optimizer */
} /* namespace peloton */
