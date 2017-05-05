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

#include "common/sql_node_visitor.h"
#include <unordered_set>

namespace peloton {

namespace parser {}

namespace optimizer {
class OperatorExpression;
class ColumnManager;
}

namespace optimizer {

// Transform a query from parsed statement to operator expressions.
class QueryToOperatorTransformer : public SqlNodeVisitor {
 public:
  QueryToOperatorTransformer() {}

  std::shared_ptr<OperatorExpression> ConvertToOpExpression(
      parser::SQLStatement *op);

  void Visit(const parser::SelectStatement *op) override;

  void Visit(const parser::TableRef *) override;
  void Visit(const parser::JoinDefinition *) override;
  void Visit(const parser::GroupByDescription *) override;
  void Visit(const parser::OrderDescription *) override;
  void Visit(const parser::LimitDescription *) override;

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
  std::shared_ptr<OperatorExpression> output_expr;
  MultiTablePredicates join_predicates_;
  std::unordered_set<std::string> table_alias_set_;

  // For expr nodes
  type::Type::TypeId output_type;
  int output_size;
  bool output_inlined;
};

} /* namespace optimizer */
} /* namespace peloton */
