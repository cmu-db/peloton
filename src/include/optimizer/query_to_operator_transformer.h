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

#include <unordered_set>
#include "common/sql_node_visitor.h"

namespace peloton {

namespace parser {
class SQLStatement;
}  // namespace parser

namespace concurrency {
class Transaction;
}

namespace expression {
class AbstractExpression;
}

namespace optimizer {

class OperatorExpression;
class ColumnManager;

// Transform a query from parsed statement to operator expressions.
class QueryToOperatorTransformer : public SqlNodeVisitor {
 public:
  QueryToOperatorTransformer(concurrency::Transaction *txn);

  std::shared_ptr<OperatorExpression> ConvertToOpExpression(
      parser::SQLStatement *op);

  void Visit(parser::SelectStatement *op) override;

  void Visit(parser::TableRef *) override;
  void Visit(parser::JoinDefinition *) override;
  void Visit(parser::GroupByDescription *) override;
  void Visit(parser::OrderDescription *) override;
  void Visit(parser::LimitDescription *) override;

  void Visit(parser::CreateStatement *op) override;
  void Visit(parser::InsertStatement *op) override;
  void Visit(parser::DeleteStatement *op) override;
  void Visit(parser::DropStatement *op) override;
  void Visit(parser::PrepareStatement *op) override;
  void Visit(parser::ExecuteStatement *op) override;
  void Visit(parser::TransactionStatement *op) override;
  void Visit(parser::UpdateStatement *op) override;
  void Visit(parser::CopyStatement *op) override;
  void Visit(parser::AnalyzeStatement *op) override;

  inline oid_t GetAndIncreaseGetId() { return get_id++; }

 private:
  std::shared_ptr<OperatorExpression> output_expr_;
  MultiTablePredicates join_predicates_;
  SingleTablePredicatesMap single_table_predicates_map;
  std::unordered_set<std::string> table_alias_set_;

  // For expr nodes
  type::TypeId output_type;
  int output_size;
  bool output_inlined;
  concurrency::Transaction *txn_;

  // identifier for get operators
  oid_t get_id;

};

}  // namespace optimizer
}  // namespace peloton
