//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bind_node_visitor.h
//
// Identification: src/include/binder/binder_node_visitor.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "binder/binder_context.h"
#include "common/sql_node_visitor.h"
#include "parser/statements.h"
#include "common/internal_types.h"

namespace peloton {

namespace expression {
class CaseExpression;
class ConstantExpression;
class TupleValueExpression;
class SubqueryExpression;
class StarExpression;
class OperatorExpression;
class AggregateExpression;
}  // namespace expression

namespace parser {
class SQLStatement;
}  // namespace parser
namespace catalog {
class Catalog;
}

namespace binder {

class BindNodeVisitor : public SqlNodeVisitor {
 public:
  BindNodeVisitor(concurrency::TransactionContext *txn,
                  std::string default_database_name);

  void BindNameToNode(parser::SQLStatement *tree);
  void Visit(parser::SelectStatement *) override;

  // Some sub query nodes inside SelectStatement
  void Visit(parser::JoinDefinition *) override;
  void Visit(parser::TableRef *) override;
  void Visit(parser::GroupByDescription *) override;
  void Visit(parser::OrderDescription *) override;
  void Visit(parser::LimitDescription *) override;

  void Visit(parser::CreateStatement *) override;
  void Visit(parser::CreateFunctionStatement *) override;
  void Visit(parser::InsertStatement *) override;
  void Visit(parser::DeleteStatement *) override;
  void Visit(parser::DropStatement *) override;
  void Visit(parser::PrepareStatement *) override;
  void Visit(parser::ExecuteStatement *) override;
  void Visit(parser::TransactionStatement *) override;
  void Visit(parser::UpdateStatement *) override;
  void Visit(parser::CopyStatement *) override;
  void Visit(parser::AnalyzeStatement *) override;

  void Visit(expression::CaseExpression *expr) override;
  void Visit(expression::SubqueryExpression *expr) override;

  // void Visit(const expression::ConstantValueExpression *expr) override;
  void Visit(expression::TupleValueExpression *expr) override;
  void Visit(expression::StarExpression *expr) override;
  void Visit(expression::FunctionExpression *expr) override;

  // Deduce value type for these expressions
  void Visit(expression::OperatorExpression *expr) override;
  void Visit(expression::AggregateExpression *expr) override;

  void SetTxn(concurrency::TransactionContext *txn) { this->txn_ = txn; }

 private:
  std::shared_ptr<BinderContext> context_;
  concurrency::TransactionContext *txn_;
  std::string default_database_name_;
  catalog::Catalog *catalog_;
};

}  // namespace binder
}  // namespace peloton
