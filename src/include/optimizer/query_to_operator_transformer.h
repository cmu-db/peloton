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

#include <memory>
#include <unordered_set>
#include "common/sql_node_visitor.h"

namespace peloton {

namespace parser {
class SQLStatement;
}  // namespace parser

namespace concurrency {
class TransactionContext;
}

namespace expression {
class AbstractExpression;
}

namespace optimizer {

class OperatorExpression;
class ColumnManager;

/**
 * @brief Transform a query from parsed statement to operator expressions.
 */
class QueryToOperatorTransformer : public SqlNodeVisitor {
 public:
  QueryToOperatorTransformer(concurrency::TransactionContext *txn);

  std::shared_ptr<OperatorExpression> ConvertToOpExpression(
      parser::SQLStatement *op);

  void Visit(parser::SelectStatement *op) override;

  void Visit(parser::TableRef *) override;
  void Visit(parser::JoinDefinition *) override;
  void Visit(parser::GroupByDescription *) override;
  void Visit(parser::OrderDescription *) override;
  void Visit(parser::LimitDescription *) override;

  void Visit(parser::CreateStatement *op) override;
  void Visit(parser::CreateFunctionStatement *op) override;
  void Visit(parser::InsertStatement *op) override;
  void Visit(parser::DeleteStatement *op) override;
  void Visit(parser::DropStatement *op) override;
  void Visit(parser::PrepareStatement *op) override;
  void Visit(parser::ExecuteStatement *op) override;
  void Visit(parser::TransactionStatement *op) override;
  void Visit(parser::UpdateStatement *op) override;
  void Visit(parser::CopyStatement *op) override;
  void Visit(parser::AnalyzeStatement *op) override;
  void Visit(expression::ComparisonExpression *expr) override;
  void Visit(expression::OperatorExpression *expr) override;

  inline oid_t GetAndIncreaseGetId() { return get_id++; }

  /**
   * @brief Walk through an enxpression, split it into a set of predicates that
   *  could be joined by conjunction. We need the set to perform predicate
   *  push-down. For example, for the following query
   *  "SELECT test.a, test1.b FROM test, test1 WHERE test.a = test1.b and test.a
   *  = 5"
   *  we could will extract "test.a = test1.b" and "test.a = 5" from the
   *  original predicate and we could let "test.a = 5" to be evaluated at the
   *  table scan level
   *
   * @param expr The original predicate
   */
  void CollectPredicates(expression::AbstractExpression *expr);

  // TODO(boweic): Since we haven't migrated all the functionalities needed to
  // generate mark-join and single-join to the optimizer, currently this
  // function has not been tested, and it may be a bit hard to understand. We
  // may integrate the unnesting functionality in the next PR
  /**
   * @brief Transform a sub-query in an expression to use
   *
   * @param expr The potential sub-query expression
   * @param select_list The select list of the sub-query we generate
   * @param single_join A boolean to tell if
   *
   * @return If the expression could be transformed into sub-query, return true,
   *  return false otherwise
   */
  bool GenerateSubquerytree(
      expression::AbstractExpression *expr,
      std::vector<expression::AbstractExpression *> &select_list,
      bool single_join = false);

  static bool RequireAggregation(const parser::SelectStatement *op);

 private:
  std::shared_ptr<OperatorExpression> output_expr_;

  concurrency::TransactionContext *txn_;
  /**
   * @brief identifier for get operators
   */
  oid_t get_id;
  bool enable_predicate_push_down_;

  /**
   * @brief The Depth of the current operator, we need this to tell if there's
   *  dependent join in the query. Dependent join transformation logic is not
   *  implemented yet
   */
  int depth_;
  /**
   * @brief A set of predicates the current operator generated, we use them to
   *  generate filter operator
   */
  std::vector<AnnotatedExpression> predicates_;
};

}  // namespace optimizer
}  // namespace peloton
