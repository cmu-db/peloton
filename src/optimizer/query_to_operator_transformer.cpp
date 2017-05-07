//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_to_operator_transformer.cpp
//
// Identification: src/optimizer/query_to_operator_transformer.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cmath>

#include "expression/expression_util.h"

#include "optimizer/operator_expression.h"
#include "optimizer/operators.h"
#include "optimizer/query_node_visitor.h"
#include "optimizer/query_to_operator_transformer.h"

#include "planner/order_by_plan.h"
#include "planner/projection_plan.h"
#include "planner/seq_scan_plan.h"

#include "parser/statements.h"

#include "catalog/catalog.h"
#include "catalog/manager.h"

using std::vector;
using std::shared_ptr;

namespace peloton {
namespace optimizer {
std::shared_ptr<OperatorExpression>
QueryToOperatorTransformer::ConvertToOpExpression(parser::SQLStatement *op) {
  output_expr = nullptr;
  op->Accept(this);
  return output_expr;
}

void QueryToOperatorTransformer::Visit(const parser::SelectStatement *op) {
  auto upper_expr = output_expr;

  if (op->where_clause != nullptr) {
    SingleTablePredicates where_predicates;
    util::ExtractPredicates(op->where_clause, where_predicates, join_predicates_);

    // Remove join predicates in the where clause
    if (!join_predicates_.empty()) {
      // Discard the const qualifier to update the where clause.
      // The select statement can only be updated in this way. This is a hack.
      ((parser::SelectStatement *)op)
          ->UpdateWhereClause(util::CombinePredicates(where_predicates));
    }
    else {
      for (auto expr : where_predicates)
        delete expr;
    }
  }

  if (op->from_table != nullptr) {
    // SELECT with FROM
    op->from_table->Accept(this);
    if (op->group_by != nullptr) {
      // Make copies of groupby columns
      vector<shared_ptr<expression::AbstractExpression>> group_by_cols;
      for (auto col : *op->group_by->columns)
        group_by_cols.emplace_back(col->Copy());
      auto group_by = std::make_shared<OperatorExpression>(
          LogicalGroupBy::make(move(group_by_cols), op->group_by->having));
      group_by->PushChild(output_expr);
      output_expr = group_by;
    } else {
      // Check plain aggregation
      bool has_aggregation = false;
      bool has_other_exprs = false;
      for (auto expr : *op->getSelectList()) {
        vector<shared_ptr<expression::AggregateExpression>> aggr_exprs;
        expression::ExpressionUtil::GetAggregateExprs(aggr_exprs, expr);
        if (aggr_exprs.size() > 0)
          has_aggregation = true;
        else
          has_other_exprs = true;
      }
      // Syntax error when there are mixture of aggregation and other exprs
      // when group by is absent
      if (has_aggregation && has_other_exprs)
        throw SyntaxException(
            "Non aggregation expression must appear in the GROUP BY "
            "clause or be used in an aggregate function");
      // Plain aggregation
      else if (has_aggregation && !has_other_exprs) {
        auto aggregate =
            std::make_shared<OperatorExpression>(LogicalAggregate::make());
        aggregate->PushChild(output_expr);
        output_expr = aggregate;
      }
    }
  } else {
    // SELECT without FROM
    output_expr = std::make_shared<OperatorExpression>(LogicalGet::make());
  }

  // Update output_expr if upper_expr exists
  if (upper_expr != nullptr) {
    upper_expr->PushChild(output_expr);
    output_expr = upper_expr;
  }
}
void QueryToOperatorTransformer::Visit(const parser::JoinDefinition *node) {
  // Get left operator
  node->left->Accept(this);
  auto left_expr = output_expr;
  auto left_table_alias_set = table_alias_set_;
  table_alias_set_.clear();

  // Get right operator
  node->right->Accept(this);
  auto right_expr = output_expr;
  util::SetUnion(table_alias_set_, left_table_alias_set);

  // Construct join operator
  std::shared_ptr<OperatorExpression> join_expr;
  switch (node->type) {
    case JoinType::INNER: {
      if (node->condition != nullptr) {
        // Add join condition into join predicates
        std::unordered_set<std::string> join_condition_table_alias_set;
        expression::ExpressionUtil::GenerateTableAliasSet(
            node->condition, join_condition_table_alias_set);
        join_predicates_.emplace_back(
            MultiTableExpression(node->condition->Copy(), join_condition_table_alias_set));
      }
      join_expr = std::make_shared<OperatorExpression>(
          LogicalInnerJoin::make(
              util::ConstructJoinPredicate(table_alias_set_, join_predicates_)));
      break;
    }
    case JoinType::OUTER: {
      join_expr = std::make_shared<OperatorExpression>(
          LogicalOuterJoin::make(node->condition->Copy()));
      break;
    }
    case JoinType::LEFT: {
      join_expr = std::make_shared<OperatorExpression>(
          LogicalLeftJoin::make(node->condition->Copy()));
      break;
    }
    case JoinType::RIGHT: {
      join_expr = std::make_shared<OperatorExpression>(
          LogicalRightJoin::make(node->condition->Copy()));
      break;
    }
    case JoinType::SEMI: {
      join_expr = std::make_shared<OperatorExpression>(
          LogicalSemiJoin::make(node->condition->Copy()));
      break;
    }
    default:
      throw Exception("Join type invalid");
  }

  join_expr->PushChild(left_expr);
  join_expr->PushChild(right_expr);

  output_expr = join_expr;
}
void QueryToOperatorTransformer::Visit(const parser::TableRef *node) {
  // Nested select. Not supported in the current executors
  if (node->select != nullptr) {
    throw NotImplementedException("Not support joins");
    node->select->Accept(this);
  }
  // Join
  else if (node->join != nullptr) {
    node->join->Accept(this);
  }
  // Multiple tables
  else if (node->list != nullptr && node->list->size() > 1) {
    node->list->at(0)->Accept(this);
    auto left_expr = output_expr;
    auto left_table_alias_set = table_alias_set_;
    table_alias_set_.clear();

    node->list->at(1)->Accept(this);
    auto right_expr = output_expr;

    util::SetUnion(table_alias_set_, left_table_alias_set);

    auto join_expr = std::make_shared<OperatorExpression>(
        LogicalInnerJoin::make(
            util::ConstructJoinPredicate(table_alias_set_, join_predicates_)));
    join_expr->PushChild(left_expr);
    join_expr->PushChild(right_expr);

    for (size_t i=2; i<node->list->size(); i++) {
      node->list->at(i)->Accept(this);
      auto old_join_expr = join_expr;
      join_expr = std::make_shared<OperatorExpression>(
          LogicalInnerJoin::make(
              util::ConstructJoinPredicate(table_alias_set_, join_predicates_)));
      join_expr->PushChild(old_join_expr);
      join_expr->PushChild(output_expr);
    }
    output_expr = join_expr;
  }
  // Single table
  else {
    if (node->list != nullptr && node->list->size() == 1)
      node = node->list->at(0);
    storage::DataTable *target_table =
        catalog::Catalog::GetInstance()->GetTableWithName(
            node->GetDatabaseName(), node->GetTableName());
    // Update table alias map
    table_alias_set_.insert(StringUtil::Lower(std::string(node->GetTableAlias())));
    // Construct logical operator
    auto get_expr = std::make_shared<OperatorExpression>(
        LogicalGet::make(target_table, node->GetTableAlias()));
    output_expr = get_expr;
  }
}

void QueryToOperatorTransformer::Visit(const parser::GroupByDescription *) {}
void QueryToOperatorTransformer::Visit(const parser::OrderDescription *) {}
void QueryToOperatorTransformer::Visit(const parser::LimitDescription *) {}

void QueryToOperatorTransformer::Visit(
    UNUSED_ATTRIBUTE const parser::CreateStatement *op) {}
void QueryToOperatorTransformer::Visit(const parser::InsertStatement *op) {
  storage::DataTable *target_table =
      catalog::Catalog::GetInstance()->GetTableWithName(op->GetDatabaseName(),
                                                        op->GetTableName());

  auto insert_expr = std::make_shared<OperatorExpression>(
      LogicalInsert::make(target_table, op->columns, op->insert_values));

  output_expr = insert_expr;
}
void QueryToOperatorTransformer::Visit(const parser::DeleteStatement *op) {
  auto target_table = catalog::Catalog::GetInstance()->GetTableWithName(
      op->GetDatabaseName(), op->GetTableName());
  auto table_scan = std::make_shared<OperatorExpression>(
      LogicalGet::make(target_table, op->GetTableName()));
  auto delete_expr =
      std::make_shared<OperatorExpression>(LogicalDelete::make(target_table));
  delete_expr->PushChild(table_scan);

  output_expr = delete_expr;
}
void QueryToOperatorTransformer::Visit(
    UNUSED_ATTRIBUTE const parser::DropStatement *op) {}
void QueryToOperatorTransformer::Visit(
    UNUSED_ATTRIBUTE const parser::PrepareStatement *op) {}
void QueryToOperatorTransformer::Visit(
    UNUSED_ATTRIBUTE const parser::ExecuteStatement *op) {}
void QueryToOperatorTransformer::Visit(
    UNUSED_ATTRIBUTE const parser::TransactionStatement *op) {}
void QueryToOperatorTransformer::Visit(const parser::UpdateStatement *op) {
  auto target_table = catalog::Catalog::GetInstance()->GetTableWithName(
      op->table->GetDatabaseName(), op->table->GetTableName());

  auto update_expr = std::make_shared<OperatorExpression>(
      LogicalUpdate::make(target_table, *op->updates));

  auto table_scan = std::make_shared<OperatorExpression>(
      LogicalGet::make(target_table, op->table->GetTableName(), true));

  update_expr->PushChild(table_scan);

  output_expr = update_expr;
}
void QueryToOperatorTransformer::Visit(
    UNUSED_ATTRIBUTE const parser::CopyStatement *op) {}

} /* namespace optimizer */
} /* namespace peloton */
