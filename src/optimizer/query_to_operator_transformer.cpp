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
#include <include/settings/settings_manager.h>

#include "expression/expression_util.h"

#include "optimizer/operator_expression.h"
#include "optimizer/operators.h"
#include "optimizer/query_node_visitor.h"
#include "optimizer/query_to_operator_transformer.h"

#include "planner/seq_scan_plan.h"

#include "parser/statements.h"

#include "catalog/manager.h"

using std::vector;
using std::shared_ptr;

namespace peloton {
namespace optimizer {
QueryToOperatorTransformer::QueryToOperatorTransformer(
    concurrency::Transaction *txn)
    : txn_(txn), get_id(0),
      enable_predicate_push_down_(settings::SettingsManager::GetBool(settings::SettingId::predicate_push_down)) {}
std::shared_ptr<OperatorExpression>
QueryToOperatorTransformer::ConvertToOpExpression(parser::SQLStatement *op) {
  output_expr_ = nullptr;
  op->Accept(this);
  return output_expr_;
}

void QueryToOperatorTransformer::Visit(parser::SelectStatement *op) {
  if (op->where_clause != nullptr) {
    // Extract single table predicates and join predicates from the where clause
    util::ExtractPredicates(op->where_clause.get(), single_table_predicates_map,
                            join_predicates_);
  }

  if (op->from_table != nullptr) {
    // SELECT with FROM
    op->from_table->Accept(this);
    if (op->group_by != nullptr) {
      // Make copies of groupby columns
      vector<shared_ptr<expression::AbstractExpression>> group_by_cols;
      for (auto &col : op->group_by->columns)
        group_by_cols.emplace_back(col->Copy());
      auto group_by = std::make_shared<OperatorExpression>(LogicalGroupBy::make(
          move(group_by_cols), op->group_by->having.get()));
      group_by->PushChild(output_expr_);
      output_expr_ = group_by;
    } else {
      // Check plain aggregation
      bool has_aggregation = false;
      bool has_other_exprs = false;
      for (auto &expr : op->getSelectList()) {
        vector<shared_ptr<expression::AggregateExpression>> aggr_exprs;
        expression::ExpressionUtil::GetAggregateExprs(aggr_exprs, expr.get());
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
        aggregate->PushChild(output_expr_);
        output_expr_ = aggregate;
      }
    }
  } else {
    // SELECT without FROM
    output_expr_ = std::make_shared<OperatorExpression>(LogicalGet::make());
  }
}
void QueryToOperatorTransformer::Visit(parser::JoinDefinition *node) {
  // Get left operator
  node->left->Accept(this);
  auto left_expr = output_expr_;
  auto left_table_alias_set = table_alias_set_;
  // If not do this, when traversing the right subtree, table_alias_set will
  // not be empty, which is incorrect.
  table_alias_set_.clear();

  // Get right operator
  node->right->Accept(this);
  auto right_expr = output_expr_;
  util::SetUnion(table_alias_set_, left_table_alias_set);

  // Construct join operator
  std::shared_ptr<OperatorExpression> join_expr;
  switch (node->type) {
    case JoinType::INNER: {
      if (node->condition != nullptr) {
        // Add join condition into join predicates
        std::unordered_set<std::string> join_condition_table_alias_set;
        expression::ExpressionUtil::GenerateTableAliasSet(
            node->condition.get(), join_condition_table_alias_set);
        join_predicates_.emplace_back(
            AnnotatedExpression(std::shared_ptr<expression::AbstractExpression>(
                                    node->condition->Copy()),
                                join_condition_table_alias_set));
      }
      // Based on the set of all table alias in the subtree, extract those
      // join predicates that applies to this join.
      join_expr = std::make_shared<OperatorExpression>(LogicalInnerJoin::make(
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

  output_expr_ = join_expr;
}
void QueryToOperatorTransformer::Visit(parser::TableRef *node) {
  // Nested select. Not supported in the current executors
  if (node->select != nullptr) {
    // Store previous context
    auto pre_join_predicates = join_predicates_;
    auto pre_single_table_predicates_map = single_table_predicates_map;
    auto pre_table_alias_set = table_alias_set_;
    join_predicates_.clear();
    single_table_predicates_map.clear();
    table_alias_set_.clear();

    // Construct query derived table predicates
    auto table_alias = StringUtil::Lower(node->GetTableAlias());
    auto alias_to_expr_map =
        util::ConstructSelectElementMap(node->select->select_list);
    auto predicates = pre_single_table_predicates_map[table_alias];
    std::vector<expression::AbstractExpression *> transformed_predicates;
    for (auto &original_predicate : predicates) {
      util::ExtractPredicates(util::TransformQueryDerivedTablePredicates(
                                  alias_to_expr_map, original_predicate.get()),
                              single_table_predicates_map, join_predicates_);
    }

    node->select->Accept(this);

    auto alias = StringUtil::Lower(node->GetTableAlias());
    pre_table_alias_set.insert(alias);
    join_predicates_ = pre_join_predicates;
    single_table_predicates_map = pre_single_table_predicates_map;
    table_alias_set_ = pre_table_alias_set;

    auto child_expr = output_expr_;
    output_expr_ =
        std::make_shared<OperatorExpression>(LogicalQueryDerivedGet::make(
            GetAndIncreaseGetId(), alias, alias_to_expr_map));
    output_expr_->PushChild(child_expr);

  }
  // Explicit Join
  else if (node->join != nullptr) {
    node->join->Accept(this);
  }
  // Multiple tables (Implicit Join)
  else if (node->list.size() > 1) {
    // Create a join operator between the first two tables
    node->list.at(0)->Accept(this);
    auto left_expr = output_expr_;
    auto left_table_alias_set = table_alias_set_;
    table_alias_set_.clear();

    node->list.at(1)->Accept(this);
    auto right_expr = output_expr_;
    util::SetUnion(table_alias_set_, left_table_alias_set);

    auto join_expr =
        std::make_shared<OperatorExpression>(LogicalInnerJoin::make(
            util::ConstructJoinPredicate(table_alias_set_, join_predicates_)));
    join_expr->PushChild(left_expr);
    join_expr->PushChild(right_expr);

    // Build a left deep join tree
    for (size_t i = 2; i < node->list.size(); i++) {
      node->list.at(i)->Accept(this);
      auto old_join_expr = join_expr;
      join_expr = std::make_shared<OperatorExpression>(LogicalInnerJoin::make(
          util::ConstructJoinPredicate(table_alias_set_, join_predicates_)));
      join_expr->PushChild(old_join_expr);
      join_expr->PushChild(output_expr_);
    }
    output_expr_ = join_expr;
  }
  // Single table
  else {
    if (node->list.size() == 1) node = node->list.at(0).get();
    storage::DataTable *target_table =
        catalog::Catalog::GetInstance()->GetTableWithName(
            node->GetDatabaseName(), node->GetTableName(), txn_);
    std::string table_alias =
        StringUtil::Lower(std::string(node->GetTableAlias()));
    // Update table alias map
    table_alias_set_.insert(table_alias);
    // Construct logical operator
    auto predicates_entry = single_table_predicates_map.find(table_alias);
    if (predicates_entry != single_table_predicates_map.end())
      output_expr_ = std::make_shared<OperatorExpression>(LogicalGet::make(
          GetAndIncreaseGetId(), target_table, node->GetTableAlias(),
          std::shared_ptr<expression::AbstractExpression>(
              util::CombinePredicates(predicates_entry->second))));
    else
      output_expr_ = std::make_shared<OperatorExpression>(LogicalGet::make(
          GetAndIncreaseGetId(), target_table, node->GetTableAlias()));
  }
}

void QueryToOperatorTransformer::Visit(parser::GroupByDescription *) {}
void QueryToOperatorTransformer::Visit(parser::OrderDescription *) {}
void QueryToOperatorTransformer::Visit(parser::LimitDescription *) {}

void QueryToOperatorTransformer::Visit(
    UNUSED_ATTRIBUTE parser::CreateStatement *op) {}
void QueryToOperatorTransformer::Visit(parser::InsertStatement *op) {
  storage::DataTable *target_table =
      catalog::Catalog::GetInstance()->GetTableWithName(
          op->GetDatabaseName(), op->GetTableName(), txn_);
  if (op->type == InsertType::SELECT) {
    auto insert_expr = std::make_shared<OperatorExpression>(
        LogicalInsertSelect::make(target_table));
    op->select->Accept(this);
    insert_expr->PushChild(output_expr_);
    output_expr_ = insert_expr;
  } else {
    auto insert_expr = std::make_shared<OperatorExpression>(
        LogicalInsert::make(target_table, &op->columns, &op->insert_values));
    output_expr_ = insert_expr;
  }
}

void QueryToOperatorTransformer::Visit(parser::DeleteStatement *op) {
  auto target_table = catalog::Catalog::GetInstance()->GetTableWithName(
      op->GetDatabaseName(), op->GetTableName(), txn_);
  std::shared_ptr<OperatorExpression> table_scan;
  if (op->expr != nullptr)
    table_scan = std::make_shared<OperatorExpression>(LogicalGet::make(
        GetAndIncreaseGetId(), target_table, op->GetTableName(),
        std::shared_ptr<expression::AbstractExpression>(op->expr->Copy())));
  else
    table_scan = std::make_shared<OperatorExpression>(LogicalGet::make(
        GetAndIncreaseGetId(), target_table, op->GetTableName()));
  auto delete_expr =
      std::make_shared<OperatorExpression>(LogicalDelete::make(target_table));
  delete_expr->PushChild(table_scan);

  output_expr_ = delete_expr;
}
void QueryToOperatorTransformer::Visit(
    UNUSED_ATTRIBUTE parser::DropStatement *op) {}
void QueryToOperatorTransformer::Visit(
    UNUSED_ATTRIBUTE parser::PrepareStatement *op) {}
void QueryToOperatorTransformer::Visit(
    UNUSED_ATTRIBUTE parser::ExecuteStatement *op) {}
void QueryToOperatorTransformer::Visit(
    UNUSED_ATTRIBUTE parser::TransactionStatement *op) {}
void QueryToOperatorTransformer::Visit(parser::UpdateStatement *op) {
  auto target_table = catalog::Catalog::GetInstance()->GetTableWithName(
      op->table->GetDatabaseName(), op->table->GetTableName(), txn_);
  std::shared_ptr<OperatorExpression> table_scan;

  auto update_expr = std::make_shared<OperatorExpression>(
      LogicalUpdate::make(target_table, &op->updates));

  if (op->where != nullptr)
    table_scan = std::make_shared<OperatorExpression>(LogicalGet::make(
        GetAndIncreaseGetId(), target_table, op->table->GetTableName(),
        std::shared_ptr<expression::AbstractExpression>(op->where->Copy()),
        true));
  else
    table_scan = std::make_shared<OperatorExpression>(
        LogicalGet::make(GetAndIncreaseGetId(), target_table,
                         op->table->GetTableName(), nullptr, true));

  update_expr->PushChild(table_scan);

  output_expr_ = update_expr;
}
void QueryToOperatorTransformer::Visit(
    UNUSED_ATTRIBUTE parser::CopyStatement *op) {}
void QueryToOperatorTransformer::Visit(
    UNUSED_ATTRIBUTE parser::AnalyzeStatement *op) {}

}  // namespace optimizer
}  // namespace peloton
