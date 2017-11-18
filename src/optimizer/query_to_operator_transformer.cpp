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
#include "settings/settings_manager.h"

#include "catalog/database_catalog.h"
#include "expression/expression_util.h"
#include "expression/subquery_expression.h"
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
    concurrency::TransactionContext *txn)
    : txn_(txn),
      get_id(0),
      enable_predicate_push_down_(settings::SettingsManager::GetBool(
          settings::SettingId::predicate_push_down)) {}
std::shared_ptr<OperatorExpression>
QueryToOperatorTransformer::ConvertToOpExpression(parser::SQLStatement *op) {
  output_expr_ = nullptr;
  op->Accept(this);
  return output_expr_;
}

void QueryToOperatorTransformer::Visit(parser::SelectStatement *op) {
  // We do not visit the select list of a base table because the column
  // information is derived before the plan generation, at this step we
  // don't need to derive that
  auto pre_predicates = std::move(predicates_);
  auto pre_depth = depth_;
  depth_ = op->depth;

  if (op->from_table != nullptr) {
    // SELECT with FROM
    op->from_table->Accept(this);
  } else {
    // SELECT without FROM
    output_expr_ = std::make_shared<OperatorExpression>(LogicalGet::make());
  }

  if (op->where_clause != nullptr) {
    CollectPredicates(op->where_clause.get());
  }

  if (!predicates_.empty()) {
    auto filter_expr =
        std::make_shared<OperatorExpression>(LogicalFilter::make(predicates_));
    filter_expr->PushChild(output_expr_);
    output_expr_ = filter_expr;
  }

  if (RequireAggregation(op)) {
    // Plain aggregation
    std::shared_ptr<OperatorExpression> agg_expr;
    if (op->group_by == nullptr) {
      agg_expr = std::make_shared<OperatorExpression>(
          LogicalAggregateAndGroupBy::make());
    } else {
      size_t num_group_by_cols = op->group_by->columns.size();
      auto group_by_cols =
          std::vector<std::shared_ptr<expression::AbstractExpression>>(
              num_group_by_cols);
      for (size_t i = 0; i < num_group_by_cols; i++) {
        group_by_cols[i] = std::shared_ptr<expression::AbstractExpression>(
            op->group_by->columns[i]->Copy());
      }
      std::vector<AnnotatedExpression> having;
      if (op->group_by->having != nullptr) {
        util::ExtractPredicates(op->group_by->having.get(), having);
      }
      agg_expr = std::make_shared<OperatorExpression>(
          LogicalAggregateAndGroupBy::make(group_by_cols, having));
    }
    agg_expr->PushChild(output_expr_);
    output_expr_ = agg_expr;
  }

  if (op->select_distinct) {
    auto distinct_expr =
        std::make_shared<OperatorExpression>(LogicalDistinct::make());
    distinct_expr->PushChild(output_expr_);
    output_expr_ = distinct_expr;
  }

  if (op->limit != nullptr) {
    auto limit_expr = std::make_shared<OperatorExpression>(
        LogicalLimit::make(op->limit->offset, op->limit->limit));
    limit_expr->PushChild(output_expr_);
    output_expr_ = limit_expr;
  }

  predicates_ = std::move(pre_predicates);
  depth_ = pre_depth;
}
void QueryToOperatorTransformer::Visit(parser::JoinDefinition *node) {
  // Get left operator
  node->left->Accept(this);
  auto left_expr = output_expr_;

  // Get right operator
  node->right->Accept(this);
  auto right_expr = output_expr_;

  // Construct join operator
  std::shared_ptr<OperatorExpression> join_expr;
  switch (node->type) {
    case JoinType::INNER: {
      CollectPredicates(node->condition.get());
      join_expr =
          std::make_shared<OperatorExpression>(LogicalInnerJoin::make());
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
  if (node->select != nullptr) {
    // Store previous context

    // Construct query derived table predicates
    // i.e. the mapping from column name to the underlying expression in the
    // sub-query. This is needed to generate input/output information for
    // subqueries
    auto table_alias = StringUtil::Lower(node->GetTableAlias());
    auto alias_to_expr_map =
        util::ConstructSelectElementMap(node->select->select_list);

    node->select->Accept(this);

    auto alias = StringUtil::Lower(node->GetTableAlias());

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
    auto prev_expr = output_expr_;
    // Build a left deep join tree
    for (size_t i = 1; i < node->list.size(); i++) {
      node->list.at(i)->Accept(this);
      auto join_expr =
          std::make_shared<OperatorExpression>(LogicalInnerJoin::make());
      join_expr->PushChild(prev_expr);
      join_expr->PushChild(output_expr_);
      PL_ASSERT(join_expr->Children().size() == 2);
      prev_expr = join_expr;
    }
    output_expr_ = prev_expr;
  }
  // Single table
  else {
    if (node->list.size() == 1) node = node->list.at(0).get();
    std::shared_ptr<catalog::TableCatalogObject> target_table =
        catalog::Catalog::GetInstance()->GetTableObject(
            node->GetDatabaseName(), node->GetTableName(), txn_);
    std::string table_alias =
        StringUtil::Lower(std::string(node->GetTableAlias()));
    output_expr_ = std::make_shared<OperatorExpression>(LogicalGet::make(
        GetAndIncreaseGetId(), {}, target_table, node->GetTableAlias()));
  }
}

void QueryToOperatorTransformer::Visit(parser::GroupByDescription *) {}
void QueryToOperatorTransformer::Visit(parser::OrderDescription *) {}
void QueryToOperatorTransformer::Visit(parser::LimitDescription *) {}
void QueryToOperatorTransformer::Visit(parser::CreateFunctionStatement *) {}

void QueryToOperatorTransformer::Visit(
    UNUSED_ATTRIBUTE parser::CreateStatement *op) {}
void QueryToOperatorTransformer::Visit(parser::InsertStatement *op) {
  std::shared_ptr<catalog::TableCatalogObject> target_table =
      catalog::Catalog::GetInstance()
          ->GetDatabaseObject(op->GetDatabaseName(), txn_)
          ->GetTableObject(op->GetTableName());
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
  auto target_table = catalog::Catalog::GetInstance()
                          ->GetDatabaseObject(op->GetDatabaseName(), txn_)
                          ->GetTableObject(op->GetTableName());
  std::shared_ptr<OperatorExpression> table_scan;
  if (op->expr != nullptr) {
    std::vector<AnnotatedExpression> predicates;
    util::ExtractPredicates(op->expr.get(), predicates);
    table_scan = std::make_shared<OperatorExpression>(LogicalGet::make(
        GetAndIncreaseGetId(), predicates, target_table, op->GetTableName()));
  } else
    table_scan = std::make_shared<OperatorExpression>(LogicalGet::make(
        GetAndIncreaseGetId(), {}, target_table, op->GetTableName()));
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
  auto target_table =
      catalog::Catalog::GetInstance()
          ->GetDatabaseObject(op->table->GetDatabaseName(), txn_)
          ->GetTableObject(op->table->GetTableName());
  std::shared_ptr<OperatorExpression> table_scan;

  auto update_expr = std::make_shared<OperatorExpression>(
      LogicalUpdate::make(target_table, &op->updates));

  if (op->where != nullptr) {
    std::vector<AnnotatedExpression> predicates;
    util::ExtractPredicates(op->where.get(), predicates);
    table_scan = std::make_shared<OperatorExpression>(
        LogicalGet::make(GetAndIncreaseGetId(), predicates, target_table,
                         op->table->GetTableName(), true));
  } else
    table_scan = std::make_shared<OperatorExpression>(
        LogicalGet::make(GetAndIncreaseGetId(), {}, target_table,
                         op->table->GetTableName(), true));

  update_expr->PushChild(table_scan);

  output_expr_ = update_expr;
}
void QueryToOperatorTransformer::Visit(
    UNUSED_ATTRIBUTE parser::CopyStatement *op) {}
void QueryToOperatorTransformer::Visit(
    UNUSED_ATTRIBUTE parser::AnalyzeStatement *op) {}

void QueryToOperatorTransformer::Visit(expression::ComparisonExpression *expr) {
  auto expr_type = expr->GetExpressionType();
  if (expr->GetExpressionType() == ExpressionType::COMPARE_IN) {
    std::vector<expression::AbstractExpression *> select_list;
    if (GenerateSubquerytree(expr->GetModifiableChild(1), select_list) ==
        true) {
      if (select_list.size() != 1) {
        throw Exception("Array in predicates not supported");
      }

      // Set the right child as the output of the subquery
      expr->SetChild(1, select_list.at(0)->Copy());
      expr->SetExpressionType(ExpressionType::COMPARE_EQUAL);
    }

  } else if (expr_type == ExpressionType::COMPARE_EQUAL ||
             expr_type == ExpressionType::COMPARE_GREATERTHAN ||
             expr_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO ||
             expr_type == ExpressionType::COMPARE_LESSTHAN ||
             expr_type == ExpressionType::COMPARE_LESSTHANOREQUALTO) {
    std::vector<expression::AbstractExpression *> select_list;
    if (GenerateSubquerytree(expr->GetModifiableChild(0), select_list, true) ==
        true) {
      if (select_list.size() != 1) {
        throw Exception("Array in predicates not supported");
      }

      // Set the left child as the output of the subquery
      expr->SetChild(0, select_list.at(0)->Copy());
    }
    select_list.clear();
    if (GenerateSubquerytree(expr->GetModifiableChild(1), select_list, true) ==
        true) {
      if (select_list.size() != 1) {
        throw Exception("Array in predicates not supported");
      }

      // Set the right child as the output of the subquery
      expr->SetChild(1, select_list.at(0)->Copy());
    }
  }
  expr->AcceptChildren(this);
}

void QueryToOperatorTransformer::Visit(expression::OperatorExpression *expr) {
  if (expr->GetExpressionType() == ExpressionType::OPERATOR_EXISTS) {
    std::vector<expression::AbstractExpression *> select_list;
    if (GenerateSubquerytree(expr->GetModifiableChild(0), select_list) ==
        true) {
      PL_ASSERT(!select_list.empty());

      // Set the right child as the output of the subquery
      expr->SetExpressionType(ExpressionType::OPERATOR_IS_NOT_NULL);
      expr->SetChild(0, select_list.at(0)->Copy());
    }
  }

  expr->AcceptChildren(this);
}

bool QueryToOperatorTransformer::RequireAggregation(
    const parser::SelectStatement *op) {
  if (op->group_by != nullptr) {
    return true;
  }
  // Check plain aggregation
  bool has_aggregation = false;
  bool has_other_exprs = false;
  for (auto &expr : op->getSelectList()) {
    std::vector<expression::AggregateExpression *> aggr_exprs;
    expression::ExpressionUtil::GetAggregateExprs(aggr_exprs, expr.get());
    if (!aggr_exprs.empty()) {
      has_aggregation = true;
    } else {
      has_other_exprs = true;
    }
  }
  // TODO: Should be handled in the binder
  // Syntax error when there are mixture of aggregation and other exprs
  // when group by is absent
  if (has_aggregation && has_other_exprs) {
    throw SyntaxException(
        "Non aggregation expression must appear in the GROUP BY "
        "clause or be used in an aggregate function");
  }
  return has_aggregation;
}

void QueryToOperatorTransformer::CollectPredicates(
    expression::AbstractExpression *expr) {
  expr->Accept(this);
  util::ExtractPredicates(expr, predicates_);
}

bool QueryToOperatorTransformer::GenerateSubquerytree(
    expression::AbstractExpression *expr,
    std::vector<expression::AbstractExpression *> &select_list,
    bool single_join) {
  if (expr->GetExpressionType() != ExpressionType::ROW_SUBQUERY) {
    return false;
  }
  auto subquery_expr = dynamic_cast<expression::SubqueryExpression *>(expr);
  auto sub_select = subquery_expr->GetSubSelect();

  for (auto &ele : sub_select->select_list) {
    select_list.push_back(ele.get());
  }

  // Construct join
  std::shared_ptr<OperatorExpression> op_expr;
  if (single_join) {
    op_expr = std::make_shared<OperatorExpression>(LogicalSingleJoin::make());
  } else {
    op_expr = std::make_shared<OperatorExpression>(LogicalMarkJoin::make());
  }

  // Push previous output
  op_expr->PushChild(output_expr_);

  sub_select->Accept(this);

  // Push subquery output
  op_expr->PushChild(output_expr_);

  output_expr_ = op_expr;
  return true;
}

}  // namespace optimizer
}  // namespace peloton
