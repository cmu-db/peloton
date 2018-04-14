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

#include "catalog/column_catalog.h"
#include "catalog/database_catalog.h"
#include "catalog/table_catalog.h"
#include "expression/expression_util.h"
#include "expression/subquery_expression.h"
#include "optimizer/operator_expression.h"
#include "optimizer/operators.h"
#include "optimizer/query_node_visitor.h"
#include "optimizer/query_to_operator_transformer.h"
#include "parser/statements.h"
#include "settings/settings_manager.h"

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

  if (op->from_table != nullptr) {
    // SELECT with FROM
    op->from_table->Accept(this);
  } else {
    // SELECT without FROM
    output_expr_ = std::make_shared<OperatorExpression>(LogicalGet::make());
  }

  if (op->where_clause != nullptr) {
    predicates_ = CollectPredicates(op->where_clause.get(), predicates_);
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
      // TODO(boweic): aggregation without groupby could still have having
      // clause
      agg_expr = std::make_shared<OperatorExpression>(
          LogicalAggregateAndGroupBy::make());
      agg_expr->PushChild(output_expr_);
      output_expr_ = agg_expr;
    } else {
      size_t num_group_by_cols = op->group_by->columns.size();
      auto group_by_cols =
          std::vector<std::shared_ptr<expression::AbstractExpression>>(
              num_group_by_cols);
      for (size_t i = 0; i < num_group_by_cols; i++) {
        group_by_cols[i] = std::shared_ptr<expression::AbstractExpression>(
            op->group_by->columns[i]->Copy());
      }
      agg_expr = std::make_shared<OperatorExpression>(
          LogicalAggregateAndGroupBy::make(group_by_cols));
      agg_expr->PushChild(output_expr_);
      output_expr_ = agg_expr;
      std::vector<AnnotatedExpression> having;
      if (op->group_by->having != nullptr) {
        having = CollectPredicates(op->group_by->having.get());
      }
      if (!having.empty()) {
        auto filter_expr =
            std::make_shared<OperatorExpression>(LogicalFilter::make(having));
        filter_expr->PushChild(output_expr_);
        output_expr_ = filter_expr;
      }
    }
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
      predicates_ = CollectPredicates(node->condition.get(), predicates_);
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
      PELOTON_ASSERT(join_expr->Children().size() == 2);
      prev_expr = join_expr;
    }
    output_expr_ = prev_expr;
  }
  // Single table
  else {
    if (node->list.size() == 1) node = node->list.at(0).get();
    std::shared_ptr<catalog::TableCatalogObject> target_table =
        catalog::Catalog::GetInstance()->GetTableObject(
            node->GetDatabaseName(), node->GetSchemaName(),
            node->GetTableName(), txn_);
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
      catalog::Catalog::GetInstance()->GetTableObject(
          op->GetDatabaseName(), op->GetSchemaName(), op->GetTableName(), txn_);

  if (op->type == InsertType::SELECT) {
    auto insert_expr = std::make_shared<OperatorExpression>(
        LogicalInsertSelect::make(target_table));
    op->select->Accept(this);
    insert_expr->PushChild(output_expr_);
    output_expr_ = insert_expr;
    return;
  }
  // column_objects represents the columns for the current table as defined in
  // its schema
  auto column_objects = target_table->GetColumnObjects();
  // INSERT INTO table_name VALUES (val1, val2, ...), (val_a, val_b, ...), ...
  if (op->columns.empty()) {
    for (const auto &values : op->insert_values) {
      if (values.size() > column_objects.size()) {
        throw CatalogException(
            "ERROR:  INSERT has more expressions than target columns");
      } else if (values.size() < column_objects.size()) {
        for (oid_t i = values.size(); i != column_objects.size(); ++i) {
          // check whether null values are allowed in the rest of the columns
          if (column_objects[i]->IsNotNull()) {
            // TODO: Add check for default value's existence for the current
            // column
            throw CatalogException(
                StringUtil::Format("ERROR:  null value in column \"%s\" "
                                   "violates not-null constraint",
                                   column_objects[i]->GetColumnName().c_str()));
          }
        }
      }
    }
  } else {
    // INSERT INTO table_name (col1, col2, ...) VALUES (val1, val2, ...), ...
    auto num_columns = op->columns.size();
    for (const auto &tuple : op->insert_values) {  // check size of each tuple
      if (tuple.size() > num_columns) {
        throw CatalogException(
            "ERROR:  INSERT has more expressions than target columns");
      } else if (tuple.size() < num_columns) {
        throw CatalogException(
            "ERROR:  INSERT has more target columns than expressions");
      }
    }

    // set below contains names of columns mentioned in the insert statement
    std::unordered_set<std::string> specified;
    auto column_names = target_table->GetColumnNames();

    for (const auto col : op->columns) {
      if (column_names.find(col) == column_names.end()) {
        throw CatalogException(StringUtil::Format(
            "ERROR:  column \"%s\" of relation \"%s\" does not exist",
            col.c_str(), target_table->GetTableName().c_str()));
      }
      specified.insert(col);
    }

    for (auto column : column_names) {
      // this loop checks not null constraint for unspecified columns
      if (specified.find(column.first) == specified.end() &&
          column.second->IsNotNull()) {
        // TODO: Add check for default value's existence for the current column
        throw CatalogException(StringUtil::Format(
            "ERROR:  null value in column \"%s\" violates not-null constraint",
            column.first.c_str()));
      }
    }
  }
  auto insert_expr = std::make_shared<OperatorExpression>(
      LogicalInsert::make(target_table, &op->columns, &op->insert_values));
  output_expr_ = insert_expr;
}

void QueryToOperatorTransformer::Visit(parser::DeleteStatement *op) {
  auto target_table = catalog::Catalog::GetInstance()->GetTableObject(
      op->GetDatabaseName(), op->GetSchemaName(), op->GetTableName(), txn_);
  std::shared_ptr<OperatorExpression> table_scan;
  if (op->expr != nullptr) {
    std::vector<AnnotatedExpression> predicates =
        util::ExtractPredicates(op->expr.get());
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
  auto target_table = catalog::Catalog::GetInstance()->GetTableObject(
      op->table->GetDatabaseName(), op->table->GetSchemaName(),
      op->table->GetTableName(), txn_);
  std::shared_ptr<OperatorExpression> table_scan;

  auto update_expr = std::make_shared<OperatorExpression>(
      LogicalUpdate::make(target_table, &op->updates));

  if (op->where != nullptr) {
    std::vector<AnnotatedExpression> predicates =
        util::ExtractPredicates(op->where.get());
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
    if (GenerateSubquerytree(expr, 1)) {
      // TODO(boweic): Should use IN to preserve the semantic, for now we do not
      // have semi-join so use = to transform into inner join
      expr->SetExpressionType(ExpressionType::COMPARE_EQUAL);
    }

  } else if (expr_type == ExpressionType::COMPARE_EQUAL ||
             expr_type == ExpressionType::COMPARE_GREATERTHAN ||
             expr_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO ||
             expr_type == ExpressionType::COMPARE_LESSTHAN ||
             expr_type == ExpressionType::COMPARE_LESSTHANOREQUALTO) {
    if (expr->GetChild(0)->GetExpressionType() ==
            ExpressionType::ROW_SUBQUERY &&
        expr->GetChild(1)->GetExpressionType() ==
            ExpressionType::ROW_SUBQUERY) {
      throw Exception("Do not support comparison between sub-select");
    }
    // Transform if either child is sub-query
    GenerateSubquerytree(expr, 0, true) || GenerateSubquerytree(expr, 1, true);
  }
  expr->AcceptChildren(this);
}

void QueryToOperatorTransformer::Visit(expression::OperatorExpression *expr) {
  // TODO(boweic): We may want to do the rewrite (exist -> in) in the binder
  if (expr->GetExpressionType() == ExpressionType::OPERATOR_EXISTS) {
    if (GenerateSubquerytree(expr, 0)) {
      // Already reset the child to column, we need to transform exist to
      // not-null to preserve semantic
      expr->SetExpressionType(ExpressionType::OPERATOR_IS_NOT_NULL);
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

std::vector<AnnotatedExpression> QueryToOperatorTransformer::CollectPredicates(
    expression::AbstractExpression *expr,
    std::vector<AnnotatedExpression> predicates) {
  // First check if all conjunctive predicates are supported before
  // transfoming
  // predicate with sub-select into regular predicates
  std::vector<expression::AbstractExpression *> predicate_ptrs;
  util::SplitPredicates(expr, predicate_ptrs);
  for (const auto &pred : predicate_ptrs) {
    if (!IsSupportedConjunctivePredicate(pred)) {
      throw Exception("Predicate type not supported yet");
    }
  }
  // Accept will change the expression, e.g. (a in (select b from test)) into
  // (a IN test.b), after the rewrite, we can extract the table aliases
  // information correctly
  expr->Accept(this);
  return util::ExtractPredicates(expr, predicates);
}

bool QueryToOperatorTransformer::IsSupportedConjunctivePredicate(
    expression::AbstractExpression *expr) {
  // Currently support : 1. expr without subquery
  // 2. subquery without disjunction. Since the expr is already one of the
  // conjunctive exprs, we'll only need to check if the root level is an
  // operator with subquery
  if (!expr->HasSubquery()) {
    return true;
  }
  auto expr_type = expr->GetExpressionType();
  // Subquery with IN
  if (expr_type == ExpressionType::COMPARE_IN &&
      expr->GetChild(0)->GetExpressionType() != ExpressionType::ROW_SUBQUERY &&
      expr->GetChild(1)->GetExpressionType() == ExpressionType::ROW_SUBQUERY) {
    return true;
  }
  // Subquery with EXIST
  if (expr_type == ExpressionType::OPERATOR_EXISTS &&
      expr->GetChild(0)->GetExpressionType() == ExpressionType::ROW_SUBQUERY) {
    return true;
  }
  // Subquery with other operator
  if (expr_type == ExpressionType::COMPARE_EQUAL ||
      expr_type == ExpressionType::COMPARE_GREATERTHAN ||
      expr_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO ||
      expr_type == ExpressionType::COMPARE_LESSTHAN ||
      expr_type == ExpressionType::COMPARE_LESSTHANOREQUALTO) {
    // Supported if one child is subquery and the other is not
    if ((!expr->GetChild(0)->HasSubquery() &&
         expr->GetChild(1)->GetExpressionType() ==
             ExpressionType::ROW_SUBQUERY) ||
        (!expr->GetChild(1)->HasSubquery() &&
         expr->GetChild(0)->GetExpressionType() ==
             ExpressionType::ROW_SUBQUERY)) {
      return true;
    }
  }
  return false;
}

bool QueryToOperatorTransformer::IsSupportedSubSelect(
    const parser::SelectStatement *op) {
  // Supported if 1. No aggregation. 2. With aggregation and WHERE clause only
  // have correlated columns in conjunctive predicates in the form of
  // "outer_relation.a = ..."
  // TODO(boweic): Add support for arbitary expressions, this would require
  // the
  // support for mark join & some special operators, see Hyper's unnesting
  // arbitary query slides
  if (!RequireAggregation(op)) {
    return true;
  }

  std::vector<expression::AbstractExpression *> predicates;
  util::SplitPredicates(op->where_clause.get(), predicates);
  for (const auto &pred : predicates) {
    // Depth is used to detect correlated subquery, it is set in the binder,
    // if a TVE has depth less than the depth of the current operator, then it
    // is correlated predicate
    if (pred->GetDepth() < op->depth) {
      if (pred->GetExpressionType() != ExpressionType::COMPARE_EQUAL) {
        return false;
      }
      // Check if in the form of
      // "outer_relation.a = (expr with only columns in inner relation)"
      if (!((pred->GetChild(1)->GetDepth() == op->depth &&
             pred->GetChild(0)->GetExpressionType() ==
                 ExpressionType::VALUE_TUPLE) ||
            (pred->GetChild(0)->GetDepth() == op->depth &&
             pred->GetChild(1)->GetExpressionType() ==
                 ExpressionType::VALUE_TUPLE))) {
        return false;
      }
    }
  }
  return true;
}

bool QueryToOperatorTransformer::GenerateSubquerytree(
    expression::AbstractExpression *expr, oid_t child_id, bool single_join) {
  // Get potential subquery
  auto subquery_expr = expr->GetChild(child_id);
  if (subquery_expr->GetExpressionType() != ExpressionType::ROW_SUBQUERY) {
    return false;
  }
  auto sub_select =
      static_cast<const expression::SubqueryExpression *>(subquery_expr)
          ->GetSubSelect()
          .get();
  if (!IsSupportedSubSelect(sub_select)) {
    throw Exception("Sub-select not supported");
  }
  // We only support subselect with single row
  if (sub_select->select_list.size() != 1) {
    throw Exception("Array in predicates not supported");
  }
  std::vector<expression::AbstractExpression *> select_list;
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
  // Convert subquery to the selected column in the sub-select
  expr->SetChild(child_id, sub_select->select_list.at(0)->Copy());
  return true;
}

}  // namespace optimizer
}  // namespace peloton
