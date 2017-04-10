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

namespace peloton {
namespace optimizer {

QueryToOperatorTransformer::QueryToOperatorTransformer(ColumnManager &manager)
    : manager_(manager) {}

std::shared_ptr<OperatorExpression>
QueryToOperatorTransformer::ConvertToOpExpression(parser::SQLStatement *op) {
  output_expr = nullptr;
  op->Accept(this);
  return output_expr;
}

void QueryToOperatorTransformer::Visit(const parser::SelectStatement *op) {
  auto upper_expr = output_expr;
  if (op->from_table != nullptr) op->from_table->Accept(this);
  if (op->group_by != nullptr) {
    auto aggregate = std::make_shared<OperatorExpression>(
        LogicalAggregate::make(op->group_by->columns, op->group_by->having));
    aggregate->PushChild(output_expr);
    output_expr = aggregate;
  }
  if (op->limit != nullptr) {
    // When offset is not specified in the query, parser will set offset to -1
    if (op->limit->offset == -1)
      op->limit->offset = 0;
    auto limit = std::make_shared<OperatorExpression>(
        LogicalLimit::make(op->limit->limit, op->limit->offset));
    limit->PushChild(output_expr);
    output_expr = limit;
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

  // Get right operator
  node->right->Accept(this);
  auto right_expr = output_expr;

  // Construct join operator
  std::shared_ptr<OperatorExpression> join_expr;
  switch (node->type) {
    case JoinType::INNER: {
      join_expr = std::make_shared<OperatorExpression>(
          LogicalInnerJoin::make(node->condition));
      break;
    }
    case JoinType::OUTER: {
      join_expr = std::make_shared<OperatorExpression>(
          LogicalOuterJoin::make(node->condition));
      break;
    }
    case JoinType::LEFT: {
      join_expr = std::make_shared<OperatorExpression>(
          LogicalLeftJoin::make(node->condition));
      break;
    }
    case JoinType::RIGHT: {
      join_expr = std::make_shared<OperatorExpression>(
          LogicalRightJoin::make(node->condition));
      break;
    }
    case JoinType::SEMI: {
      join_expr = std::make_shared<OperatorExpression>(
          LogicalSemiJoin::make(node->condition));
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
  if (node->select != nullptr) node->select->Accept(this);
  // Join
  else if (node->join != nullptr)
    node->join->Accept(this);
  // Multiple tables
  else if (node->list != nullptr && node->list->size() > 1) {
    std::shared_ptr<OperatorExpression> join_expr = nullptr;
    std::shared_ptr<OperatorExpression> next_join_expr = nullptr;

    // Construct join sequences with Cartesian products
    for (parser::TableRef *table : *(node->list)) {
      if (join_expr == nullptr) {
        join_expr =
            std::make_shared<OperatorExpression>(LogicalInnerJoin::make());
      } else {
        next_join_expr =
            std::make_shared<OperatorExpression>(LogicalInnerJoin::make());
        next_join_expr->PushChild(join_expr);
        join_expr = next_join_expr;
      }
      table->Accept(this);
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
    // Construct logical operator
    auto get_expr = std::make_shared<OperatorExpression>(
        LogicalGet::make(target_table, node->GetTableAlias()));
    output_expr = get_expr;
  }
}

// Not support ORDER BY in sub-queries
void QueryToOperatorTransformer::Visit(const parser::GroupByDescription *) {}
void QueryToOperatorTransformer::Visit(const parser::OrderDescription *) {}
void QueryToOperatorTransformer::Visit(const parser::LimitDescription *) {}

void QueryToOperatorTransformer::Visit(
    UNUSED_ATTRIBUTE const parser::CreateStatement *op) {}
void QueryToOperatorTransformer::Visit(
    const parser::InsertStatement *op) {
  storage::DataTable* target_table =
      catalog::Catalog::GetInstance()->GetTableWithName(
          op->GetDatabaseName(), op->GetTableName());

  auto insert_expr =
      std::make_shared<OperatorExpression>(
          LogicalInsert::make(target_table, op->columns, op->insert_values));

  output_expr = insert_expr;
}
void QueryToOperatorTransformer::Visit(
    const parser::DeleteStatement *op) {
  auto target_table = catalog::Catalog::GetInstance()->GetTableWithName(
      op->GetDatabaseName(), op->GetTableName());
  auto table_scan =
      std::make_shared<OperatorExpression>(
          LogicalGet::make(target_table, op->GetTableName()));
  auto delete_expr =
      std::make_shared<OperatorExpression>(LogicalDelete::make(target_table));
  delete_expr->PushChild(table_scan);

  // Fills in information about columns and functions i
  // n their respective objects given a set of schemas.
  // Otherwise tuple_idx will equals to -1 for scan plan, causing error.
  if (op->expr != nullptr) {
    expression::ExpressionUtil::TransformExpression(target_table->GetSchema(), op->expr);
  }

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
void QueryToOperatorTransformer::Visit(
    const parser::UpdateStatement *op) {

  auto target_table = catalog::Catalog::GetInstance()->GetTableWithName(
      op->table->GetDatabaseName(),
      op->table->GetTableName());

  auto update_clauses = op->updates;

  for (auto clause : *update_clauses) {
    auto clause_expr = clause->value;
    auto columnID = target_table->GetSchema()->GetColumnID(clause->column);
    auto column = target_table->GetSchema()->GetColumn(columnID);

    if (clause_expr->GetExpressionType() ==
        ExpressionType::VALUE_CONSTANT) {
      auto value = static_cast<const expression::ConstantValueExpression*>(
          clause_expr)
          ->GetValue()
          .CastAs(column.GetType());
      auto value_expression =
          new expression::ConstantValueExpression(value);

      delete clause->value;
      clause->value = value_expression;

    } else {
      for (unsigned int child_index = 0;
           child_index < clause_expr->GetChildrenSize(); child_index++) {
        auto child = clause_expr->GetChild(child_index);

        if (child->GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
          auto value =
              static_cast<const expression::ConstantValueExpression*>(child)
                  ->GetValue()
                  .CastAs(column.GetType());
          auto value_expression =
              new expression::ConstantValueExpression(value);

          clause_expr->SetChild(child_index, value_expression);
        }
      }
    }
  }

  auto update_expr =
      std::make_shared<OperatorExpression>(
          LogicalUpdate::make(op));

  output_expr = update_expr;
}
void QueryToOperatorTransformer::Visit(
    UNUSED_ATTRIBUTE const parser::CopyStatement *op) {}

} /* namespace optimizer */
} /* namespace peloton */
