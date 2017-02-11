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
#include <include/parser/table_ref.h>

#include "expression/expression_util.h"

#include "optimizer/operator_expression.h"
#include "optimizer/operators.h"
#include "optimizer/column_manager.h"
#include "optimizer/query_node_visitor.h"
#include "optimizer/query_to_operator_transformer.h"

#include "planner/order_by_plan.h"
#include "planner/projection_plan.h"
#include "planner/seq_scan_plan.h"

#include "parser/statements.h"
#include "parser/select_statement.h"

#include "catalog/catalog.h"
#include "catalog/manager.h"

namespace peloton {
namespace optimizer {

QueryToOperatorTransformer::QueryToOperatorTransformer(ColumnManager &manager)
    : manager_(manager) {}

std::shared_ptr<OperatorExpression>
QueryToOperatorTransformer::ConvertToOpExpression(parser::SQLStatement *op) {
  op->Accept(this);
  output_expr = nullptr;
  return output_expr;
}

void QueryToOperatorTransformer::Visit(const parser::SelectStatement *op) {
  // Construct the logical get operator to visit the target table
  if (op->from_table != nullptr) {
    op->from_table->Accept(this);
  }
}
void QueryToOperatorTransformer::Visit(const parser::TableRef* node) {
  // Nested select. Not supported in the current executors
  if (node->select != nullptr)
    node->select->Accept(this);
  // Join
  else if (node->join != nullptr)
    node->join->Accept(this);
  // Multiple tables
  else if (node->list != nullptr) {
    for (parser::TableRef* table:*(node->list))
      table->Accept(this);
  }
  // Single table
  else {
    storage::DataTable *target_table =
        catalog::Catalog::GetInstance()->GetTableWithName(
            node->GetDatabaseName(),
            node->GetTableName());
    // Construct logical operator
    auto get_expr =
        std::make_shared<OperatorExpression>(LogicalGet::make(target_table));
    output_expr = get_expr;

    // Get the columns required by the result
    auto table_oid = target_table->GetOid();
    auto db_oid = target_table->GetDatabaseOid();
    auto schema = target_table->GetSchema();
    auto schema_cols = schema->GetColumns();
    for (size_t col_id = 0; col_id < schema_cols.size(); col_id++) {
      Column *col = manager_.LookupColumn(table_oid, col_id);
      if (col == nullptr) {
        auto schema_col = schema_cols[col_id];
        manager_.AddBaseColumn(schema_col.GetType(), schema_col.GetLength(),
                                 schema_col.GetName(), schema_col.IsInlined(),
                                 table_oid, col_id);
      }
    }
    manager_.AddTable(db_oid, table_oid, node);
  }

}
void QueryToOperatorTransformer::Visit(const parser::JoinDefinition* node) {
  node->left->Accept(this);
  auto left_expr = output_expr;
  node->right->Accept(this);
  auto right_expr = output_expr;

  std::shared_ptr<OperatorExpression> join_expr;
  switch (node->type) {
    case JoinType::INNER: {
      join_expr = std::make_shared<OperatorExpression>(LogicalInnerJoin::make());
      break;
    }
    case JoinType::OUTER: {
      join_expr = std::make_shared<OperatorExpression>(LogicalOuterJoin::make());
      break;
    }
    case JoinType::LEFT: {
      join_expr = std::make_shared<OperatorExpression>(LogicalLeftJoin::make());
      break;
    }
    case JoinType::RIGHT: {
      join_expr = std::make_shared<OperatorExpression>(LogicalRightJoin::make());
      break;
    }
    case JoinType::SEMI: {
      join_expr = std::make_shared<OperatorExpression>(LogicalSemiJoin::make());
      break;
    }
    default:
      throw Exception("Join type invalid");
      break;
  }
  join_expr->PushChild(left_expr);
  join_expr->PushChild(right_expr);

  output_expr = join_expr;

}
void QueryToOperatorTransformer::Visit(const parser::GroupByDescription *) {}
void QueryToOperatorTransformer::Visit(const parser::OrderDescription *) {}
void QueryToOperatorTransformer::Visit(const parser::LimitDescription *) {}

void QueryToOperatorTransformer::Visit(
    UNUSED_ATTRIBUTE const parser::CreateStatement *op) {}
void QueryToOperatorTransformer::Visit(
    UNUSED_ATTRIBUTE const parser::InsertStatement *op) {}
void QueryToOperatorTransformer::Visit(
    UNUSED_ATTRIBUTE const parser::DeleteStatement *op) {}
void QueryToOperatorTransformer::Visit(
    UNUSED_ATTRIBUTE const parser::DropStatement *op) {}
void QueryToOperatorTransformer::Visit(
    UNUSED_ATTRIBUTE const parser::PrepareStatement *op) {}
void QueryToOperatorTransformer::Visit(
    UNUSED_ATTRIBUTE const parser::ExecuteStatement *op) {}
void QueryToOperatorTransformer::Visit(
    UNUSED_ATTRIBUTE const parser::TransactionStatement *op) {}
void QueryToOperatorTransformer::Visit(
    UNUSED_ATTRIBUTE const parser::UpdateStatement *op) {}
void QueryToOperatorTransformer::Visit(
    UNUSED_ATTRIBUTE const parser::CopyStatement *op) {}

} /* namespace optimizer */
} /* namespace peloton */
