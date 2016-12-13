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
    : manager(manager) {}

std::shared_ptr<OperatorExpression>
QueryToOperatorTransformer::ConvertToOpExpression(parser::SQLStatement *op) {
  op->Accept(this);
  return output_expr;
}

void QueryToOperatorTransformer::visit(UNUSED_ATTRIBUTE const Table *op) {}

void QueryToOperatorTransformer::visit(const Join *op) {
  // Self
  std::shared_ptr<OperatorExpression> expr;
  switch (op->join_type) {
    case JOIN_TYPE_INNER: {
      expr = std::make_shared<OperatorExpression>(LogicalInnerJoin::make());
    } break;
    case JOIN_TYPE_LEFT: {
      expr = std::make_shared<OperatorExpression>(LogicalLeftJoin::make());
    } break;
    case JOIN_TYPE_RIGHT: {
      expr = std::make_shared<OperatorExpression>(LogicalRightJoin::make());
    } break;
    case JOIN_TYPE_OUTER: {
      expr = std::make_shared<OperatorExpression>(LogicalOuterJoin::make());
    } break;
    default:
      assert(false);
  }

  // Left child
  op->left_node->accept(this);
  expr->PushChild(output_expr);

  // Right child
  op->right_node->accept(this);
  expr->PushChild(output_expr);

  // Join condition predicate
  op->predicate->accept(this);
  expr->PushChild(output_expr);

  output_expr = expr;
}

void QueryToOperatorTransformer::visit(const OrderBy *op) { (void)op; }

void QueryToOperatorTransformer::Visit(const parser::SelectStatement *op) {
  // Construct the logical get operator to visit the target table
  storage::DataTable *target_table =
      catalog::Catalog::GetInstance()->GetTableWithName(
          op->from_table->GetDatabaseName(), op->from_table->GetTableName());

  auto get_expr =
      std::make_shared<OperatorExpression>(LogicalGet::make(target_table));

  output_expr = get_expr;
}
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
