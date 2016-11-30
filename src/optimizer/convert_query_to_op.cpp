//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// convert_query_to_op.cpp
//
// Identification: src/optimizer/convert_query_to_op.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cmath>

#include "expression/expression_util.h"

#include "optimizer/convert_query_to_op.h"
#include "optimizer/operators.h"
#include "optimizer/query_node_visitor.h"

#include "planner/order_by_plan.h"
#include "planner/projection_plan.h"
#include "planner/seq_scan_plan.h"

#include "parser/statements.h"

#include "catalog/catalog.h"
#include "catalog/manager.h"

namespace peloton {
namespace optimizer {

namespace {

class QueryToOpTransformer : public QueryNodeVisitor {
 public:
  QueryToOpTransformer(ColumnManager &manager) : manager(manager) {}

  std::shared_ptr<OperatorExpression> ConvertToOpExpression(
      parser::SQLStatement *op) {
    op->Accept(this);
    return output_expr;
  }

  void visit(UNUSED_ATTRIBUTE const Table *op) override {}

  void visit(const Join *op) override {
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

  void visit(const OrderBy *op) override { (void)op; }

  void Visit(const parser::SelectStatement *op) override {
    // Construct the logical get operator to visit the target table
    storage::DataTable *target_table =
        catalog::Catalog::GetInstance()->GetTableWithName(
            op->from_table->GetDatabaseName(), op->from_table->GetTableName());

    auto get_expr =
        std::make_shared<OperatorExpression>(LogicalGet::make(target_table));

    // Check whether we need to add a logical project operator
    bool needs_projection = false;
    for (auto col : *op->select_list) {
      if (col->GetExpressionType() != EXPRESSION_TYPE_STAR) {
        needs_projection = true;
        break;
      }
    }

    if (!needs_projection) {
      output_expr = get_expr;
      return;
    }

    // Add a projection at top level
    auto project_expr = std::make_shared<OperatorExpression>(LogicalProject::make());
    project_expr->PushChild(get_expr);
    output_expr = project_expr;
  }
  void Visit(UNUSED_ATTRIBUTE const parser::CreateStatement *op) override {}
  void Visit(UNUSED_ATTRIBUTE const parser::InsertStatement *op) override {}
  void Visit(UNUSED_ATTRIBUTE const parser::DeleteStatement *op) override {}
  void Visit(UNUSED_ATTRIBUTE const parser::DropStatement *op) override {}
  void Visit(UNUSED_ATTRIBUTE const parser::PrepareStatement *op) override {}
  void Visit(UNUSED_ATTRIBUTE const parser::ExecuteStatement *op) override {}
  void Visit(UNUSED_ATTRIBUTE const parser::TransactionStatement *op) override {
  }
  void Visit(UNUSED_ATTRIBUTE const parser::UpdateStatement *op) override {}
  void Visit(UNUSED_ATTRIBUTE const parser::CopyStatement *op) override {}

 private:
  ColumnManager &manager;

  std::shared_ptr<OperatorExpression> output_expr;
  // For expr nodes
  common::Type::TypeId output_type;
  int output_size;
  bool output_inlined;
};
}

std::shared_ptr<OperatorExpression> ConvertQueryToOpExpression(
    ColumnManager &manager, parser::SQLStatement *tree) {
  QueryToOpTransformer converter(manager);
  return converter.ConvertToOpExpression(tree);
}

} /* namespace optimizer */
} /* namespace peloton */
