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

bool IsCompareOp(ExpressionType et) {
  switch (et) {
    case (EXPRESSION_TYPE_COMPARE_EQUAL):
    case (EXPRESSION_TYPE_COMPARE_NOTEQUAL):
    case (EXPRESSION_TYPE_COMPARE_LESSTHAN):
    case (EXPRESSION_TYPE_COMPARE_GREATERTHAN):
    case (EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO):
    case (EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO):
    case (EXPRESSION_TYPE_COMPARE_LIKE):
    case (EXPRESSION_TYPE_COMPARE_NOTLIKE):
    case (EXPRESSION_TYPE_COMPARE_IN):
      return true;
    default:
      return false;
  }
}

class QueryToOpTransformer : public QueryNodeVisitor {
 public:
  QueryToOpTransformer(ColumnManager &manager) : manager(manager) {}

  std::shared_ptr<OpExpression> ConvertToOpExpression(
      parser::SQLStatement *op) {
    op->Accept(this);
    return output_expr;
  }

  void visit(const Variable *op) override {
    catalog::Column schema_col = op->column;

    oid_t table_oid = op->base_table_oid;
    oid_t col_id = op->column_index;
    assert(col_id != INVALID_OID);

    Column *col = manager.LookupColumn(table_oid, col_id);
    if (col == nullptr) {
      col = manager.AddBaseColumn(schema_col.GetType(), schema_col.GetLength(),
                                  schema_col.GetName(), schema_col.IsInlined(),
                                  table_oid, col_id);
    }

    output_expr = std::make_shared<OpExpression>(ExprVariable::make(col));
    output_type = col->Type();
    output_size = common::Type::GetTypeSize(output_type);
    output_inlined = col->Inlined();
  }

  void visit(const Constant *op) override {
    output_expr = std::make_shared<OpExpression>(ExprConstant::make(op->value));
    output_type = op->value.GetTypeId();
    output_size = common::Type::GetTypeSize(output_type);
    output_inlined = op->value.IsInlined();
  }

  void visit(const OperatorExpression *op) override {
    std::shared_ptr<OpExpression> expr;
    if (IsCompareOp(op->type)) {
      expr = std::make_shared<OpExpression>(ExprCompare::make(op->type));
    } else {
      expr = std::make_shared<OpExpression>(
          ExprOp::make(op->type, op->value_type));
    }

    for (QueryExpression *arg : op->args) {
      arg->accept(this);
      expr->PushChild(output_expr);
    }

    if (IsCompareOp(op->type)) {
      output_type = common::Type::BOOLEAN;
      output_size = common::Type::GetTypeSize(common::Type::BOOLEAN);
      output_inlined = true;
    } else {
      output_type = op->value_type;
      output_size = common::Type::GetTypeSize(output_type);
      // TODO(abpoms): how can this be determined?
      output_inlined = true;
    }

    output_expr = expr;
  }

  void visit(const AndOperator *op) override {
    auto expr =
        std::make_shared<OpExpression>(ExprBoolOp::make(BoolOpType::And));

    for (QueryExpression *arg : op->args) {
      arg->accept(this);
      expr->PushChild(output_expr);
    }

    output_expr = expr;
    output_type = common::Type::BOOLEAN;
    output_size = common::Type::GetTypeSize(common::Type::BOOLEAN);
    output_inlined = true;
  }

  void visit(const OrOperator *op) override {
    auto expr =
        std::make_shared<OpExpression>(ExprBoolOp::make(BoolOpType::Or));

    for (QueryExpression *arg : op->args) {
      arg->accept(this);
      expr->PushChild(output_expr);
    }

    output_expr = expr;
    output_type = common::Type::BOOLEAN;
    output_size = common::Type::GetTypeSize(common::Type::BOOLEAN);
    output_inlined = true;
  }

  void visit(const NotOperator *op) override {
    auto expr =
        std::make_shared<OpExpression>(ExprBoolOp::make(BoolOpType::Not));

    QueryExpression *arg = op->arg;
    arg->accept(this);
    expr->PushChild(output_expr);

    output_expr = expr;
    output_type = common::Type::BOOLEAN;
    output_size = common::Type::GetTypeSize(common::Type::BOOLEAN);
    output_inlined = true;
  }

  void visit(const Attribute *op) override {
    op->expression->accept(this);

    // TODO(abpoms): actually figure out what the type should be by deriving
    // it from the expression tree
    if (output_size == 0) {
      output_size = std::pow(2, 16);
    }
    Column *col = manager.AddExprColumn(output_type, output_size, op->name,
                                        output_inlined);
    auto expr = std::make_shared<OpExpression>(ExprProjectColumn::make(col));
    expr->PushChild(output_expr);

    output_expr = expr;
  }

  void visit(UNUSED_ATTRIBUTE const Table *op) override {}

  void visit(const Join *op) override {
    // Self
    std::shared_ptr<OpExpression> expr;
    switch (op->join_type) {
      case JOIN_TYPE_INNER: {
        expr = std::make_shared<OpExpression>(LogicalInnerJoin::make());
      } break;
      case JOIN_TYPE_LEFT: {
        expr = std::make_shared<OpExpression>(LogicalLeftJoin::make());
      } break;
      case JOIN_TYPE_RIGHT: {
        expr = std::make_shared<OpExpression>(LogicalRightJoin::make());
      } break;
      case JOIN_TYPE_OUTER: {
        expr = std::make_shared<OpExpression>(LogicalOuterJoin::make());
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

  void visit(const Select *op) override {
    // Add join tree op expression
    op->join_tree->accept(this);
    std::shared_ptr<OpExpression> join_expr = output_expr;

    // Add filter for where predicate
    if (op->where_predicate) {
      auto select_expr = std::make_shared<OpExpression>(LogicalFilter::make());
      select_expr->PushChild(join_expr);
      op->where_predicate->accept(this);
      select_expr->PushChild(output_expr);
      join_expr = select_expr;
    }

    // Add all attributes in output list as projection at top level
    auto project_expr = std::make_shared<OpExpression>(LogicalProject::make());
    project_expr->PushChild(join_expr);
    auto project_list = std::make_shared<OpExpression>(ExprProjectList::make());
    project_expr->PushChild(project_list);
    for (Attribute *attr : op->output_list) {
      // Ignore intermediate columns for output projection
      if (!attr->intermediate) {
        attr->accept(this);
        project_list->PushChild(output_expr);
      }
    }

    output_expr = project_expr;
  }

  void Visit(const parser::SelectStatement *op) override {
    // Construct the logical get operator to visit the target table
    storage::DataTable *target_table =
        catalog::Catalog::GetInstance()->GetTableWithName(
            op->from_table->GetDatabaseName(), op->from_table->GetTableName());

    auto get_expr =
        std::make_shared<OpExpression>(LogicalGet::make(target_table));

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
    auto project_expr = std::make_shared<OpExpression>(LogicalProject::make());
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

  std::shared_ptr<OpExpression> output_expr;
  // For expr nodes
  common::Type::TypeId output_type;
  int output_size;
  bool output_inlined;
};
}

std::shared_ptr<OpExpression> ConvertQueryToOpExpression(
    ColumnManager &manager, parser::SQLStatement *tree) {
  QueryToOpTransformer converter(manager);
  return converter.ConvertToOpExpression(tree);
}

} /* namespace optimizer */
} /* namespace peloton */
