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

#include "optimizer/convert_query_to_op.h"
#include "optimizer/query_node_visitor.h"
#include "optimizer/operators.h"

#include "planner/projection_plan.h"
#include "planner/seq_scan_plan.h"
#include "planner/order_by_plan.h"

#include "catalog/manager.h"
#include "bridge/ddl/bridge.h"
#include "bridge/dml/mapper/mapper.h"

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
  QueryToOpTransformer(ColumnManager &manager)
    : manager(manager) {
  }

  std::shared_ptr<OpExpression> ConvertToOpExpression(
    std::shared_ptr<Select> op)
  {
    op->accept(this);
    return output_expr;
  }

  void visit(const Variable *op) override {
    catalog::Column schema_col = op->column;

    oid_t table_oid = op->base_table_oid;
    oid_t col_id = op->column_index;
    assert(col_id != INVALID_OID);

    Column *col = manager.LookupColumn(table_oid, col_id);
    if (col == nullptr) {
      col = manager.AddBaseColumn(schema_col.GetType(),
                                  schema_col.GetLength(),
                                  schema_col.GetName(),
                                  schema_col.IsInlined(),
                                  table_oid,
                                  col_id);
    }

    output_expr = std::make_shared<OpExpression>(ExprVariable::make(col));
    output_type = col->Type();
    output_size = GetTypeSize(output_type);
    output_inlined = col->Inlined();
  }

  void visit(const Constant *op) override {
    output_expr = std::make_shared<OpExpression>(ExprConstant::make(op->value));
    output_type = op->value.GetValueType();
    output_size = GetTypeSize(output_type);
    output_inlined = op->value.GetSourceInlined();
  }

  void visit(const OperatorExpression *op) override {
    std::shared_ptr<OpExpression> expr;
    if (IsCompareOp(op->type)) {
      expr = std::make_shared<OpExpression>(ExprCompare::make(op->type));
    } else {
      expr = std::make_shared<OpExpression>(ExprOp::make(op->type,
                                                         op->value_type));
    }

    for (QueryExpression *arg : op->args) {
      arg->accept(this);
      expr->PushChild(output_expr);
    }

    if (IsCompareOp(op->type)) {
      output_type = VALUE_TYPE_BOOLEAN;
      output_size = GetTypeSize(VALUE_TYPE_BOOLEAN);
      output_inlined = true;
    } else {
      output_type = op->value_type;
      output_size = GetTypeSize(output_type);
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
    output_type = VALUE_TYPE_BOOLEAN;
    output_size = GetTypeSize(VALUE_TYPE_BOOLEAN);
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
    output_type = VALUE_TYPE_BOOLEAN;
    output_size = GetTypeSize(VALUE_TYPE_BOOLEAN);
    output_inlined = true;
  }

  void visit(const NotOperator *op) override {
    auto expr =
      std::make_shared<OpExpression>(ExprBoolOp::make(BoolOpType::Not));

    QueryExpression *arg = op->arg;
    arg->accept(this);
    expr->PushChild(output_expr);

    output_expr = expr;
    output_type = VALUE_TYPE_BOOLEAN;
    output_size = GetTypeSize(VALUE_TYPE_BOOLEAN);
    output_inlined = true;
  }

  void visit(const Attribute *op) override {
    op->expression->accept(this);

    // TODO(abpoms): actually figure out what the type should be by deriving
    // it from the expression tree
    if (output_size == 0) {
      output_size = std::pow(2, 16);
    }
    Column *col =
      manager.AddExprColumn(output_type,
                            output_size,
                            op->name,
                            output_inlined);
    auto expr = std::make_shared<OpExpression>(ExprProjectColumn::make(col));
    expr->PushChild(output_expr);

    output_expr = expr;
  }

  void visit(const Table *op) override {
    std::vector<Column *> columns;

    storage::DataTable *table = op->data_table;
    catalog::Schema *schema = table->GetSchema();
    oid_t table_oid = table->GetOid();
    for (oid_t col_id = 0; col_id < schema->GetColumnCount(); col_id++) {
      catalog::Column schema_col = schema->GetColumn(col_id);
      Column *col = manager.LookupColumn(table_oid, col_id);
      if (col == nullptr) {
        col = manager.AddBaseColumn(schema_col.GetType(),
                                    schema_col.GetLength(),
                                    schema_col.GetName(),
                                    schema_col.IsInlined(),
                                    table_oid,
                                    col_id);
      }
      columns.push_back(col);
    }
    output_expr =
      std::make_shared<OpExpression>(LogicalGet::make(op->data_table, columns));
  }

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

  void visit(const OrderBy *op) override {
    (void) op;
  }

  void visit(const Select *op) override {
    // Add join tree op expression
    op->join_tree->accept(this);
    std::shared_ptr<OpExpression> join_expr = output_expr;

    // Add filter for where predicate
    if (op->where_predicate) {
      auto select_expr = std::make_shared<OpExpression>(LogicalSelect::make());
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

 private:
  ColumnManager &manager;

  std::shared_ptr<OpExpression> output_expr;
  // For expr nodes
  ValueType output_type;
  int output_size;
  bool output_inlined;
};

}

std::shared_ptr<OpExpression> ConvertQueryToOpExpression(
  ColumnManager &manager,
  std::shared_ptr<Select> tree)
{
  QueryToOpTransformer converter(manager);
  return converter.ConvertToOpExpression(tree);
}


} /* namespace optimizer */
} /* namespace peloton */
