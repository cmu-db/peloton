//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// convert_op_to_plan.cpp
//
// Identification: src/optimizer/convert_op_to_plan.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/convert_op_to_plan.h"
#include "optimizer/operator_visitor.h"

#include "planner/hash_join_plan.h"
#include "planner/nested_loop_join_plan.h"
#include "planner/seq_scan_plan.h"
#include "planner/projection_plan.h"

#include "expression/expression_util.h"

namespace peloton {
namespace optimizer {

namespace {

std::tuple<oid_t, oid_t> FindRelativeIndex(
  const std::vector<Column *> &left_columns,
  const std::vector<Column *> &right_columns,
  const Column *column)
{
  assert(!(left_columns.empty() && right_columns.empty()));
  for (size_t i = 0; i < left_columns.size(); ++i) {
    if (column->ID() == left_columns[i]->ID()) {
      return std::make_tuple(0, i);
    }
  }
  for (size_t i = 0; i < right_columns.size(); ++i) {
    if (column->ID() == right_columns[i]->ID()) {
      return std::make_tuple(1, i);
    }
  }
  return std::make_tuple(INVALID_OID, INVALID_OID);
}

class ExprOpToAbstractExpressionTransformer : public OperatorVisitor {
 public:
  ExprOpToAbstractExpressionTransformer(
    const std::vector<Column *> &left_columns,
    const std::vector<Column *> &right_columns)
    : left_columns(left_columns), right_columns(right_columns) {
  }

  expression::AbstractExpression *ConvertOpExpression(
    std::shared_ptr<OpExpression> op)
  {
    VisitOpExpression(op);
    return output_expr;
  }

  void visit(const ExprVariable *var) override {
    assert(!(left_columns.empty() && right_columns.empty()));
    oid_t table_idx = INVALID_OID;
    oid_t column_idx = INVALID_OID;
    std::tie(table_idx, column_idx) =
      FindRelativeIndex(left_columns, right_columns, var->column);
    output_expr =
      expression::ExpressionUtil::TupleValueFactory(VALUE_TYPE_NULL,table_idx, column_idx);
  }

  void visit(const ExprConstant *op) override {
    output_expr =
      expression::ExpressionUtil::ConstantValueFactory(op->value);
  }

  void visit(const ExprCompare *op) override {
    auto children = current_children;
    assert(children.size() == 2);

    std::vector<expression::AbstractExpression *> child_exprs;
    for (std::shared_ptr<OpExpression> child : children) {
      VisitOpExpression(child);
      child_exprs.push_back(output_expr);
    }
    child_exprs.resize(2, nullptr);

    output_expr = expression::ExpressionUtil::ComparisonFactory(
      op->expr_type, child_exprs[0], child_exprs[1]);
  }

  void visit(const ExprBoolOp *op) override {
    auto children = current_children;

    std::list<expression::AbstractExpression *> child_exprs;
    for (std::shared_ptr<OpExpression> child : children) {
      VisitOpExpression(child);
      child_exprs.push_back(output_expr);
    }

    switch (op->bool_type) {
    case BoolOpType::Not: {
      assert(children.size() == 1);
      output_expr = expression::ExpressionUtil::OperatorFactory(
        EXPRESSION_TYPE_OPERATOR_NOT,VALUE_TYPE_NULL, child_exprs.front(), nullptr);
    } break;
    case BoolOpType::And: {
      assert(children.size() > 0);
      output_expr = expression::ExpressionUtil::ConjunctionFactory(
        EXPRESSION_TYPE_CONJUNCTION_AND, child_exprs);
    } break;
    case BoolOpType::Or: {
      assert(children.size() > 0);
      output_expr = expression::ExpressionUtil::ConjunctionFactory(
        EXPRESSION_TYPE_CONJUNCTION_OR, child_exprs);
    } break;
    default: {
      assert(false);
    }
    }
    assert(output_expr != nullptr);
  }

  void visit(const ExprOp *op) override {
    auto children = current_children;

    std::vector<expression::AbstractExpression *> child_exprs;
    for (std::shared_ptr<OpExpression> child : children) {
      VisitOpExpression(child);
      child_exprs.push_back(output_expr);
    }
    child_exprs.resize(4, nullptr);

    output_expr = expression::ExpressionUtil::OperatorFactory(
      op->expr_type,
	  VALUE_TYPE_NULL,
      child_exprs[0],
      child_exprs[1],
      child_exprs[2],
      child_exprs[3]);
  }

 private:
  void VisitOpExpression(std::shared_ptr<OpExpression> op) {
    std::vector<std::shared_ptr<OpExpression>> prev_children = current_children;
    current_children = op->Children();
    op->Op().accept(this);
    current_children = prev_children;
  }

  expression::AbstractExpression *output_expr;
  std::vector<std::shared_ptr<OpExpression>> current_children;
  std::vector<Column *> left_columns;
  std::vector<Column *> right_columns;
};

class OpToPlanTransformer : public OperatorVisitor {
 public:
  OpToPlanTransformer() {
  }

  planner::AbstractPlan *ConvertOpExpression(
    std::shared_ptr<OpExpression> plan)
  {
    VisitOpExpression(plan);
    return output_plan;
  }

  void visit(const PhysicalScan *op) override {
    std::vector<oid_t> column_ids;
    for (Column *col : op->columns) {
      TableColumn *table_col = dynamic_cast<TableColumn *>(col);
      assert(table_col != nullptr);
      column_ids.push_back(table_col->ColumnIndexOid());
    }
    left_columns = op->columns;

    output_columns = op->columns;
    output_plan = new planner::SeqScanPlan(op->table, nullptr, column_ids);
  }

  void visit(const PhysicalComputeExprs *) override {
    auto children = current_children;
    assert(children.size() == 2);

    VisitOpExpression(children[0]);
    planner::AbstractPlan *child_plan = output_plan;
    left_columns = output_columns;

    std::vector<Column *> proj_columns;
    std::vector<expression::AbstractExpression *> exprs;
    {
      for (std::shared_ptr<OpExpression> op_expr : children[1]->Children()) {
        assert(op_expr->Op().type() == OpType::ProjectColumn);
        assert(op_expr->Children().size() == 1);

        const ExprProjectColumn *proj_col =
          op_expr->Op().as<ExprProjectColumn>();
        proj_columns.push_back(proj_col->column);

        exprs.push_back(ConvertToAbstractExpression(op_expr->Children()[0]));
      }
    }
    catalog::Schema *project_schema = BuildSchemaFromColumns(proj_columns);

    // Build projection info from target list
    planner::ProjectInfo *project_info = BuildProjectInfoFromExprs(exprs);

    output_columns = proj_columns;
    output_plan = new planner::ProjectionPlan(project_info, project_schema);
    output_plan->AddChild(child_plan);
  }

  void visit(const PhysicalFilter *) override {
    auto children = current_children;
    assert(children.size() == 2);

    VisitOpExpression(children[0]);
    planner::AbstractPlan *child_plan = output_plan;
    left_columns = output_columns;

    expression::AbstractExpression *predicate =
      ConvertToAbstractExpression(children[1]);

    output_plan = new planner::SeqScanPlan(nullptr, predicate, {});
    output_plan->AddChild(child_plan);
  }

  void visit(const PhysicalInnerNLJoin *) override {
    auto children = current_children;
    assert(children.size() == 3);

    VisitOpExpression(children[0]);
    planner::AbstractPlan *left_child = output_plan;
    left_columns = output_columns;

    VisitOpExpression(children[1]);
    planner::AbstractPlan *right_child = output_plan;
    right_columns = output_columns;

    expression::AbstractExpression *predicate =
      ConvertToAbstractExpression(children[2]);

    output_columns = ConcatLeftAndRightColumns();

    catalog::Schema *project_schema = BuildSchemaFromColumns(output_columns);
    planner::ProjectInfo *proj_info =
      BuildProjectInfoFromColumns(output_columns);

    output_plan = new planner::NestedLoopJoinPlan(
      JOIN_TYPE_INNER, predicate, proj_info, project_schema, nullptr);
    output_plan->AddChild(left_child);
    output_plan->AddChild(right_child);
  }

  void visit(const PhysicalLeftNLJoin *) override {
  }

  void visit(const PhysicalRightNLJoin *) override {
  }

  void visit(const PhysicalOuterNLJoin *) override {
  }

  void visit(const PhysicalInnerHashJoin *) override {
    // auto children = current_children;
    // assert(children.size() == 2);

    // VisitOpExpression(children[0]);
    // planner::AbstractPlan *lef_child = output_plan;
    // left_columns = output_columns;

    // VisitOpExpression(children[1]);
    // planner::AbstractPlan *right_child = output_plan;
    // right_columns = output_columns;

    // planner::AbstractPlan *right_hash =
    //   new planner::HashPlan();
  }

  void visit(const PhysicalLeftHashJoin *) override {
  }

  void visit(const PhysicalRightHashJoin *) override {
  }

  void visit(const PhysicalOuterHashJoin *) override {
  }

 private:
  void VisitOpExpression(std::shared_ptr<OpExpression> op) {
    std::vector<std::shared_ptr<OpExpression>> prev_children = current_children;
    auto prev_left_cols = left_columns;
    auto prev_right_cols = right_columns;
    current_children = op->Children();
    op->Op().accept(this);
    current_children = prev_children;
    left_columns = prev_left_cols;
    right_columns = prev_right_cols;
  }

  expression::AbstractExpression *ConvertToAbstractExpression(
    std::shared_ptr<OpExpression> op)
  {
    return ConvertOpExpressionToAbstractExpression(op,
                                                   left_columns,
                                                   right_columns);
  }

  catalog::Schema *BuildSchemaFromColumns(
    std::vector<Column *> columns)
  {
    std::vector<catalog::Column> schema_columns;
    for (Column *column : columns) {
      schema_columns.push_back(GetSchemaColumnFromOptimizerColumn(column));
    }
    return new catalog::Schema(schema_columns);
  }

  planner::ProjectInfo *BuildProjectInfoFromColumns(
    std::vector<Column *> columns)
  {
    DirectMapList dm_list;
    for (size_t col_id = 0; col_id < columns.size(); ++col_id) {
      Column *column = columns[col_id];

      oid_t table_idx = INVALID_OID;
      oid_t column_idx = INVALID_OID;
      std::tie(table_idx, column_idx) =
        FindRelativeIndex(left_columns, right_columns, column);

      dm_list.push_back({col_id, {table_idx, column_idx}});
    }
    return new planner::ProjectInfo({}, std::move(dm_list));
  }

  planner::ProjectInfo *BuildProjectInfoFromExprs(
    std::vector<expression::AbstractExpression *> exprs)
  {
    TargetList target_list;
    for (size_t col_id = 0; col_id < exprs.size(); ++col_id) {
      target_list.push_back({col_id, exprs[col_id]});
    }
    return new planner::ProjectInfo(std::move(target_list), {});
  }

  std::vector<Column *> ConcatLeftAndRightColumns() {
    std::vector<Column *> columns = left_columns;
    columns.insert(columns.end(), right_columns.begin(), right_columns.end());
    return columns;
  }

  planner::AbstractPlan *output_plan;
  std::vector<std::shared_ptr<OpExpression>> current_children;
  std::vector<Column *> output_columns;
  std::vector<Column *> left_columns;
  std::vector<Column *> right_columns;
};

}

expression::AbstractExpression *ConvertOpExpressionToAbstractExpression(
  std::shared_ptr<OpExpression> op_expr,
  std::vector<Column *> left_columns,
  std::vector<Column *> right_columns)
{
  ExprOpToAbstractExpressionTransformer transformer(left_columns,
                                                    right_columns);
  return transformer.ConvertOpExpression(op_expr);
}

planner::AbstractPlan *ConvertOpExpressionToPlan(
  std::shared_ptr<OpExpression> plan)
{
  OpToPlanTransformer transformer;
  return transformer.ConvertOpExpression(plan);
}

} /* namespace optimizer */
} /* namespace peloton */
