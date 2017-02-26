//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// operator_to_plan_transformer.cpp
//
// Identification: src/optimizer/operator_to_plan_transformer.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "expression/expression_util.h"

#include "optimizer/operator_expression.h"
#include "optimizer/operator_to_plan_transformer.h"
#include "optimizer/properties.h"
#include "planner/hash_join_plan.h"
#include "planner/nested_loop_join_plan.h"
#include "planner/projection_plan.h"
#include "planner/seq_scan_plan.h"
#include "planner/order_by_plan.h"
#include <tuple>

namespace peloton {
namespace optimizer {

OperatorToPlanTransformer::OperatorToPlanTransformer() {}

std::unique_ptr<planner::AbstractPlan>
OperatorToPlanTransformer::ConvertOpExpression(
    std::shared_ptr<OperatorExpression> plan, PropertySet *requirements,
    std::vector<PropertySet> *required_input_props) {
  requirements_ = requirements;
  required_input_props_ = required_input_props;
  VisitOpExpression(plan);
  return std::move(output_plan_);
}

void OperatorToPlanTransformer::Visit(const PhysicalScan *op) {
  std::vector<oid_t> column_ids;

  // Scan predicates
  auto predicate_prop =
      requirements_->GetPropertyOfType(PropertyType::PREDICATE)
          ->As<PropertyPredicate>();

  expression::AbstractExpression *predicate = nullptr;
  if (predicate_prop != nullptr) {
    predicate = predicate_prop->GetPredicate()->Copy();
  }

  // Scan columns
  auto column_prop = requirements_->GetPropertyOfType(PropertyType::COLUMNS)
                         ->As<PropertyColumns>();

  PL_ASSERT(column_prop != nullptr);

  for (size_t column_idx = 0; column_idx < column_prop->GetSize();
       column_idx++) {
    auto col = column_prop->GetColumn(column_idx);
    oid_t id = std::get<2>(col->bound_obj_id);
    column_ids.push_back(id);
  }

  output_plan_.reset(
      new planner::SeqScanPlan(op->table_, predicate, column_ids));




}

void OperatorToPlanTransformer::Visit(const PhysicalProject *) {
  auto project_prop = requirements_->GetPropertyOfType(PropertyType::PROJECT)
                          ->As<PropertyProjection>();
  (void)project_prop;

  size_t project_list_size = project_prop->GetProjectionListSize();

  // expressions to evaluate
  TargetList tl = TargetList();
  // columns which can be returned directly
  DirectMapList dml = DirectMapList();
  // schema of the projections output
  std::vector<catalog::Column> columns;

  for (size_t project_idx = 0; project_idx < project_list_size; project_idx++) {
    auto expr = project_prop->GetProjection(project_idx);
    std::string column_name;

    // if the root of the expression is a column value we can
    // just do a direct mapping
    if (expr->GetExpressionType() == ExpressionType::VALUE_TUPLE) {
      auto tup_expr = (expression::TupleValueExpression *)expr;
      column_name = tup_expr->GetColumnName();
      dml.push_back(
          DirectMap(project_idx, std::make_pair(0, tup_expr->GetColumnId())));
    }
    // otherwise we need to evaluat the expression
    else {
      column_name = "expr" + std::to_string(project_idx);
      tl.push_back(Target(project_idx, expr->Copy()));
    }
    columns.push_back(catalog::Column(
        expr->GetValueType(), type::Type::GetTypeSize(expr->GetValueType()),
        column_name));
  }
  // build the projection plan node and insert aboce the scan
  std::unique_ptr<planner::ProjectInfo> proj_info(
      new planner::ProjectInfo(std::move(tl), std::move(dml)));
  std::shared_ptr<catalog::Schema> schema_ptr(new catalog::Schema(columns));
  std::unique_ptr<planner::AbstractPlan> project_plan(
      new planner::ProjectionPlan(std::move(proj_info), schema_ptr));

  output_plan_ = std::move(project_plan);
}

void OperatorToPlanTransformer::Visit(const PhysicalOrderBy *op) {
  // Order by
  PL_ASSERT(output_plan_->GetPlanNodeType() == PlanNodeType::SEQSCAN || output_plan_->GetPlanNodeType() == PlanNodeType::INDEXSCAN);
  auto child_scan_plan = static_cast<planner::AbstractScan *>(output_plan_.get());

  auto sort_prop = op->property_sort;
  std::vector<oid_t> sort_col_ids;
  std::vector<bool> sort_flags;
  for (size_t column_idx = 0; column_idx < sort_prop->GetSortColumnSize();
       column_idx++) {
    auto col = sort_prop->GetSortColumn(column_idx);
    sort_col_ids.emplace_back(std::get<2>(col->bound_obj_id));
    sort_flags.push_back(sort_prop->GetSortAscending(column_idx));
  }
  auto order_by_plan = new planner::OrderByPlan(sort_col_ids, sort_flags, child_scan_plan->GetColumnIds());
  order_by_plan->AddChild(std::move(output_plan_));
  output_plan_.reset(order_by_plan);

}
void OperatorToPlanTransformer::Visit(const PhysicalFilter *) {}

void OperatorToPlanTransformer::Visit(const PhysicalInnerNLJoin *) {}

void OperatorToPlanTransformer::Visit(const PhysicalLeftNLJoin *) {}

void OperatorToPlanTransformer::Visit(const PhysicalRightNLJoin *) {}

void OperatorToPlanTransformer::Visit(const PhysicalOuterNLJoin *) {}

void OperatorToPlanTransformer::Visit(const PhysicalInnerHashJoin *) {}

void OperatorToPlanTransformer::Visit(const PhysicalLeftHashJoin *) {}

void OperatorToPlanTransformer::Visit(const PhysicalRightHashJoin *) {}

void OperatorToPlanTransformer::Visit(const PhysicalOuterHashJoin *) {}

void OperatorToPlanTransformer::VisitOpExpression(
    std::shared_ptr<OperatorExpression> op) {
  op->Op().Accept(this);
}

} /* namespace optimizer */
} /* namespace peloton */
