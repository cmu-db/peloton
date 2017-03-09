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

#include "planner/update_plan.h"
#include "expression/expression_util.h"

#include "optimizer/operator_expression.h"
#include "optimizer/operator_to_plan_transformer.h"
#include "optimizer/properties.h"
#include "planner/hash_join_plan.h"
#include "planner/nested_loop_join_plan.h"
#include "planner/projection_plan.h"
#include "planner/seq_scan_plan.h"
#include "planner/order_by_plan.h"
#include "planner/limit_plan.h"
//#include <tuple>

namespace peloton {
namespace optimizer {

OperatorToPlanTransformer::OperatorToPlanTransformer() {}

std::unique_ptr<planner::AbstractPlan>
OperatorToPlanTransformer::ConvertOpExpression(
    std::shared_ptr<OperatorExpression> plan, PropertySet *requirements,
    std::vector<PropertySet> *required_input_props,
    std::vector<std::unique_ptr<planner::AbstractPlan>> &children_plans,
    std::vector<std::vector<std::tuple<oid_t, oid_t, oid_t>>> &
        children_output_columns,
    std::vector<std::tuple<oid_t, oid_t, oid_t>> *output_columns) {
  requirements_ = requirements;
  required_input_props_ = required_input_props;
  children_plans_ = std::move(children_plans);
  children_output_columns_ = std::move(children_output_columns);
  output_columns_ = output_columns;
  VisitOpExpression(plan);
  return std::move(output_plan_);
}

void OperatorToPlanTransformer::Visit(const PhysicalScan *op) {
  std::vector<oid_t> column_ids;
  // LOG_DEBUG("PhysicalScan");
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

  // Add col_ids for SELECT *
  if (column_prop->IsStarExpressionInColumn()) {
    size_t col_num = op->table_->GetSchema()->GetColumnCount();
    auto db_id = op->table_->GetDatabaseOid();
    oid_t table_id = op->table_->GetOid();
    for (oid_t i = 0; i < col_num; ++i) {
      column_ids.push_back(i);
      if (output_columns_ != nullptr)
        output_columns_->emplace_back(std::make_tuple(db_id, table_id, i));
    }
  }
  else {
    for (size_t column_idx = 0; column_idx < column_prop->GetSize();
         column_idx++) {
      auto col = column_prop->GetColumn(column_idx);
      oid_t id = std::get<2>(col->bound_obj_id);
      column_ids.push_back(id);

      // record output column mapping
      if (output_columns_ != nullptr)
        output_columns_->emplace_back(col->bound_obj_id);
    }
  }

  // for (auto &col : column_ids) {
  //  LOG_DEBUG("Output Col : %u", col);
  //}
  output_plan_.reset(
      new planner::SeqScanPlan(op->table_, predicate, column_ids));
}

void OperatorToPlanTransformer::Visit(const PhysicalProject *) {
  auto project_prop = requirements_->GetPropertyOfType(PropertyType::PROJECT)
                          ->As<PropertyProjection>();
  (void)project_prop;
  size_t project_list_size = project_prop->GetProjectionListSize();
  // LOG_DEBUG("PhysicalProject");

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

  PL_ASSERT(children_plans_.size() == 1);
  project_plan->AddChild(std::move(children_plans_[0]));

  output_plan_ = std::move(project_plan);
}

void OperatorToPlanTransformer::Visit(const PhysicalLimit *op) {
  PL_ASSERT(children_plans_.size() == 1);

  // Limit Operator does not change the column mapping
  if (output_columns_ != nullptr)
    *output_columns_ = children_output_columns_[0];

  std::unique_ptr<planner::AbstractPlan> limit_plan(
      new planner::LimitPlan(op->limit, op->offset));
  limit_plan->AddChild(std::move(children_plans_[0]));
  output_plan_ = std::move(limit_plan);
}

void OperatorToPlanTransformer::Visit(const PhysicalOrderBy *op) {
  // LOG_DEBUG("PhysicalOrderBy");

  // Get child plan
  PL_ASSERT(children_plans_.size() == 1);

  auto sort_prop = op->property_sort;
  std::vector<oid_t> sort_col_ids;
  std::vector<bool> sort_flags;
  for (size_t column_idx = 0; column_idx < sort_prop->GetSortColumnSize();
       column_idx++) {
    auto col = sort_prop->GetSortColumn(column_idx);
    // Check which column
    // in the table produced
    // by the child operator
    // is the sort column
    for (oid_t child_col_id = 0;
         child_col_id < children_output_columns_[0].size(); ++child_col_id) {
      if (col->bound_obj_id == children_output_columns_[0][child_col_id]) {
        sort_col_ids.emplace_back(child_col_id);
        break;
      }
    }

    // Planner use desc flag
    sort_flags.push_back(sort_prop->GetSortAscending(column_idx) ^ 1);
    // if (sort_prop->GetSortAscending(column_idx))
    //  LOG_DEBUG("Sort Order : ASC");
  }
  // for (auto &sort_col : sort_col_ids) {
  //  LOG_DEBUG("Sort Col : %u", sort_col);
  //}

  std::vector<oid_t> column_ids;
  // Get output columns
  auto column_prop = requirements_->GetPropertyOfType(PropertyType::COLUMNS)
                         ->As<PropertyColumns>();

  PL_ASSERT(column_prop != nullptr);

  if (column_prop->IsStarExpressionInColumn()) {
    for (oid_t child_col_id = 0;
         child_col_id < children_output_columns_[0].size(); ++child_col_id) {
      column_ids.emplace_back(child_col_id);
      if (output_columns_ != nullptr)
        output_columns_->emplace_back(children_output_columns_[0][child_col_id]);
    }
  }
  else {
    for (size_t column_idx = 0; column_idx < column_prop->GetSize();
         column_idx++) {
      auto col = column_prop->GetColumn(column_idx);
      // transform global column
      // to column offset
      for (oid_t child_col_id = 0;
           child_col_id < children_output_columns_[0].size(); ++child_col_id) {
        if (col->bound_obj_id == children_output_columns_[0][child_col_id]) {
          column_ids.emplace_back(child_col_id);
          // record output column mapping
          if (output_columns_ != nullptr)
            output_columns_->emplace_back(col->bound_obj_id);
          break;
        }
      }
    }
  }
  // for (auto &col : column_ids) {
  //  LOG_DEBUG("Output Col : %u", col);
  //}
  std::unique_ptr<planner::AbstractPlan> order_by_plan(
      new planner::OrderByPlan(sort_col_ids, sort_flags, column_ids));

  // Add child
  order_by_plan->AddChild(std::move(children_plans_[0]));
  output_plan_ = std::move(order_by_plan);
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
void OperatorToPlanTransformer::Visit(const PhysicalInsert *op) {
  std::unique_ptr<planner::AbstractPlan> insert_plan(
      new planner::InsertPlan(op->target_table, op->columns,
                              op->values));
  output_plan_ = std::move(insert_plan);
}
void OperatorToPlanTransformer::Visit(const PhysicalDelete *op) {
  // TODO: Support index scan
  auto scan_plan = (planner::AbstractScan*)children_plans_[0].get();
  PL_ASSERT(scan_plan != nullptr);

  // Add predicates
  expression::AbstractExpression* predicates =
      (scan_plan->GetPredicate() == nullptr
       ? nullptr
       : scan_plan->GetPredicate()->Copy());
  std::unique_ptr<planner::AbstractPlan> delete_plan(
      new planner::DeletePlan(op->target_table, predicates));

  // Add child
  delete_plan->AddChild(std::move(children_plans_[0]));
  output_plan_ = std::move(delete_plan);
}
void OperatorToPlanTransformer::Visit(const PhysicalUpdate *op) {
  // TODO: Support index scan
  std::unique_ptr<planner::AbstractPlan> update_plan(
      new planner::UpdatePlan(op->update_stmt));
  output_plan_ = std::move(update_plan);
}

void OperatorToPlanTransformer::VisitOpExpression(
    std::shared_ptr<OperatorExpression> op) {
  op->Op().Accept(this);
}

} /* namespace optimizer */
} /* namespace peloton */
