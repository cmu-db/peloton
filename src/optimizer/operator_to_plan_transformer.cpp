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

using std::vector;
using std::make_pair;
using std::string;
using std::to_string;
using std::unique_ptr;
using std::shared_ptr;
using std::move;
using std::make_tuple;
using std::make_pair;

namespace peloton {
namespace optimizer {

OperatorToPlanTransformer::OperatorToPlanTransformer() {}

unique_ptr<planner::AbstractPlan>
OperatorToPlanTransformer::ConvertOpExpression(
    shared_ptr<OperatorExpression> plan, PropertySet *requirements,
    vector<PropertySet> *required_input_props,
    vector<unique_ptr<planner::AbstractPlan>> &children_plans,
    vector<ExprMap> &children_expr_map, ExprMap *output_expr_map) {
  requirements_ = requirements;
  required_input_props_ = required_input_props;
  children_plans_ = move(children_plans);
  children_expr_map_ = move(children_expr_map);
  output_expr_map_ = output_expr_map;
  VisitOpExpression(plan);
  return move(output_plan_);
}

void OperatorToPlanTransformer::Visit(const PhysicalScan *op) {
  vector<oid_t> column_ids;

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

  if (column_prop->IsStarExpressionInColumn()) {
    // if SELECT *, add all exprs to output column
    size_t num_col = op->table_->GetSchema()->GetColumnCount();

    auto db_id = op->table_->GetDatabaseOid();
    oid_t table_id = op->table_->GetOid();
    for (oid_t idx = 0; idx < num_col; ++idx) {
      column_ids.push_back(idx);
      // The root plan node doesn't need
      // the expr_map
      if (output_expr_map_ != nullptr) {
        // We don't need column name in optimizer
        expression::TupleValueExpression *col_expr =
            new expression::TupleValueExpression("");

        // Set bound oid for each output column
        auto bound_oid = make_tuple(db_id, table_id, idx);
        col_expr->SetBoundOid(bound_oid);
        col_expr->SetIsBound();
        (*output_expr_map_)[reinterpret_cast<expression::AbstractExpression *>(
            col_expr)] = idx;
        
      }
    }
  } else {
    auto output_column_size = column_prop->GetSize();
    for (oid_t idx = 0; idx < output_column_size; ++idx) {
      
      auto output_expr = column_prop->GetColumn(idx);
      auto output_tvexpr =
          reinterpret_cast<expression::TupleValueExpression *>(output_expr);

      // Set column offset
      auto col_id = std::get<2>(output_tvexpr->GetBoundOid());
      column_ids.push_back(col_id);
      if (output_expr_map_ != nullptr)
        (*output_expr_map_)[output_expr] = idx;
    }
  }

  unique_ptr<planner::AbstractPlan> seq_scan_plan(
      new planner::SeqScanPlan(op->table_, predicate, column_ids));

  output_plan_ = move(seq_scan_plan);
}

void OperatorToPlanTransformer::Visit(const PhysicalProject *) {
    auto project_prop =
    requirements_->GetPropertyOfType(PropertyType::PROJECT)
                            ->As<PropertyProjection>();
    size_t proj_list_size = project_prop->GetProjectionListSize();
  
    // expressions to evaluate
    TargetList tl = TargetList();
    // columns which can be returned directly
    DirectMapList dml = DirectMapList();
    // schema of the projections output
    vector<catalog::Column> columns;
  
    for (size_t project_idx = 0; project_idx < proj_list_size; project_idx++) {
      auto expr = project_prop->GetProjection(project_idx);
      expression::ExpressionUtil::EvaluateExpression(children_expr_map_[0], expr);
      // For TupleValueExpr, we can just do a direct mapping.
      if (expr->GetExpressionType() == ExpressionType::VALUE_TUPLE) {
        auto tup_expr = (expression::TupleValueExpression *)expr;
        dml.push_back(
            DirectMap(project_idx, make_pair(0,tup_expr->GetColumnId())));
      } else {
        // For more complex expression, we need to do evaluation in Executor
        tl.push_back(Target(project_idx, expr->Copy()));
      }
      columns.push_back(catalog::Column(
          expr->GetValueType(), type::Type::GetTypeSize(expr->GetValueType()),
          expr->GetExpressionName()));
    }
  
    // build the projection plan node and insert aboce the scan
    unique_ptr<planner::ProjectInfo> proj_info(
        new planner::ProjectInfo(move(tl), move(dml)));
    shared_ptr<catalog::Schema> schema_ptr(new catalog::Schema(columns));
    unique_ptr<planner::AbstractPlan> project_plan(
        new planner::ProjectionPlan(move(proj_info), schema_ptr));
  
    PL_ASSERT(children_plans_.size() == 1);
    project_plan->AddChild(move(children_plans_[0]));
  
    output_plan_ = move(project_plan);
}

void OperatorToPlanTransformer::Visit(const PhysicalLimit *op) {
  (void)op;
  //  PL_ASSERT(children_plans_.size() == 1);
  //
  //  // Limit Operator does not change the column mapping
  //  if (output_columns_ != nullptr)
  //    *output_columns_ = children_output_columns_[0];
  //
  //  unique_ptr<planner::AbstractPlan> limit_plan(
  //      new planner::LimitPlan(op->limit, op->offset));
  //  limit_plan->AddChild(move(children_plans_[0]));
  //  output_plan_ = move(limit_plan);
}

void OperatorToPlanTransformer::Visit(const PhysicalOrderBy *op) {
  // Get child plan
  PL_ASSERT(children_plans_.size() == 1);
  PL_ASSERT(children_expr_map_.size() == 1);

  vector<oid_t> sort_col_ids;
  vector<bool> sort_flags;

  auto sort_columns_size = op->sort_exprs.size();
  auto &sort_exprs = op->sort_exprs;
  auto &sort_ascending = op->sort_ascending;

  for (size_t idx = 0; idx < sort_columns_size; ++idx) {
    sort_col_ids.push_back(children_expr_map_[0][sort_exprs[idx]]);
    // planner use desc flag
    sort_flags.push_back(sort_ascending[idx] ^ 1);
  }

  vector<oid_t> column_ids;
  // Get output columns
  auto column_prop = requirements_->GetPropertyOfType(PropertyType::COLUMNS)
                         ->As<PropertyColumns>();

  PL_ASSERT(column_prop != nullptr);

  // construct output column offset & output expr map
  if (column_prop->IsStarExpressionInColumn()) {
    // if SELECT *, add all exprs to output column
    column_ids.resize(children_expr_map_[0].size());
    for (auto &expr_idx_pair : children_expr_map_[0]) {
      auto &expr = expr_idx_pair.first;
      oid_t &idx = expr_idx_pair.second;
      if (output_expr_map_ != nullptr)
        (*output_expr_map_)[expr] = idx;
      column_ids[idx] = idx;
    }
 
  } else {
    auto output_column_size = column_prop->GetSize();
    for (oid_t idx = 0; idx < output_column_size; ++idx) {
      auto output_expr = column_prop->GetColumn(idx);
      column_ids.push_back(children_expr_map_[0][output_expr]);
      if (output_expr_map_ != nullptr)
        (*output_expr_map_)[output_expr] = idx;
    }
  }

  unique_ptr<planner::AbstractPlan> order_by_plan(
      new planner::OrderByPlan(sort_col_ids, sort_flags, column_ids));

  // Add child
  order_by_plan->AddChild(move(children_plans_[0]));
  output_plan_ = move(order_by_plan);
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
  unique_ptr<planner::AbstractPlan> insert_plan(
      new planner::InsertPlan(op->target_table, op->columns, op->values));
  output_plan_ = move(insert_plan);
}
void OperatorToPlanTransformer::Visit(const PhysicalDelete *op) {
  // TODO: Support index scan
  auto scan_plan = (planner::AbstractScan *)children_plans_[0].get();
  PL_ASSERT(scan_plan != nullptr);

  // Add predicates
  const expression::AbstractExpression *predicates = scan_plan->GetPredicate();
  unique_ptr<planner::AbstractPlan> delete_plan(
      new planner::DeletePlan(op->target_table, predicates));

  // Add child
  delete_plan->AddChild(move(children_plans_[0]));
  output_plan_ = move(delete_plan);
}
void OperatorToPlanTransformer::Visit(const PhysicalUpdate *op) {
  // TODO: Support index scan
  unique_ptr<planner::AbstractPlan> update_plan(
      new planner::UpdatePlan(op->update_stmt));
  output_plan_ = move(update_plan);
}

void OperatorToPlanTransformer::VisitOpExpression(
    shared_ptr<OperatorExpression> op) {
  op->Op().Accept(this);
}

} /* namespace optimizer */
} /* namespace peloton */
