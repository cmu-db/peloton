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

#include "planner/aggregate_plan.h"
#include "planner/update_plan.h"

#include "optimizer/operator_expression.h"
#include "optimizer/operator_to_plan_transformer.h"
#include "optimizer/properties.h"
#include "planner/hash_join_plan.h"
#include "planner/nested_loop_join_plan.h"
#include "planner/projection_plan.h"
#include "planner/seq_scan_plan.h"
#include "planner/order_by_plan.h"
#include "planner/limit_plan.h"
#include "planner/hash_plan.h"
#include "expression/expression_util.h"
#include "expression/aggregate_expression.h"

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
  // Generate column ids to pass into scan plan and generate output expr map
  auto column_prop = requirements_->GetPropertyOfType(PropertyType::COLUMNS)
                         ->As<PropertyColumns>();
  vector<oid_t> column_ids;
  // TODO: handle star expression
  bool has_star_expr = false;
  if (has_star_expr) {
    size_t num_col = op->table_->GetSchema()->GetColumnCount();
    for (oid_t col_id = 0; col_id < num_col; ++col_id)
      column_ids.push_back(col_id);
    GenerateTableExprMap(*output_expr_map_, op->table_);
  } else {
    auto output_column_size = column_prop->GetSize();
    for (oid_t idx = 0; idx < output_column_size; ++idx) {
      auto output_expr = column_prop->GetColumn(idx);
      auto output_tvexpr =
          reinterpret_cast<expression::TupleValueExpression *>(output_expr);

      // Set column offset
      PL_ASSERT(output_tvexpr->GetIsBound() == true);
      auto col_id = std::get<2>(output_tvexpr->GetBoundOid());
      column_ids.push_back(col_id);
      (*output_expr_map_)[output_expr] = idx;
    }
  }

  // Add Scan Predicates
  auto predicate_prop =
      requirements_->GetPropertyOfType(PropertyType::PREDICATE)
          ->As<PropertyPredicate>();
  expression::AbstractExpression *predicate = nullptr;
  if (predicate_prop != nullptr) {
    ExprMap table_expr_map;
    GenerateTableExprMap(table_expr_map, op->table_);
    predicate = predicate_prop->GetPredicate()->Copy();
    expression::ExpressionUtil::EvaluateExpression(table_expr_map, predicate);
  }

  // Create scan plan
  unique_ptr<planner::AbstractPlan> seq_scan_plan(
      new planner::SeqScanPlan(op->table_, predicate, column_ids));
  output_plan_ = move(seq_scan_plan);
}

void OperatorToPlanTransformer::Visit(const PhysicalProject *) {
  auto cols_prop = requirements_->GetPropertyOfType(PropertyType::COLUMNS)
                          ->As<PropertyColumns>();
  size_t proj_list_size = cols_prop->GetSize();
  ExprMap& child_expr_map = children_expr_map_[0];

  // expressions to evaluate
  TargetList tl = TargetList();
  // columns which can be returned directly
  DirectMapList dml = DirectMapList();
  // schema of the projections output
  vector<catalog::Column> columns;

  for (size_t project_idx = 0; project_idx < proj_list_size; project_idx++) {
    auto expr = cols_prop->GetColumn(project_idx);
    
    // For TupleValueExpr, we can just do a direct mapping.
    if (expr->GetExpressionType() == ExpressionType::VALUE_TUPLE) {
      auto tup_expr = (expression::TupleValueExpression *)expr;
      dml.emplace_back(project_idx, make_pair(0, child_expr_map[tup_expr]));
    } else {
      // For more complex expression, we need to do evaluation in Executor
      expression::ExpressionUtil::EvaluateExpression(child_expr_map, expr);
      tl.emplace_back(project_idx, expr->Copy());
    }
    columns.push_back(catalog::Column(
        expr->GetValueType(), type::Type::GetTypeSize(expr->GetValueType()),
        expr->GetExpressionName()));
    (*output_expr_map_)[expr] = project_idx;
  }

  // build the projection plan node and insert above the scan
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
  PL_ASSERT(children_plans_.size() == 1);

  // Limit Operator does not change the column mapping
  *output_expr_map_ = children_expr_map_[0];

  unique_ptr<planner::AbstractPlan> limit_plan(
      new planner::LimitPlan(op->limit, op->offset));
  limit_plan->AddChild(move(children_plans_[0]));
  output_plan_ = move(limit_plan);
}

void OperatorToPlanTransformer::Visit(const PhysicalOrderBy *op) {
  auto sort_columns_size = op->sort_exprs.size();
  auto &sort_exprs = op->sort_exprs;
  auto &sort_ascending = op->sort_ascending;
  ExprMap &child_expr_map = children_expr_map_[0];

  auto cols_prop = requirements_->GetPropertyOfType(PropertyType::COLUMNS)
                         ->As<PropertyColumns>();

  // Construct output column offset.
  vector<oid_t> column_ids;
  for (size_t i = 0; i < cols_prop->GetSize(); i++)
    column_ids.push_back(child_expr_map[cols_prop->GetColumn(i)]);

  // Construct sort ids and sort flags
  vector<oid_t> sort_col_ids;
  vector<bool> sort_flags;
  for (size_t i = 0; i < sort_columns_size; i++) {
    sort_col_ids.push_back(child_expr_map[sort_exprs[i]]);
    // planner use desc flag
    sort_flags.push_back(!sort_ascending[i]);
  }

  // Create and insert OrderBy Plan
  unique_ptr<planner::AbstractPlan> order_by_plan(
      new planner::OrderByPlan(sort_col_ids, sort_flags, column_ids));
  order_by_plan->AddChild(move(children_plans_[0]));
  output_plan_ = move(order_by_plan);
}

void OperatorToPlanTransformer::Visit(const PhysicalAggregate *op) {
  auto proj_prop = requirements_->GetPropertyOfType(PropertyType::PROJECT)
                       ->As<PropertyProjection>();
  auto col_prop = requirements_->GetPropertyOfType(PropertyType::COLUMNS)
                      ->As<PropertyColumns>();
  auto child_expr_map = children_expr_map_[0];

  // TODO: Consider different type in the logical to physical implementation
  auto agg_type = AggregateType::HASH;

  // Metadata needed in AggregationPlan
  vector<planner::AggregatePlan::AggTerm> agg_terms;
  vector<catalog::Column> output_schema_columns;
  DirectMapList dml;
  TargetList tl;

  auto agg_id = 0;
  if (proj_prop != nullptr) {
    auto expr_len = proj_prop->GetProjectionListSize();
    for (size_t col_pos = 0; col_pos < expr_len; col_pos++) {
      auto expr = proj_prop->GetProjection(col_pos);
      expression::ExpressionUtil::EvaluateExpression(child_expr_map, expr);

      if (expression::ExpressionUtil::IsAggregateExpression(
              expr->GetExpressionType())) {
        // For AggregateExpr, add aggregate term
        auto agg_expr = (expression::AggregateExpression *)expr;
        auto agg_col = expr->GetModifiableChild(0);
        if (agg_col != nullptr)
          expression::ExpressionUtil::EvaluateExpression(child_expr_map,
                                                         agg_col);

        // Maps the aggregate value in the right tuple to the output.
        // See aggregator.cpp for more info.
        dml.emplace_back(col_pos, make_pair(1, agg_id++));
        planner::AggregatePlan::AggTerm agg_term(
            agg_expr->GetExpressionType(),
            agg_col == nullptr ? nullptr : agg_col->Copy(),
            agg_expr->distinct_);
        agg_terms.push_back(agg_term);
      } else if (expr->GetExpressionType() == ExpressionType::VALUE_TUPLE) {
        // For TupleValueExpr, do direct mapping
        dml.emplace_back(col_pos, make_pair(0, child_expr_map[expr]));
        (*output_expr_map_)[expr] = col_pos;
      } else {
        // For other exprs such as OperatorExpr, use target list
        tl.emplace_back(col_pos, expr->Copy());
      }

      output_schema_columns.push_back(catalog::Column(
          expr->GetValueType(), type::Type::GetTypeSize(expr->GetValueType()),
          expr->expr_name_));
    }
  } else {
    // Only have base columns
    PL_ASSERT(col_prop != nullptr);

    *output_expr_map_ = child_expr_map;
    output_schema_columns.resize(child_expr_map.size());
    for (auto iter : child_expr_map) {
      auto expr = iter.first;
      size_t col_pos = iter.second;
      // Keep the column order
      dml.emplace_back(col_pos, make_pair(0, col_pos));
      output_schema_columns[col_pos] = catalog::Column(
          expr->GetValueType(), type::Type::GetTypeSize(expr->GetValueType()),
          expr->expr_name_);
    }
  }

  // Handle group by columns
  vector<oid_t> col_ids;
  for (auto col : *(op->columns)) col_ids.push_back(child_expr_map[col]);

  // Handle having clause
  expression::AbstractExpression *having = nullptr;
  if (op->having != nullptr) {
    expression::ExpressionUtil::EvaluateExpression(child_expr_map, op->having);
    having = op->having->Copy();
  }

  // Generate the Aggregate Plan
  unique_ptr<const planner::ProjectInfo> proj_info(
      new planner::ProjectInfo(move(tl), move(dml)));
  unique_ptr<const expression::AbstractExpression> predicate(having);
  shared_ptr<const catalog::Schema> output_table_schema(
      new catalog::Schema(output_schema_columns));
  unique_ptr<planner::AggregatePlan> agg_plan(new planner::AggregatePlan(
      move(proj_info), move(predicate), move(agg_terms), move(col_ids),
      output_table_schema, agg_type));
  agg_plan->AddChild(move(children_plans_[0]));
  output_plan_ = move(agg_plan);
}

void OperatorToPlanTransformer::Visit(const PhysicalHash *op) {
  ExprMap &child_expr_map = children_expr_map_[0];

  std::vector<std::unique_ptr<const expression::AbstractExpression>> hash_keys;
  // TODO handle star expr
  // Hash executor uses TVexpr to store the column,
  // but only uses their offset, we may want to modify
  // the interface to the offset directly
  for (auto expr : op->hash_keys) {
    auto column_idx = child_expr_map[expr];
    auto col_expr = new expression::TupleValueExpression("");
    col_expr->SetValueIdx(static_cast<int>(column_idx));
    hash_keys.emplace_back(col_expr);
  }
  unique_ptr<planner::HashPlan> hash_plan(new planner::HashPlan(hash_keys));
  hash_plan->AddChild(move(children_plans_[0]));
  output_plan_ = move(hash_plan);

  // Hash does not change the layout of the column mapping
  *output_expr_map_ = move(child_expr_map); 
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

  // Add predicates. The predicate should already be evaluated in Scan.
  // Currently, the delete executor does not use predicate at all.
  const expression::AbstractExpression *predicates = scan_plan->GetPredicate();
  unique_ptr<planner::AbstractPlan> delete_plan(
      new planner::DeletePlan(op->target_table, predicates));

  // Add child
  delete_plan->AddChild(move(children_plans_[0]));
  output_plan_ = move(delete_plan);
}

void OperatorToPlanTransformer::Visit(const PhysicalUpdate *op) {
  // TODO: Support index scan
  ExprMap table_expr_map;
  auto db_name = op->update_stmt->table->GetDatabaseName();
  auto table_name = op->update_stmt->table->GetTableName();
  auto table =
      catalog::Catalog::GetInstance()->GetTableWithName(db_name, table_name);
  GenerateTableExprMap(table_expr_map, table);

  // Evaluate update expression
  for (auto update : *op->update_stmt->updates) {
    expression::ExpressionUtil::EvaluateExpression(table_expr_map,
                                                   update->value);
  }
  // Evaluate predicate if any
  if (op->update_stmt->where != nullptr) {
    expression::ExpressionUtil::EvaluateExpression(table_expr_map,
                                                   op->update_stmt->where);
  }

  unique_ptr<planner::AbstractPlan> update_plan(
      new planner::UpdatePlan(op->update_stmt));
  output_plan_ = move(update_plan);
}

/************************* Private Functions *******************************/
// Generate expr map for all the columns in the given table. Used to evaluate
// the predicate.
void OperatorToPlanTransformer::GenerateTableExprMap(
    ExprMap &expr_map, storage::DataTable *table) {
  auto db_id = table->GetDatabaseOid();
  oid_t table_id = table->GetOid();
  size_t num_col = table->GetSchema()->GetColumnCount();
  for (oid_t col_id = 0; col_id < num_col; ++col_id) {
    // Only bound_obj_id is needed for expr_map
    // TODO potential memory leak here?
    auto col_expr = new expression::TupleValueExpression("");
    col_expr->SetBoundOid(db_id, table_id, col_id);
    expr_map[col_expr] = col_id;
  }
}

void OperatorToPlanTransformer::VisitOpExpression(
    shared_ptr<OperatorExpression> op) {
  op->Op().Accept(this);
}

} /* namespace optimizer */
} /* namespace peloton */
