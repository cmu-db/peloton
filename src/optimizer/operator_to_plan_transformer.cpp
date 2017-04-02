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
  if (column_prop->IsStarExpressionInColumn()) {
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
  // Ideally, predicate should be taken out as a separate operator. Since
  // now the predicate is coupled with scan, we need to evaluate predicate here
  auto predicate_prop = requirements_->GetPropertyOfType(PropertyType::PREDICATE)
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
    PL_ASSERT(children_plans_.size() == 1);
  
    // Limit Operator does not change the column mapping
      *output_expr_map_ = children_expr_map_[0];
  
    unique_ptr<planner::AbstractPlan> limit_plan(
        new planner::LimitPlan(op->limit, op->offset));
    limit_plan->AddChild(move(children_plans_[0]));
    output_plan_ = move(limit_plan);
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

  // When executor support order by expression, we need to call EvaluateExpr
  // expression::ExpressionUtil::EvaluateExpression(expr_map, sort_expr);
  
  for (size_t idx = 0; idx < sort_columns_size; ++idx) {
    sort_col_ids.push_back(children_expr_map_[0][sort_exprs[idx]]);
    // planner use desc flag
    sort_flags.push_back(!sort_ascending[idx]);
  }

  // Get output columns
  auto column_prop = requirements_->GetPropertyOfType(PropertyType::COLUMNS)
                         ->As<PropertyColumns>();
  PL_ASSERT(column_prop != nullptr);

  // construct output column offset & output expr map
  vector<oid_t> column_ids;
  if (column_prop->IsStarExpressionInColumn()) {
    // if SELECT *, add all exprs to output column
    column_ids.resize(children_expr_map_[0].size());
    for (auto &expr_idx_pair : children_expr_map_[0]) {
      auto &expr = expr_idx_pair.first;
      oid_t &idx = expr_idx_pair.second;
      (*output_expr_map_)[expr] = idx;
      column_ids[idx] = idx;
    }
  } else {
    auto output_column_size = column_prop->GetSize();
    for (oid_t idx = 0; idx < output_column_size; ++idx) {
      auto output_expr = column_prop->GetColumn(idx);
      column_ids.push_back(children_expr_map_[0][output_expr]);
      (*output_expr_map_)[output_expr] = idx;
    }
  }

  unique_ptr<planner::AbstractPlan> order_by_plan(
      new planner::OrderByPlan(sort_col_ids, sort_flags, column_ids));

  // Add child
  order_by_plan->AddChild(move(children_plans_[0]));
  output_plan_ = move(order_by_plan);
}

void OperatorToPlanTransformer::Visit(const PhysicalAggregate *op) {
  auto proj_prop = requirements_
      ->GetPropertyOfType(PropertyType::PROJECT)->As<PropertyProjection>();
  auto col_prop = requirements_
      ->GetPropertyOfType(PropertyType::COLUMNS)->As<PropertyColumns>();
  auto child_expr_map = children_expr_map_[0];

  //TODO: Consider different type in the logical to physical implementation
  auto agg_type = AggregateType::PLAIN;
  auto agg_id = 0;

  // Metadata needed in AggregationPlan
  std::vector<planner::AggregatePlan::AggTerm> agg_terms;
  DirectMapList direct_map_list = {};
  std::vector<catalog::Column> output_schema_columns;



  PL_ASSERT(col_prop != nullptr);

  if (proj_prop != nullptr) {
    auto expr_len = proj_prop->GetProjectionListSize();
    for (size_t col_pos = 0; col_pos<expr_len; col_pos++) {
      auto expr = proj_prop->GetProjection(col_pos);
      // Aggregate function
      // Aggregate executor only supports aggregation on single col/*, not exprs
      if (expression::ExpressionUtil::IsAggregateExpression(
          expr->GetExpressionType())) {
        auto agg_expr = (expression::AggregateExpression *) expr;
        auto agg_col = expr->GetModifiableChild(0);

        // Maps the aggregate value in the right tuple to the output.
        // See aggregator.cpp for more info.
        direct_map_list.emplace_back(
            std::make_pair(col_pos, std::make_pair(1, agg_id++)));

        planner::AggregatePlan::AggTerm agg_term(
            agg_expr->GetExpressionType(),
            agg_col == nullptr ? nullptr : agg_col->Copy(),
            agg_expr->distinct_);
        agg_terms.push_back(agg_term);
      } else {
        agg_type = AggregateType::PLAIN;
        // Pass through non-aggregate values. See aggregator.cpp for more info.
        direct_map_list.emplace_back(
            std::make_pair(col_pos, std::make_pair(0, child_expr_map[expr])));
      }

      (*output_expr_map_)[expr] = col_pos;

      type::Type::TypeId schema_col_type;
      switch (expr->GetExpressionType()) {
        case ExpressionType::AGGREGATE_AVG:
          schema_col_type = type::Type::DECIMAL;
          break;
        case ExpressionType::AGGREGATE_COUNT_STAR:
        case ExpressionType::AGGREGATE_COUNT:
          schema_col_type = type::Type::INTEGER;
          break;
        default:
          schema_col_type = expr->GetValueType();
      }
      output_schema_columns.push_back(
          catalog::Column(schema_col_type,
                          type::Type::GetTypeSize(schema_col_type),
                          expr->expr_name_));
    }
  }
  // Only have base columns
  else if (col_prop != nullptr) {
    if (col_prop->IsStarExpressionInColumn()) {
      (*output_expr_map_) = child_expr_map;
      std::unordered_map<unsigned, expression::AbstractExpression*> inverted_index;
      for (auto entry : child_expr_map)
        inverted_index[entry.second] = entry.first;
      for (size_t col_pos = 0; col_pos < child_expr_map.size(); col_pos++) {
        auto expr = inverted_index[col_pos];
        auto schema_col_type = expr->GetValueType();
        output_schema_columns.push_back(
            catalog::Column(schema_col_type,
                            type::Type::GetTypeSize(schema_col_type),
                            expr->expr_name_));
        direct_map_list.emplace_back(std::make_pair(col_pos, std::make_pair(0, col_pos)));
      }
    }
    else {
      auto col_len = col_prop->GetSize();
      for (size_t col_pos = 0; col_pos<col_len; col_pos++) {
        auto expr = col_prop->GetColumn(col_pos);
        auto old_col_pos = child_expr_map[expr];
        auto schema_col_type = expr->GetValueType();
        output_schema_columns.push_back(
            catalog::Column(schema_col_type,
                            type::Type::GetTypeSize(schema_col_type),
                            expr->expr_name_));
        direct_map_list.emplace_back(std::make_pair(col_pos, std::make_pair(0, old_col_pos)));
      }
    }
  }
  std::vector<oid_t> col_ids;
  for (auto col : *(op->columns))
    col_ids.push_back(child_expr_map[col]);

  std::unique_ptr<const planner::ProjectInfo> proj_info(
      new planner::ProjectInfo(TargetList(), std::move(direct_map_list)));
  std::unique_ptr<const expression::AbstractExpression> predicate(op->having);
  std::shared_ptr<const catalog::Schema> output_table_schema(
      new catalog::Schema(output_schema_columns));
  std::unique_ptr<planner::AggregatePlan> agg_plan(
      new planner::AggregatePlan(
          std::move(proj_info), std::move(predicate),
          std::move(agg_terms), std::move(col_ids),
          output_table_schema, agg_type));
  agg_plan->AddChild(move(children_plans_[0]));
  output_plan_ = move(agg_plan);
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
  auto table = catalog::Catalog::GetInstance()->GetTableWithName(db_name, table_name);
  GenerateTableExprMap(table_expr_map, table);
  
  // Evaluate update expression
  for (auto update : *op->update_stmt->updates) {
    expression::ExpressionUtil::EvaluateExpression(table_expr_map, update->value);
  }
  // Evaluate predicate if any
  if (op->update_stmt->where != nullptr) {
    expression::ExpressionUtil::EvaluateExpression(table_expr_map, op->update_stmt->where);
  }

  unique_ptr<planner::AbstractPlan> update_plan(
          new planner::UpdatePlan(op->update_stmt));
  output_plan_ = move(update_plan);
}

  
/************************* Private Functions *******************************/
// Generate expr map for all the columns in the given table. Used to evaluate
// the predicate.
void OperatorToPlanTransformer::GenerateTableExprMap(ExprMap& expr_map,
                                                     storage::DataTable* table) {
  auto db_id = table->GetDatabaseOid();
  oid_t table_id = table->GetOid();
  size_t num_col = table->GetSchema()->GetColumnCount();
  for (oid_t col_id = 0; col_id < num_col; ++col_id) {
    // Only bound_obj_id is needed for expr_map
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
