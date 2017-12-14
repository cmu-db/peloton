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

#include "optimizer/plan_generator.h"

#include "optimizer/operator_expression.h"
#include "planner/aggregate_plan.h"
#include "planner/delete_plan.h"
#include "planner/hash_join_plan.h"
#include "planner/hash_plan.h"
#include "planner/index_scan_plan.h"
#include "planner/insert_plan.h"
#include "planner/limit_plan.h"
#include "planner/nested_loop_join_plan.h"
#include "planner/order_by_plan.h"
#include "planner/projection_plan.h"
#include "planner/seq_scan_plan.h"
#include "planner/update_plan.h"
#include "settings/settings_manager.h"
#include "storage/data_table.h"

using std::vector;
using std::make_pair;
using std::string;
using std::to_string;
using std::unique_ptr;
using std::shared_ptr;
using std::move;
using std::make_tuple;
using std::make_pair;
using std::pair;

namespace peloton {
namespace optimizer {

PlanGenerator::PlanGenerator() {}

unique_ptr<planner::AbstractPlan> PlanGenerator::ConvertOpExpression(
    shared_ptr<OperatorExpression> op, shared_ptr<PropertySet> required_props,
    vector<expression::AbstractExpression *> required_cols,
    vector<expression::AbstractExpression *> output_cols,
    vector<unique_ptr<planner::AbstractPlan>> &children_plans,
    vector<ExprMap> children_expr_map) {
  required_props_ = required_props;
  required_cols_ = required_cols;
  output_cols_ = output_cols;
  children_plans_ = move(children_plans);
  children_expr_map_ = move(children_expr_map);
  op->Op().Accept(this);
  BuildProjectionPlan();
  return move(output_plan_);
}

void PlanGenerator::Visit(const DummyScan *) {
  // DummyScan is used in case of SELECT without FROM so that enforcer
  // can enforce a PhysicalProjection on top of DummyScan to generate correct
  // result. But here, no need to translate DummyScan to any physical plan.
  output_plan_ = nullptr;
}

void PlanGenerator::Visit(const PhysicalSeqScan *op) {
  vector<oid_t> column_ids = GenerateColumnsForScan();
  auto predicate = GeneratePredicateForScan(
      expression::ExpressionUtil::JoinAnnotatedExprs(op->predicates),
      op->table_alias, op->table_);

  output_plan_.reset(new planner::SeqScanPlan(op->table_, predicate.release(), column_ids));
}

void PlanGenerator::Visit(const PhysicalIndexScan *op) {
  vector<oid_t> column_ids = GenerateColumnsForScan();
  auto predicate = GeneratePredicateForScan(
      expression::ExpressionUtil::JoinAnnotatedExprs(op->predicates),
      op->table_alias, op->table_);

  auto index = op->table_->GetIndex(op->index_id);
  vector<expression::AbstractExpression *> runtime_keys;

  // Create index scan desc
  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      index, op->key_column_id_list, op->expr_type_list, op->value_list, runtime_keys);
  output_plan_.reset(new planner::IndexScanPlan(
      op->table_, predicate.release(), column_ids, index_scan_desc, false));
}

void PlanGenerator::Visit(const QueryDerivedScan *) {}

void PlanGenerator::Visit(const PhysicalProject *) {}

void PlanGenerator::Visit(const PhysicalLimit *) {}

void PlanGenerator::Visit(const PhysicalOrderBy *) {
  vector<oid_t> column_ids;
  PL_ASSERT(children_expr_map_.size() == 1);
  auto &child_cols_map = children_expr_map_[0];
  for (size_t i = 0; i < required_cols_.size(); ++i) {
    column_ids.push_back(child_cols_map[required_cols_[i]]);
  }

  auto sort_prop = required_props_->GetPropertyOfType(PropertyType::SORT)
                       ->As<PropertySort>();
  auto sort_columns_size = sort_prop->GetSortColumnSize();
  vector<oid_t> sort_col_ids;
  vector<bool> sort_flags;
  for (size_t i = 0; i < sort_columns_size; ++i) {
    sort_col_ids.push_back(child_cols_map[sort_prop->GetSortColumn(i)]);
    // planner use desc flag
    sort_flags.push_back(!sort_prop->GetSortAscending(i));
  }
  output_plan_.reset(
      new planner::OrderByPlan(sort_col_ids, sort_flags, column_ids));
  output_plan_->AddChild(move(children_plans_[0]));
}

void PlanGenerator::Visit(const PhysicalHashGroupBy *) {}

void PlanGenerator::Visit(const PhysicalSortGroupBy *) {}

void PlanGenerator::Visit(const PhysicalAggregate *) {}

void PlanGenerator::Visit(const PhysicalDistinct *) {}

void PlanGenerator::Visit(const PhysicalFilter *) {}

void PlanGenerator::Visit(const PhysicalInnerNLJoin *) {}

void PlanGenerator::Visit(const PhysicalLeftNLJoin *) {}

void PlanGenerator::Visit(const PhysicalRightNLJoin *) {}

void PlanGenerator::Visit(const PhysicalOuterNLJoin *) {}

void PlanGenerator::Visit(const PhysicalInnerHashJoin *) {}

void PlanGenerator::Visit(const PhysicalLeftHashJoin *) {}

void PlanGenerator::Visit(const PhysicalRightHashJoin *) {}

void PlanGenerator::Visit(const PhysicalOuterHashJoin *) {}

void PlanGenerator::Visit(const PhysicalInsert *op) {
   unique_ptr<planner::AbstractPlan> insert_plan(
       new planner::InsertPlan(op->target_table, op->columns, op->values));
   output_plan_ = move(insert_plan);
}

void PlanGenerator::Visit(const PhysicalInsertSelect *op) {
   unique_ptr<planner::AbstractPlan> insert_plan(
       new planner::InsertPlan(op->target_table));
   // Add child
   insert_plan->AddChild(move(children_plans_[0]));
   output_plan_ = move(insert_plan);
}

void PlanGenerator::Visit(const PhysicalDelete *) {}

void PlanGenerator::Visit(const PhysicalUpdate *) {}

ExprMap PlanGenerator::GenerateTableExprMap(const std::string &alias,
                                            const storage::DataTable *table) {
  ExprMap expr_map;
  auto db_id = table->GetDatabaseOid();
  oid_t table_id = table->GetOid();
  auto cols = table->GetSchema()->GetColumns();
  size_t num_col = cols.size();
  for (oid_t col_id = 0; col_id < num_col; ++col_id) {
    // Only bound_obj_id is needed for expr_map
    // TODO potential memory leak here?
    expression::TupleValueExpression *col_expr =
        new expression::TupleValueExpression(cols[col_id].column_name.c_str(),
                                             alias.c_str());
    col_expr->SetValueType(table->GetSchema()->GetColumn(col_id).GetType());
    col_expr->SetBoundOid(db_id, table_id, col_id);
    expr_map[col_expr] = col_id;
  }
  return expr_map;
}
// Generate columns for scan plan
vector<oid_t> PlanGenerator::GenerateColumnsForScan() {
  vector<oid_t> column_ids;
  for (oid_t idx = 0; idx < output_cols_.size(); ++idx) {
    auto &output_expr = output_cols_[idx];
    PL_ASSERT(output_expr->GetExpressionType() == ExpressionType::VALUE_TUPLE);
    auto output_tvexpr =
        reinterpret_cast<expression::TupleValueExpression *>(output_expr);

    // Set column offset
    PL_ASSERT(output_tvexpr->GetIsBound() == true);
    auto col_id = std::get<2>(output_tvexpr->GetBoundOid());
    column_ids.push_back(col_id);
  }

  return column_ids;
}

std::unique_ptr<expression::AbstractExpression>
PlanGenerator::GeneratePredicateForScan(
    const std::shared_ptr<expression::AbstractExpression> predicate_expr,
    const std::string &alias, const storage::DataTable *table) {
  if (predicate_expr == nullptr) {
    return nullptr;
  }
  ExprMap table_expr_map = GenerateTableExprMap(alias, table);
  unique_ptr<expression::AbstractExpression> predicate =
      std::unique_ptr<expression::AbstractExpression>(predicate_expr->Copy());
  expression::ExpressionUtil::EvaluateExpression({table_expr_map},
                                                 predicate.get());
  return predicate;
}

void PlanGenerator::BuildProjectionPlan() {
  bool no_projection = false;
  if (output_cols_.size() == required_cols_.size()) {
    no_projection = true;
    size_t cols_size = output_cols_.size();
    for (size_t idx = 0; idx < cols_size; ++idx) {
      if (output_cols_[idx] != required_cols_[idx]) {
        no_projection = false;
        break;
      }
    }
  }

  if (no_projection) {
    return;
  }
  // TODO Construct projection plan
  // TODO(boweic): For now, we only produce tv exprs and aggregate expr as
  // output column, but we should handle arbitary expressions in the future
  // ExprMap output_cols_map;
  // for (auto &col : output_cols_) {
  //   PL_ASSERT(col->GetExpressionType() == ExpressionType::VALUE_TUPLE ||
  //             col->GetExpressionType() == ExpressionType::AggregateType);
  //
  // }
}

}  // namespace optimizer
}  // namespace peloton
