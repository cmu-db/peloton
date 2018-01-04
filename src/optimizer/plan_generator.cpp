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

#include "catalog/column_catalog.h"
#include "catalog/index_catalog.h"
#include "catalog/table_catalog.h"
#include "concurrency/transaction_context.h"
#include "expression/expression_util.h"
#include "optimizer/operator_expression.h"
#include "optimizer/properties.h"
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
#include "storage/storage_manager.h"

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
  required_props_ = move(required_props);
  required_cols_ = move(required_cols);
  output_cols_ = move(output_cols);
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
  output_plan_.reset(new planner::SeqScanPlan(
      storage::StorageManager::GetInstance()->GetTableWithOid(
          op->table_->GetDatabaseOid(), op->table_->GetTableOid()),
      predicate.release(), column_ids));
}

void PlanGenerator::Visit(const PhysicalIndexScan *op) {
  vector<oid_t> column_ids = GenerateColumnsForScan();
  auto predicate = GeneratePredicateForScan(
      expression::ExpressionUtil::JoinAnnotatedExprs(op->predicates),
      op->table_alias, op->table_);

  vector<expression::AbstractExpression *> runtime_keys;

  // Create index scan desc
  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      storage::StorageManager::GetInstance()
          ->GetTableWithOid(op->table_->GetDatabaseOid(),
                            op->table_->GetTableOid())
          ->GetIndexWithOid(op->index_id),
      op->key_column_id_list, op->expr_type_list, op->value_list, runtime_keys);
  output_plan_.reset(new planner::IndexScanPlan(
      storage::StorageManager::GetInstance()->GetTableWithOid(
          op->table_->GetDatabaseOid(), op->table_->GetTableOid()),
      predicate.release(), column_ids, index_scan_desc, false));
}

void PlanGenerator::Visit(const QueryDerivedScan *) {
  PL_ASSERT(children_plans_.size() == 1);
  output_plan_ = move(children_plans_[0]);
}

void PlanGenerator::Visit(const PhysicalLimit *op) {
  unique_ptr<planner::AbstractPlan> limit_plan(
      new planner::LimitPlan(op->limit, op->offset));
  limit_plan->AddChild(move(children_plans_[0]));
  output_plan_ = move(limit_plan);
}

void PlanGenerator::Visit(const PhysicalOrderBy *) {
  vector<oid_t> column_ids;
  PL_ASSERT(children_expr_map_.size() == 1);
  auto &child_cols_map = children_expr_map_[0];
  for (size_t i = 0; i < output_cols_.size(); ++i) {
    column_ids.push_back(child_cols_map[output_cols_[i]]);
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

void PlanGenerator::Visit(const PhysicalHashGroupBy *op) {
  auto having_predicates =
      expression::ExpressionUtil::JoinAnnotatedExprs(op->having);
  expression::ExpressionUtil::EvaluateExpression(children_expr_map_,
                                                 having_predicates.get());
  BuildAggregatePlan(AggregateType::HASH, &op->columns,
                     having_predicates.release());
}

void PlanGenerator::Visit(const PhysicalSortGroupBy *op) {
  auto having_predicates =
      expression::ExpressionUtil::JoinAnnotatedExprs(op->having);
  expression::ExpressionUtil::EvaluateExpression(children_expr_map_,
                                                 having_predicates.get());
  BuildAggregatePlan(AggregateType::HASH, &op->columns,
                     having_predicates.release());
}

void PlanGenerator::Visit(const PhysicalAggregate *) {
  BuildAggregatePlan(AggregateType::PLAIN, nullptr, nullptr);
}

void PlanGenerator::Visit(const PhysicalDistinct *) {
  // Now distinct is a flag in the parser, so we only support
  // distinct on all output columns
  PL_ASSERT(children_expr_map_.size() == 1);
  PL_ASSERT(children_plans_.size() == 1);
  auto &child_expr_map = children_expr_map_[0];
  std::vector<std::unique_ptr<const expression::AbstractExpression>> hash_keys;
  for (auto &col : output_cols_) {
    PL_ASSERT(child_expr_map.count(col) > 0);
    auto &column_offset = child_expr_map[col];
    hash_keys.emplace_back(new expression::TupleValueExpression(
        col->GetValueType(), 0, column_offset));
  }
  unique_ptr<planner::HashPlan> hash_plan(new planner::HashPlan(hash_keys));
  hash_plan->AddChild(move(children_plans_[0]));
  output_plan_ = move(hash_plan);
}

void PlanGenerator::Visit(const PhysicalInnerNLJoin *op) {
  std::unique_ptr<const planner::ProjectInfo> proj_info;
  std::shared_ptr<const catalog::Schema> proj_schema;
  GenerateProjectionForJoin(proj_info, proj_schema);

  auto join_predicate =
      expression::ExpressionUtil::JoinAnnotatedExprs(op->join_predicates);
  expression::ExpressionUtil::EvaluateExpression(children_expr_map_,
                                                 join_predicate.get());

  vector<oid_t> left_keys;
  vector<oid_t> right_keys;
  for (auto &expr : op->left_keys) {
    PL_ASSERT(children_expr_map_[0].find(expr.get()) !=
              children_expr_map_[0].end());
    left_keys.push_back(children_expr_map_[0][expr.get()]);
  }
  for (auto &expr : op->right_keys) {
    PL_ASSERT(children_expr_map_[1].find(expr.get()) !=
              children_expr_map_[1].end());
    right_keys.emplace_back(children_expr_map_[1][expr.get()]);
  }

  auto join_plan =
      unique_ptr<planner::AbstractPlan>(new planner::NestedLoopJoinPlan(
          JoinType::INNER, move(join_predicate), move(proj_info), proj_schema,
          left_keys, right_keys));

  join_plan->AddChild(move(children_plans_[0]));
  join_plan->AddChild(move(children_plans_[1]));
  output_plan_ = move(join_plan);
}

void PlanGenerator::Visit(const PhysicalLeftNLJoin *) {}

void PlanGenerator::Visit(const PhysicalRightNLJoin *) {}

void PlanGenerator::Visit(const PhysicalOuterNLJoin *) {}

void PlanGenerator::Visit(const PhysicalInnerHashJoin *op) {
  std::unique_ptr<const planner::ProjectInfo> proj_info;
  std::shared_ptr<const catalog::Schema> proj_schema;
  GenerateProjectionForJoin(proj_info, proj_schema);

  auto join_predicate =
      expression::ExpressionUtil::JoinAnnotatedExprs(op->join_predicates);
  expression::ExpressionUtil::EvaluateExpression(children_expr_map_,
                                                 join_predicate.get());

  vector<unique_ptr<const expression::AbstractExpression>> left_keys;
  vector<unique_ptr<const expression::AbstractExpression>> right_keys;
  vector<ExprMap> l_child_map{move(children_expr_map_[0])};
  vector<ExprMap> r_child_map{move(children_expr_map_[1])};
  for (auto &expr : op->left_keys) {
    auto left_key = expr->Copy();
    expression::ExpressionUtil::EvaluateExpression(l_child_map, left_key);
    left_keys.emplace_back(left_key);
  }
  for (auto &expr : op->right_keys) {
    auto right_key = expr->Copy();
    expression::ExpressionUtil::EvaluateExpression(r_child_map, right_key);
    right_keys.emplace_back(right_key);
  }
  // Evaluate Expr for hash plan
  vector<unique_ptr<const expression::AbstractExpression>> hash_keys;
  for (auto &expr : op->right_keys) {
    auto hash_key = expr->Copy();
    expression::ExpressionUtil::EvaluateExpression(r_child_map, hash_key);
    hash_keys.emplace_back(hash_key);
  }

  unique_ptr<planner::HashPlan> hash_plan(new planner::HashPlan(hash_keys));
  hash_plan->AddChild(move(children_plans_[1]));

  auto join_plan = unique_ptr<planner::AbstractPlan>(new planner::HashJoinPlan(
      JoinType::INNER, move(join_predicate), move(proj_info), proj_schema,
      left_keys, right_keys, settings::SettingsManager::GetBool(
                                 settings::SettingId::hash_join_bloom_filter)));

  join_plan->AddChild(move(children_plans_[0]));
  join_plan->AddChild(move(hash_plan));
  output_plan_ = move(join_plan);
}

void PlanGenerator::Visit(const PhysicalLeftHashJoin *) {}

void PlanGenerator::Visit(const PhysicalRightHashJoin *) {}

void PlanGenerator::Visit(const PhysicalOuterHashJoin *) {}

void PlanGenerator::Visit(const PhysicalInsert *op) {
  unique_ptr<planner::AbstractPlan> insert_plan(new planner::InsertPlan(
      storage::StorageManager::GetInstance()->GetTableWithOid(
          op->target_table->GetDatabaseOid(), op->target_table->GetTableOid()),
      op->columns, op->values));
  output_plan_ = move(insert_plan);
}

void PlanGenerator::Visit(const PhysicalInsertSelect *op) {
  unique_ptr<planner::AbstractPlan> insert_plan(new planner::InsertPlan(
      storage::StorageManager::GetInstance()->GetTableWithOid(
          op->target_table->GetDatabaseOid(),
          op->target_table->GetTableOid())));
  // Add child
  insert_plan->AddChild(move(children_plans_[0]));
  output_plan_ = move(insert_plan);
}

void PlanGenerator::Visit(const PhysicalDelete *op) {
  unique_ptr<planner::AbstractPlan> delete_plan(new planner::DeletePlan(
      storage::StorageManager::GetInstance()->GetTableWithOid(
          op->target_table->GetDatabaseOid(),
          op->target_table->GetTableOid())));

  // Add child
  delete_plan->AddChild(move(children_plans_[0]));
  output_plan_ = move(delete_plan);
}

void PlanGenerator::Visit(const PhysicalUpdate *op) {
  DirectMapList dml;
  TargetList tl;
  std::unordered_set<oid_t> update_col_ids;
  // auto schema = op->target_table->GetSchema();
  auto table_alias = op->target_table->GetTableName();
  auto exprs = GenerateTableTVExprs(table_alias, op->target_table);
  ExprMap table_expr_map;
  for (oid_t idx = 0; idx < exprs.size(); ++idx) {
    table_expr_map[exprs[idx].get()] = idx;
  }
  // Evaluate update expression and add to target list
  for (auto &update : *(op->updates)) {
    auto column_name = update->column;
    auto col_id = op->target_table->GetColumnObject(column_name)->GetColumnId();
    if (update_col_ids.find(col_id) != update_col_ids.end())
      throw SyntaxException("Multiple assignments to same column " +
                            column_name);
    update_col_ids.insert(col_id);
    expression::ExpressionUtil::EvaluateExpression({table_expr_map},
                                                   update->value.get());
    planner::DerivedAttribute attribute{update->value->Copy()};
    tl.emplace_back(col_id, attribute);
  }

  // Add other columns to direct map
  for (auto &column_id_obj_pair : op->target_table->GetColumnObjects()) {
    auto &col_id = column_id_obj_pair.first;
    if (update_col_ids.find(col_id) == update_col_ids.end())
      dml.emplace_back(col_id, std::pair<oid_t, oid_t>(0, col_id));
  }

  unique_ptr<const planner::ProjectInfo> proj_info(
      new planner::ProjectInfo(move(tl), move(dml)));

  unique_ptr<planner::AbstractPlan> update_plan(new planner::UpdatePlan(
      storage::StorageManager::GetInstance()->GetTableWithOid(
          op->target_table->GetDatabaseOid(), op->target_table->GetTableOid()),
      move(proj_info)));
  update_plan->AddChild(move(children_plans_[0]));
  output_plan_ = move(update_plan);
}

/************************* Private Functions *******************************/
vector<unique_ptr<expression::AbstractExpression>>
PlanGenerator::GenerateTableTVExprs(
    const std::string &alias, shared_ptr<catalog::TableCatalogObject> table) {
  // TODO(boweic): we seems to provide all columns here, in case where there are
  // a lot of attributes and we're only visiting a few this is not efficient
  oid_t db_id = table->GetDatabaseOid();
  oid_t table_id = table->GetTableOid();
  auto column_objects = table->GetColumnObjects();
  vector<unique_ptr<expression::AbstractExpression>> exprs(
      column_objects.size());
  for (auto &column_id_object_pair : column_objects) {
    auto &col_id = column_id_object_pair.first;
    auto &column_object = column_id_object_pair.second;
    expression::TupleValueExpression *col_expr =
        new expression::TupleValueExpression(
            column_object->GetColumnName().c_str(), alias.c_str());
    col_expr->SetValueType(column_object->GetColumnType());
    col_expr->SetBoundOid(db_id, table_id, col_id);
    exprs[col_id].reset(col_expr);
  }
  return exprs;
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
    const std::string &alias, shared_ptr<catalog::TableCatalogObject> table) {
  if (predicate_expr == nullptr) {
    return nullptr;
  }
  auto exprs = GenerateTableTVExprs(alias, table);
  ExprMap table_expr_map;
  for (oid_t idx = 0; idx < exprs.size(); ++idx) {
    table_expr_map[exprs[idx].get()] = idx;
  }
  unique_ptr<expression::AbstractExpression> predicate =
      std::unique_ptr<expression::AbstractExpression>(predicate_expr->Copy());
  expression::ExpressionUtil::EvaluateExpression({table_expr_map},
                                                 predicate.get());
  return predicate;
}

void PlanGenerator::BuildProjectionPlan() {
  if (output_cols_ == required_cols_) {
    return;
  }

  vector<ExprMap> output_expr_maps = {ExprMap{}};
  auto &child_expr_map = output_expr_maps[0];
  for (size_t idx = 0; idx < output_cols_.size(); ++idx) {
    auto &col = output_cols_[idx];
    child_expr_map[col] = idx;
  }
  TargetList tl;
  DirectMapList dml;
  vector<catalog::Column> columns;
  for (size_t idx = 0; idx < required_cols_.size(); ++idx) {
    auto &col = required_cols_[idx];
    col->DeduceExpressionType();
    if (child_expr_map.find(col) != child_expr_map.end()) {
      dml.emplace_back(idx, make_pair(0, child_expr_map[col]));
    } else {
      // Copy then evaluate the expression and add to target list
      auto col_copy = col->Copy();
      // TODO(boweic) : integrate the following two functions
      expression::ExpressionUtil::ConvertToTvExpr(col_copy, child_expr_map);
      expression::ExpressionUtil::EvaluateExpression(output_expr_maps,
                                                     col_copy);
      planner::DerivedAttribute attribute{col_copy};
      tl.emplace_back(idx, attribute);
    }
    columns.push_back(catalog::Column(
        col->GetValueType(), type::Type::GetTypeSize(col->GetValueType()),
        col->GetExpressionName()));
  }

  unique_ptr<planner::ProjectInfo> proj_info(
      new planner::ProjectInfo(move(tl), move(dml)));
  // TODO since the plan will own the schema, we may not want to use
  // shared_ptr
  // to initialize the plan
  shared_ptr<catalog::Schema> schema_ptr(new catalog::Schema(columns));
  unique_ptr<planner::AbstractPlan> project_plan(
      new planner::ProjectionPlan(move(proj_info), schema_ptr));

  if (output_plan_ != nullptr) {
    project_plan->AddChild(move(output_plan_));
  }
  output_plan_ = move(project_plan);
}

void PlanGenerator::BuildAggregatePlan(
    AggregateType aggr_type,
    const std::vector<std::shared_ptr<expression::AbstractExpression>>
        *groupby_cols,
    expression::AbstractExpression *having_predicate) {
  vector<planner::AggregatePlan::AggTerm> aggr_terms;
  vector<catalog::Column> output_schema_columns;
  DirectMapList dml;
  TargetList tl;
  PL_ASSERT(children_expr_map_.size() == 1);
  auto &child_expr_map = children_expr_map_[0];

  auto agg_id = 0;
  for (size_t idx = 0; idx < output_cols_.size(); ++idx) {
    auto expr = output_cols_[idx];
    expr->DeduceExpressionType();
    expression::ExpressionUtil::EvaluateExpression(children_expr_map_, expr);
    if (expression::ExpressionUtil::IsAggregateExpression(
            expr->GetExpressionType())) {
      auto agg_expr = reinterpret_cast<expression::AggregateExpression *>(expr);
      auto agg_col = expr->GetModifiableChild(0);
      // Maps the aggregate value in th right tuple to the output
      // See aggregateor.cpp for more detail
      dml.emplace_back(idx, make_pair(1, agg_id++));
      aggr_terms.emplace_back(agg_expr->GetExpressionType(),
                              agg_col == nullptr ? nullptr : agg_col->Copy(),
                              agg_expr->distinct_);
    } else if (child_expr_map.find(expr) != child_expr_map.end()) {
      dml.emplace_back(idx, make_pair(0, child_expr_map[expr]));
    } else {
      planner::DerivedAttribute attribute{expr};
      tl.emplace_back(idx, attribute);
    }
    output_schema_columns.push_back(catalog::Column(
        expr->GetValueType(), type::Type::GetTypeSize(expr->GetValueType()),
        expr->expr_name_));
  }
  // Generate group by ids
  vector<oid_t> col_ids;
  if (groupby_cols != nullptr) {
    for (auto &col : *groupby_cols) {
      col_ids.push_back(child_expr_map[col.get()]);
    }
  }

  // Generate the Aggregate Plan
  unique_ptr<const planner::ProjectInfo> proj_info(
      new planner::ProjectInfo(move(tl), move(dml)));
  unique_ptr<const expression::AbstractExpression> predicate(having_predicate);
  // TODO(boweic): Ditto, since the aggregate plan will own the schema, we may
  // want make the parameter as unique_ptr
  shared_ptr<const catalog::Schema> output_table_schema(
      new catalog::Schema(output_schema_columns));

  auto agg_plan = new planner::AggregatePlan(move(proj_info), move(predicate),
                                             move(aggr_terms), move(col_ids),
                                             output_table_schema, aggr_type);
  agg_plan->AddChild(move(children_plans_[0]));
  output_plan_.reset(agg_plan);
}

void PlanGenerator::GenerateProjectionForJoin(
    std::unique_ptr<const planner::ProjectInfo> &proj_info,
    std::shared_ptr<const catalog::Schema> &proj_schema) {
  PL_ASSERT(children_expr_map_.size() == 2);
  PL_ASSERT(children_plans_.size() == 2);

  TargetList tl = TargetList();
  // columns which can be returned directly
  DirectMapList dml = DirectMapList();
  // schema of the projections output
  vector<catalog::Column> columns;
  size_t output_offset = 0;
  auto &l_child_expr_map = children_expr_map_[0];
  auto &r_child_expr_map = children_expr_map_[1];
  for (auto &expr : output_cols_) {
    // auto expr_type = expr->GetExpressionType();
    expression::ExpressionUtil::EvaluateExpression(children_expr_map_, expr);
    if (l_child_expr_map.count(expr)) {
      dml.emplace_back(output_offset, make_pair(0, l_child_expr_map[expr]));
    } else if (r_child_expr_map.count(expr)) {
      dml.emplace_back(output_offset, make_pair(1, r_child_expr_map[expr]));
    } else {
      // For more complex expression, we need to do evaluation in Executor

      planner::DerivedAttribute attribute{expr->Copy()};
      tl.emplace_back(output_offset, attribute);
    }
    columns.push_back(catalog::Column(
        expr->GetValueType(), type::Type::GetTypeSize(expr->GetValueType()),
        expr->GetExpressionName()));
    output_offset++;
  }

  // build the projection plan node and insert above the join
  proj_info = std::unique_ptr<planner::ProjectInfo>(
      new planner::ProjectInfo(move(tl), move(dml)));
  proj_schema = std::make_shared<const catalog::Schema>(columns);
}
}  // namespace optimizer
}  // namespace peloton
