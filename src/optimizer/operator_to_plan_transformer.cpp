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
#include "optimizer/util.h"
#include "planner/hash_join_plan.h"
#include "planner/nested_loop_join_plan.h"
#include "planner/projection_plan.h"
#include "planner/order_by_plan.h"
#include "planner/limit_plan.h"
#include "planner/hash_plan.h"
#include "planner/index_scan_plan.h"
#include "planner/abstract_join_plan.h"
#include "expression/aggregate_expression.h"
#include "planner/seq_scan_plan.h"

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

void OperatorToPlanTransformer::Visit(const DummyScan *) {
  // DummyScan is used in case of SELECT without FROM so that enforcer
  // can enforce a PhysicalProjection on top of DummyScan to generate correct
  // result. But here, no need to translate DummyScan to any physical plan.
  output_plan_ = nullptr;
}

void OperatorToPlanTransformer::Visit(const PhysicalSeqScan *op) {
  // Generate column ids to pass into scan plan and generate output expr map
  auto column_prop = requirements_->GetPropertyOfType(PropertyType::COLUMNS)
                         ->As<PropertyColumns>();
  vector<oid_t> column_ids =
      GenerateColumnsForScan(column_prop, op->table_alias, op->table_);
  // Add Scan Predicates
  auto predicate_prop =
      requirements_->GetPropertyOfType(PropertyType::PREDICATE)
          ->As<PropertyPredicate>();
  expression::AbstractExpression *predicate =
      GeneratePredicateForScan(predicate_prop, op->table_alias, op->table_);

  // Create scan plan
  unique_ptr<planner::AbstractPlan> seq_scan_plan(
      new planner::SeqScanPlan(op->table_, predicate, column_ids));
  output_plan_ = move(seq_scan_plan);
}

void OperatorToPlanTransformer::Visit(const PhysicalIndexScan *op) {
  auto predicate_prop =
      requirements_->GetPropertyOfType(PropertyType::PREDICATE)
          ->As<PropertyPredicate>();

  expression::AbstractExpression *predicate =
      GeneratePredicateForScan(predicate_prop, op->table_alias, op->table_);
  vector<oid_t> key_column_ids;
  vector<ExpressionType> expr_types;
  vector<type::Value> values;
  oid_t index_id = 0;

  if (!util::CheckIndexSearchable(op->table_, predicate, key_column_ids,
                                  expr_types, values, index_id)) {
    // Can't be accelerated by index scan
    // Just scan all keys using the first index
    index_id = 0;
    key_column_ids.clear();
    expr_types.clear();
    values.clear();
  }

  // Generate column ids to pass into scan plan and generate output expr map
  auto column_prop = requirements_->GetPropertyOfType(PropertyType::COLUMNS)
                         ->As<PropertyColumns>();
  vector<oid_t> column_ids =
      GenerateColumnsForScan(column_prop, op->table_alias, op->table_);

  // Create index scan plan
  auto index = op->table_->GetIndex(index_id);
  vector<expression::AbstractExpression *> runtime_keys;

  // Create index scan desc
  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      index, key_column_ids, expr_types, values, runtime_keys);

  // Create plan node.
  std::unique_ptr<planner::IndexScanPlan> index_scan_plan(
      new planner::IndexScanPlan(op->table_, predicate, column_ids,
                                 index_scan_desc, false));

  output_plan_ = move(index_scan_plan);
}

void OperatorToPlanTransformer::Visit(const PhysicalProject *) {
  auto cols_prop = requirements_->GetPropertyOfType(PropertyType::COLUMNS)
                       ->As<PropertyColumns>();
  size_t col_size = cols_prop->GetSize();
  ExprMap &child_expr_map = children_expr_map_[0];

  // first expand the star expression to include all exprs beneath
  vector<shared_ptr<expression::AbstractExpression>> output_exprs;
  for (size_t i = 0; i < col_size; i++) {
    auto expr = cols_prop->GetColumn(i);
    if (expr->GetExpressionType() == ExpressionType::STAR) {
      vector<std::shared_ptr<expression::AbstractExpression>> ordered_exprs =
          expression::ExpressionUtil::GenerateOrderedOutputExprs(
              child_expr_map);
      output_exprs.insert(output_exprs.end(), ordered_exprs.begin(),
                          ordered_exprs.end());
    } else {
      expression::ExpressionUtil::ConvertAggExprToTvExpr(expr.get(),
                                                         child_expr_map);
      output_exprs.push_back(expr);
    }
  }

  // expressions to evaluate
  TargetList tl = TargetList();
  // columns which can be returned directly
  DirectMapList dml = DirectMapList();
  // schema of the projections output
  vector<catalog::Column> columns;
  size_t curr_col_offset = 0;
  for (auto expr : output_exprs) {
    auto expr_type = expr->GetExpressionType();
    if (expr_type == ExpressionType::VALUE_TUPLE) {
      // For TupleValueExpr, we can just do a direct mapping.
      dml.emplace_back(curr_col_offset, make_pair(0, child_expr_map[expr]));
    } else {
      // For more complex expression, we need to do evaluation in Executor
      expression::ExpressionUtil::EvaluateExpression(children_expr_map_,
                                                     expr.get());
      planner::DerivedAttribute attribute{expr->Copy()};
      tl.emplace_back(curr_col_offset, attribute);
    }
    (*output_expr_map_)[expr] = curr_col_offset++;
    columns.push_back(catalog::Column(
        expr->GetValueType(), type::Type::GetTypeSize(expr->GetValueType()),
        expr->GetExpressionName()));
  }

  // build the projection plan node and insert above the scan
  unique_ptr<planner::ProjectInfo> proj_info(
      new planner::ProjectInfo(move(tl), move(dml)));
  shared_ptr<catalog::Schema> schema_ptr(new catalog::Schema(columns));
  unique_ptr<planner::AbstractPlan> project_plan(
      new planner::ProjectionPlan(move(proj_info), schema_ptr));

  PL_ASSERT(children_plans_.size() < 2);
  if (!children_plans_.empty())
    project_plan->AddChild(move(children_plans_[0]));

  output_plan_ = move(project_plan);
}

void OperatorToPlanTransformer::Visit(const PhysicalLimit *) {
  PL_ASSERT(children_plans_.size() == 1);

  auto limit_prop = requirements_->GetPropertyOfType(PropertyType::LIMIT)
                        ->As<PropertyLimit>();

  // Limit Operator does not change the column mapping
  *output_expr_map_ = children_expr_map_[0];

  unique_ptr<planner::AbstractPlan> limit_plan(
      new planner::LimitPlan(limit_prop->GetLimit(), limit_prop->GetOffset()));
  limit_plan->AddChild(move(children_plans_[0]));
  output_plan_ = move(limit_plan);
}

void OperatorToPlanTransformer::Visit(const PhysicalOrderBy *) {
  ExprMap &child_expr_map = children_expr_map_[0];

  auto cols_prop = requirements_->GetPropertyOfType(PropertyType::COLUMNS)
                       ->As<PropertyColumns>();
  auto sort_prop =
      requirements_->GetPropertyOfType(PropertyType::SORT)->As<PropertySort>();
  auto sort_columns_size = sort_prop->GetSortColumnSize();

  vector<shared_ptr<expression::AbstractExpression>> ordered_exprs =
      expression::ExpressionUtil::GenerateOrderedOutputExprs(child_expr_map);
  vector<oid_t> column_ids;

  // Construct output column offset.
  for (size_t i = 0; i < cols_prop->GetSize(); i++) {
    auto expr = cols_prop->GetColumn(i);
    if (expr->GetExpressionType() == ExpressionType::STAR) {
      // For StarExpr, Output all expressions from operator below in order
      for (size_t j = 0; j < ordered_exprs.size(); j++) {
        auto expr = ordered_exprs[j];
        if (expr->GetExpressionType() == ExpressionType::VALUE_TUPLE) {
          (*output_expr_map_)[expr] = column_ids.size();
          column_ids.emplace_back(j);
        }
      }
    } else {
      (*output_expr_map_)[expr] = column_ids.size();
      column_ids.push_back(child_expr_map[expr]);
    }
  }

  // Construct sort ids and sort flags
  vector<oid_t> sort_col_ids;
  vector<bool> sort_flags;
  for (size_t i = 0; i < sort_columns_size; i++) {
    sort_col_ids.push_back(child_expr_map[sort_prop->GetSortColumn(i)]);
    // planner use desc flag
    sort_flags.push_back(!sort_prop->GetSortAscending(i));
  }

  // Create and insert OrderBy Plan
  unique_ptr<planner::AbstractPlan> order_by_plan(
      new planner::OrderByPlan(sort_col_ids, sort_flags, column_ids));
  order_by_plan->AddChild(move(children_plans_[0]));
  output_plan_ = move(order_by_plan);
}

void OperatorToPlanTransformer::Visit(const PhysicalHashGroupBy *op) {
  auto col_prop = requirements_->GetPropertyOfType(PropertyType::COLUMNS)
                      ->As<PropertyColumns>();

  PL_ASSERT(col_prop != nullptr);

  output_plan_ = GenerateAggregatePlan(col_prop, AggregateType::HASH,
                                            &op->columns, op->having);
}

void OperatorToPlanTransformer::Visit(const PhysicalSortGroupBy *op) {
  auto col_prop = requirements_->GetPropertyOfType(PropertyType::COLUMNS)
                      ->As<PropertyColumns>();

  PL_ASSERT(col_prop != nullptr);

  output_plan_ = GenerateAggregatePlan(col_prop, AggregateType::SORTED,
                                            &op->columns, op->having);
}

void OperatorToPlanTransformer::Visit(const PhysicalAggregate *) {
  auto col_prop = requirements_->GetPropertyOfType(PropertyType::COLUMNS)
                      ->As<PropertyColumns>();

  PL_ASSERT(col_prop != nullptr);

  output_plan_ = GenerateAggregatePlan(col_prop, AggregateType::PLAIN, nullptr, nullptr);
}

void OperatorToPlanTransformer::Visit(const PhysicalDistinct *) {
  ExprMap &child_expr_map = children_expr_map_[0];

  auto prop_distinct = requirements_->GetPropertyOfType(PropertyType::DISTINCT)
                           ->As<PropertyDistinct>();

  std::vector<std::unique_ptr<const expression::AbstractExpression>> hash_keys;
  auto distinct_column_size = prop_distinct->GetSize();
  // Hash executor uses TVexpr to store the column,
  // but only uses their offset, we may want to modify
  // the interface to the offset directly
  for (size_t idx = 0; idx < distinct_column_size; ++idx) {
    auto expr = prop_distinct->GetDistinctColumn(idx);
    if (expr->GetExpressionType() == ExpressionType::STAR) {
      // If the hash key contains the star expr
      // add all TupleValueExpr to hash_keys
      for (auto &expr_idx_pair : child_expr_map) {
        auto &input_expr = expr_idx_pair.first;
        if (input_expr->GetExpressionType() == ExpressionType::VALUE_TUPLE) {
          auto &column_idx = expr_idx_pair.second;
          auto col_expr = new expression::TupleValueExpression("");
          col_expr->SetValueIdx(static_cast<int>(column_idx));
          hash_keys.emplace_back(col_expr);
        }
      }
    } else {
      auto column_idx = child_expr_map[expr];
      auto col_expr = new expression::TupleValueExpression("");
      col_expr->SetValueIdx(static_cast<int>(column_idx));
      hash_keys.emplace_back(col_expr);
    }
  }
  unique_ptr<planner::HashPlan> hash_plan(new planner::HashPlan(hash_keys));
  hash_plan->AddChild(move(children_plans_[0]));
  output_plan_ = move(hash_plan);

  // Hash does not change the layout of the column mapping
  *output_expr_map_ = move(child_expr_map);
}

void OperatorToPlanTransformer::Visit(const PhysicalFilter *) {}

void OperatorToPlanTransformer::Visit(const PhysicalInnerNLJoin *op) {
  output_plan_ = GenerateJoinPlan((op->join_predicate).get(), JoinType::INNER, false);
}

void OperatorToPlanTransformer::Visit(const PhysicalLeftNLJoin *) {}

void OperatorToPlanTransformer::Visit(const PhysicalRightNLJoin *) {}

void OperatorToPlanTransformer::Visit(const PhysicalOuterNLJoin *) {}

void OperatorToPlanTransformer::Visit(const PhysicalInnerHashJoin *op) {
  output_plan_ =
      GenerateJoinPlan((op->join_predicate).get(), JoinType::INNER, true);
}

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
  DirectMapList dml;
  TargetList tl;
  std::unordered_set<oid_t> update_col_ids;
  auto schema = op->target_table->GetSchema();
  auto table_alias = op->target_table->GetName();
  GenerateTableExprMap(table_expr_map, table_alias, op->target_table);

  // Evaluate update expression and add to target list
  for (auto update : op->updates) {
    auto column = std::string(update->column);
    auto col_id = schema->GetColumnID(column);
    if (update_col_ids.find(col_id) != update_col_ids.end())
      throw SyntaxException("Multiple assignments to same column " + column);
    update_col_ids.insert(col_id);
    expression::ExpressionUtil::EvaluateExpression({table_expr_map},
                                                   update->value);
    planner::DerivedAttribute attribute{update->value->Copy()};
    tl.emplace_back(col_id, attribute);
  }

  // Add other columns to direct map
  auto col_size = schema->GetColumnCount();
  for (size_t i = 0; i < col_size; i++) {
    if (update_col_ids.find(i) == update_col_ids.end())
      dml.emplace_back(i, std::pair<oid_t, oid_t>(0, i));
  }

  unique_ptr<const planner::ProjectInfo> proj_info(
      new planner::ProjectInfo(move(tl), move(dml)));

  unique_ptr<planner::AbstractPlan> update_plan(
      new planner::UpdatePlan(op->target_table, move(proj_info)));
  update_plan->AddChild(move(children_plans_[0]));
  output_plan_ = move(update_plan);
}

/************************* Private Functions *******************************/
// Generate expr map for all the columns in the given table. Used to evaluate
// the predicate.
void OperatorToPlanTransformer::GenerateTableExprMap(
    ExprMap &expr_map, const std::string &alias,
    const storage::DataTable *table) {
  auto db_id = table->GetDatabaseOid();
  oid_t table_id = table->GetOid();
  size_t num_col = table->GetSchema()->GetColumnCount();
  for (oid_t col_id = 0; col_id < num_col; ++col_id) {
    // Only bound_obj_id is needed for expr_map
    // TODO potential memory leak here?
    auto col_expr = shared_ptr<expression::TupleValueExpression>(
        new expression::TupleValueExpression("", alias.c_str()));
    col_expr->SetValueType(table->GetSchema()->GetColumn(col_id).GetType());
    col_expr->SetBoundOid(db_id, table_id, col_id);
    expr_map[col_expr] = col_id;
  }
}

// Generate columns for scan plan
vector<oid_t> OperatorToPlanTransformer::GenerateColumnsForScan(
    const PropertyColumns *column_prop, const std::string &alias,
    const storage::DataTable *table) {
  vector<oid_t> column_ids;
  if (column_prop->HasStarExpression()) {
    size_t num_col = table->GetSchema()->GetColumnCount();
    for (oid_t col_id = 0; col_id < num_col; ++col_id)
      column_ids.push_back(col_id);
    GenerateTableExprMap(*output_expr_map_, alias, table);
    auto t = *output_expr_map_;
    LOG_INFO("%ld", t.size());
  } else {
    auto output_column_size = column_prop->GetSize();
    for (oid_t idx = 0; idx < output_column_size; ++idx) {
      auto output_expr = column_prop->GetColumn(idx);
      auto output_tvexpr =
          (expression::TupleValueExpression *)output_expr.get();

      // Set column offset
      PL_ASSERT(output_tvexpr->GetIsBound() == true);
      auto col_id = std::get<2>(output_tvexpr->GetBoundOid());
      column_ids.push_back(col_id);
      (*output_expr_map_)[output_expr] = idx;
    }
  }

  return column_ids;
}

// Generate predicate for scan plan
expression::AbstractExpression *
OperatorToPlanTransformer::GeneratePredicateForScan(
    const PropertyPredicate *predicate_prop, const std::string &alias,
    const storage::DataTable *table) {
  expression::AbstractExpression *predicate = nullptr;
  if (predicate_prop != nullptr) {
    ExprMap table_expr_map;
    GenerateTableExprMap(table_expr_map, alias, table);
    predicate = predicate_prop->GetPredicate()->Copy();
    expression::ExpressionUtil::EvaluateExpression({table_expr_map}, predicate);
  }
  return predicate;
}

std::unique_ptr<planner::AggregatePlan>
OperatorToPlanTransformer::GenerateAggregatePlan(
    const PropertyColumns *prop_col, AggregateType agg_type,
    const std::vector<std::shared_ptr<expression::AbstractExpression>> *
        group_by_exprs,
    expression::AbstractExpression *having) {
  auto child_expr_map = children_expr_map_[0];

  vector<planner::AggregatePlan::AggTerm> agg_terms;
  vector<catalog::Column> output_schema_columns;

  // Metadata needed in AggregationPlan
  DirectMapList dml;
  TargetList tl;

  auto agg_id = 0;
  auto expr_len = prop_col->GetSize();
  for (size_t col_pos = 0; col_pos < expr_len; col_pos++) {
    auto expr = prop_col->GetColumn(col_pos);
    expression::ExpressionUtil::EvaluateExpression(children_expr_map_,
                                                   expr.get());

    if (expression::ExpressionUtil::IsAggregateExpression(
            expr->GetExpressionType())) {
      // For AggregateExpr, add aggregate term
      auto agg_expr = (expression::AggregateExpression *)expr.get();
      auto agg_col = expr->GetModifiableChild(0);

      // Maps the aggregate value in the right tuple to the output.
      // See aggregator.cpp for more info.
      dml.emplace_back(col_pos, make_pair(1, agg_id++));
      planner::AggregatePlan::AggTerm agg_term(
          agg_expr->GetExpressionType(),
          agg_col == nullptr ? nullptr : agg_col->Copy(), agg_expr->distinct_);
      agg_terms.push_back(agg_term);
    } else if (expr->GetExpressionType() == ExpressionType::VALUE_TUPLE) {
      // For TupleValueExpr, do direct mapping
      dml.emplace_back(col_pos, make_pair(0, child_expr_map[expr]));
    } else {
      // For other exprs such as OperatorExpr, use target list
      planner::DerivedAttribute attribute{expr->Copy()};
      tl.emplace_back(col_pos, attribute);
    }

    (*output_expr_map_)[expr] = col_pos;
    output_schema_columns.push_back(catalog::Column(
        expr->GetValueType(), type::Type::GetTypeSize(expr->GetValueType()),
        expr->expr_name_));
  }

  // Generate group by ids
  vector<oid_t> col_ids;
  if (group_by_exprs != nullptr)
    for (auto col : *group_by_exprs) col_ids.push_back(child_expr_map[col]);

  // Handle having clause
  expression::AbstractExpression *having_predicate = nullptr;
  if (having != nullptr) {
    expression::ExpressionUtil::EvaluateExpression(children_expr_map_, having);
    having_predicate = having->Copy();
  }

  // Generate the Aggregate Plan
  unique_ptr<const planner::ProjectInfo> proj_info(
      new planner::ProjectInfo(move(tl), move(dml)));
  unique_ptr<const expression::AbstractExpression> predicate(having_predicate);
  shared_ptr<const catalog::Schema> output_table_schema(
      new catalog::Schema(output_schema_columns));
  unique_ptr<planner::AggregatePlan> agg_plan(new planner::AggregatePlan(
      move(proj_info), move(predicate), move(agg_terms), move(col_ids),
      output_table_schema, agg_type));
  agg_plan->AddChild(move(children_plans_[0]));
  return agg_plan;
}

unique_ptr<planner::AbstractPlan> OperatorToPlanTransformer::GenerateJoinPlan(
    expression::AbstractExpression *join_predicate, JoinType join_type,
    bool is_hash) {
  auto cols_prop = requirements_->GetPropertyOfType(PropertyType::COLUMNS)
                       ->As<PropertyColumns>();

  PL_ASSERT(children_expr_map_.size() == 2);
  auto &l_child_map = children_expr_map_[0];
  auto &r_child_map = children_expr_map_[1];

  // Retrive output columns
  vector<shared_ptr<expression::AbstractExpression>> output_exprs;
  size_t col_size = cols_prop->GetSize();
  for (size_t curr_col_offset = 0; curr_col_offset < col_size;
       ++curr_col_offset) {
    auto expr = cols_prop->GetColumn(curr_col_offset);
    // Generate all output columns from child
    if (expr->GetExpressionType() == ExpressionType::STAR) {
      vector<std::shared_ptr<expression::AbstractExpression>> l_output_exprs =
          expression::ExpressionUtil::GenerateOrderedOutputExprs(
              children_expr_map_[0]);
      vector<std::shared_ptr<expression::AbstractExpression>> r_output_exprs =
          expression::ExpressionUtil::GenerateOrderedOutputExprs(
              children_expr_map_[1]);
      output_exprs.insert(output_exprs.end(), l_output_exprs.begin(),
                          l_output_exprs.end());
      output_exprs.insert(output_exprs.end(), r_output_exprs.begin(),
                          r_output_exprs.end());
    }
    else
      output_exprs.emplace_back(expr);
  }

  // expressions to evaluate
  TargetList tl = TargetList();
  // columns which can be returned directly
  DirectMapList dml = DirectMapList();
  // schema of the projections output
  vector<catalog::Column> columns;
  size_t output_size = output_exprs.size();
  for (size_t output_offset = 0; output_offset<output_size; output_offset++) {
    auto expr = output_exprs[output_offset];
    auto expr_type = expr->GetExpressionType();

    expression::ExpressionUtil::EvaluateExpression(children_expr_map_, expr.get());
    if (expr_type == ExpressionType::VALUE_TUPLE) {
      // For TupleValueExpr, we can just do a direct mapping.
      if (l_child_map.count(expr))
        dml.emplace_back(output_offset, make_pair(0, l_child_map[expr]));
      else
        dml.emplace_back(output_offset, make_pair(1, r_child_map[expr]));
    } else {
      // For more complex expression, we need to do evaluation in Executor

      planner::DerivedAttribute attribute{expr->Copy()};
      tl.emplace_back(output_offset, attribute);
    }
    (*output_expr_map_)[expr] = output_offset;
    columns.push_back(catalog::Column(
        expr->GetValueType(), type::Type::GetTypeSize(expr->GetValueType()),
        expr->GetExpressionName()));
  }

  // build the projection plan node and insert above the join
  unique_ptr<const planner::ProjectInfo> proj_info(
      new planner::ProjectInfo(move(tl), move(dml)));
  shared_ptr<const catalog::Schema> schema_ptr(new catalog::Schema(columns));

  // Combine predicate in the where clause
  // with predicate in join clause
  // TODO the two predicate should not be combined
  // But hash plan currently only have one predicate
  vector<expression::AbstractExpression *> predicates;
  auto predicate_prop =
      requirements_->GetPropertyOfType(PropertyType::PREDICATE)
          ->As<PropertyPredicate>();

  if (predicate_prop != nullptr) {
    auto where_predicate = predicate_prop->GetPredicate()->Copy();
    LOG_TRACE("where_predicate %s", where_predicate->GetInfo().c_str());
    predicates.emplace_back(where_predicate);
  }

  // Extract join columns
  expression::ExpressionUtil::EvaluateExpression(children_expr_map_,
                                                 join_predicate);
  vector<unique_ptr<const expression::AbstractExpression>> left_hash_keys,
      right_hash_keys;
  auto remaining_predicate = expression::ExpressionUtil::ExtractJoinColumns(
      left_hash_keys, right_hash_keys, join_predicate);
  if (remaining_predicate != nullptr) {
    LOG_TRACE("remaining %s", remaining_predicate->GetInfo().c_str());
    predicates.emplace_back(remaining_predicate);
  }

  // Generate the combined predicate and evaluate it
  unique_ptr<expression::AbstractExpression> predicate{
      util::CombinePredicates(predicates)};
  expression::ExpressionUtil::EvaluateExpression(children_expr_map_,
                                                 predicate.get());

  unique_ptr<planner::AbstractPlan> join_plan;
  if (is_hash) {
    // Generate hash join plan
    PL_ASSERT(left_hash_keys.size() == right_hash_keys.size());
    PL_ASSERT(left_hash_keys.size() != 0);

    // Hash keys for hash plan is
    // a copy of right hash key
    vector<unique_ptr<const expression::AbstractExpression>> hash_keys;
    for (auto &expr : right_hash_keys) hash_keys.emplace_back(expr->Copy());

    unique_ptr<planner::HashPlan> hash_plan(new planner::HashPlan(hash_keys));
    hash_plan->AddChild(move(children_plans_[1]));

    join_plan = unique_ptr<planner::AbstractPlan>(
        new planner::HashJoinPlan(join_type, move(predicate), move(proj_info),
                                  schema_ptr, left_hash_keys, right_hash_keys));

    join_plan->AddChild(move(children_plans_[0]));
    join_plan->AddChild(move(hash_plan));
  } else {
    // NL Join plan use offset for join column
    vector<oid_t> left_join_col_ids, right_join_col_ids;
    for (auto &expr : left_hash_keys)
      left_join_col_ids.emplace_back(
          reinterpret_cast<const expression::TupleValueExpression *>(expr.get())
              ->GetColumnId());

    for (auto &expr : right_hash_keys)
      right_join_col_ids.emplace_back(
          reinterpret_cast<const expression::TupleValueExpression *>(expr.get())
              ->GetColumnId());

    join_plan =
        unique_ptr<planner::AbstractPlan>(new planner::NestedLoopJoinPlan(
            join_type, move(predicate), move(proj_info), schema_ptr,
            left_join_col_ids, right_join_col_ids));
    join_plan->AddChild(move(children_plans_[0]));
    join_plan->AddChild(move(children_plans_[1]));
  }
  return join_plan;
}

void OperatorToPlanTransformer::VisitOpExpression(
    shared_ptr<OperatorExpression> op) {
  op->Op().Accept(this);
}

} /* namespace optimizer */
} /* namespace peloton */
