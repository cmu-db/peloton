//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rule_impls.cpp
//
// Identification: src/optimizer/rule_impls.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "catalog/index_catalog.h"
#include "catalog/table_catalog.h"
#include "catalog/column_catalog.h"
#include "optimizer/rule_impls.h"
#include "optimizer/util.h"
#include "optimizer/operators.h"
#include "storage/data_table.h"
#include "optimizer/properties.h"
#include "optimizer/optimizer_metadata.h"

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Transformation rules
//===--------------------------------------------------------------------===//

///////////////////////////////////////////////////////////////////////////////
/// InnerJoinCommutativity
InnerJoinCommutativity::InnerJoinCommutativity() {
  type_ = RuleType::INNER_JOIN_COMMUTE;

  std::shared_ptr<Pattern> left_child(std::make_shared<Pattern>(OpType::Leaf));
  std::shared_ptr<Pattern> right_child(std::make_shared<Pattern>(OpType::Leaf));
  match_pattern = std::make_shared<Pattern>(OpType::InnerJoin);
  match_pattern->AddChild(left_child);
  match_pattern->AddChild(right_child);
}

bool InnerJoinCommutativity::Check(std::shared_ptr<OperatorExpression> expr,
                                   OptimizeContext *context) const {
  (void)context;
  (void)expr;
  return true;
}

void InnerJoinCommutativity::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed,
    UNUSED_ATTRIBUTE OptimizeContext *context) const {
  auto join_op = input->Op().As<LogicalInnerJoin>();
  auto join_predicates =
      std::vector<AnnotatedExpression>(join_op->join_predicates);
  auto result_plan = std::make_shared<OperatorExpression>(
      LogicalInnerJoin::make(join_predicates));
  std::vector<std::shared_ptr<OperatorExpression>> children = input->Children();
  PL_ASSERT(children.size() == 2);
  LOG_TRACE(
      "Reorder left child with op %s and right child with op %s for inner join",
      children[0]->Op().name().c_str(), children[1]->Op().name().c_str());
  result_plan->PushChild(children[1]);
  result_plan->PushChild(children[0]);

  transformed.push_back(result_plan);
}

//===--------------------------------------------------------------------===//
// Implementation rules
//===--------------------------------------------------------------------===//

///////////////////////////////////////////////////////////////////////////////
/// GetToDummyScan
GetToDummyScan::GetToDummyScan() {
  type_ = RuleType::GET_TO_DUMMY_SCAN;

  match_pattern = std::make_shared<Pattern>(OpType::Get);
}

bool GetToDummyScan::Check(std::shared_ptr<OperatorExpression> plan,
                           OptimizeContext *context) const {
  (void)context;
  const LogicalGet *get = plan->Op().As<LogicalGet>();
  return get->table == nullptr;
}

void GetToDummyScan::Transform(
    UNUSED_ATTRIBUTE std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed,
    UNUSED_ATTRIBUTE OptimizeContext *context) const {
  auto result_plan = std::make_shared<OperatorExpression>(DummyScan::make());

  transformed.push_back(result_plan);
}

///////////////////////////////////////////////////////////////////////////////
/// GetToSeqScan
GetToSeqScan::GetToSeqScan() {
  type_ = RuleType::GET_TO_SEQ_SCAN;

  match_pattern = std::make_shared<Pattern>(OpType::Get);
}

bool GetToSeqScan::Check(std::shared_ptr<OperatorExpression> plan,
                         OptimizeContext *context) const {
  (void)context;
  const LogicalGet *get = plan->Op().As<LogicalGet>();
  return get->table != nullptr;
}

void GetToSeqScan::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed,
    UNUSED_ATTRIBUTE OptimizeContext *context) const {
  const LogicalGet *get = input->Op().As<LogicalGet>();

  auto result_plan = std::make_shared<OperatorExpression>(
      PhysicalSeqScan::make(get->get_id, get->table, get->table_alias,
                            get->predicates, get->is_for_update));

  UNUSED_ATTRIBUTE std::vector<std::shared_ptr<OperatorExpression>> children =
      input->Children();
  PL_ASSERT(children.size() == 0);

  transformed.push_back(result_plan);
}

///////////////////////////////////////////////////////////////////////////////
/// GetToIndexScan
GetToIndexScan::GetToIndexScan() {
  type_ = RuleType::GET_TO_INDEX_SCAN;

  match_pattern = std::make_shared<Pattern>(OpType::Get);
}

bool GetToIndexScan::Check(std::shared_ptr<OperatorExpression> plan,
                           OptimizeContext *context) const {
  // If there is a index for the table, return true,
  // else return false
  (void)context;
  const LogicalGet *get = plan->Op().As<LogicalGet>();
  bool index_exist = false;
  if (get != nullptr && get->table != nullptr &&
      !get->table->GetIndexObjects().empty()) {
    index_exist = true;
  }
  return index_exist;
}

void GetToIndexScan::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed,
    UNUSED_ATTRIBUTE OptimizeContext *context) const {
  UNUSED_ATTRIBUTE std::vector<std::shared_ptr<OperatorExpression>> children =
      input->Children();
  PL_ASSERT(children.size() == 0);

  const LogicalGet *get = input->Op().As<LogicalGet>();

  // Get sort columns if they are all base columns and all in asc order
  auto sort = context->required_prop->GetPropertyOfType(PropertyType::SORT);
  std::vector<oid_t> sort_col_ids;
  if (sort != nullptr) {
    auto sort_prop = sort->As<PropertySort>();
    bool sort_by_asc_base_column = true;
    for (size_t i = 0; i < sort_prop->GetSortColumnSize(); i++) {
      auto expr = sort_prop->GetSortColumn(i);
      if (!sort_prop->GetSortAscending(i) ||
          expr->GetExpressionType() != ExpressionType::VALUE_TUPLE) {
        sort_by_asc_base_column = false;
        break;
      }
      auto bound_oids =
          reinterpret_cast<expression::TupleValueExpression *>(expr)
              ->GetBoundOid();
      sort_col_ids.push_back(std::get<2>(bound_oids));
    }
    // Check whether any index can fulfill sort property
    if (sort_by_asc_base_column) {
      for (auto &index_id_object_pair : get->table->GetIndexObjects()) {
        auto &index_id = index_id_object_pair.first;
        auto &index = index_id_object_pair.second;
        auto &index_col_ids = index->GetKeyAttrs();
        // We want to ensure that Sort(a, b, c, d, e) can fit Sort(a, b, c)
        size_t l_num_sort_columns = index_col_ids.size();
        size_t r_num_sort_columns = sort_col_ids.size();
        if (l_num_sort_columns < r_num_sort_columns) {
          continue;
        }
        bool index_matched = true;
        for (size_t idx = 0; idx < r_num_sort_columns; ++idx) {
          if (index_col_ids[idx] != sort_col_ids[idx]) {
            index_matched = false;
            break;
          }
        }
        // Add transformed plan if found
        if (index_matched) {
          LOG_DEBUG("Index id :%u", index_id);
          auto index_scan_op = PhysicalIndexScan::make(
              get->get_id, get->table, get->table_alias, get->predicates,
              get->is_for_update, index_id, {}, {}, {});
          transformed.push_back(
              std::make_shared<OperatorExpression>(index_scan_op));
        }
      }
    }
  }

  // Check whether any index can fulfill predicate predicate evaluation
  if (!get->predicates.empty()) {
    std::vector<oid_t> key_column_id_list;
    std::vector<ExpressionType> expr_type_list;
    std::vector<type::Value> value_list;
    for (auto &pred : get->predicates) {
      auto expr = pred.expr.get();
      if (expr->GetChildrenSize() != 2) continue;
      auto expr_type = expr->GetExpressionType();
      expression::AbstractExpression *tv_expr = nullptr;
      expression::AbstractExpression *value_expr = nullptr;

      // Fetch column reference and value
      if (expr->GetChild(0)->GetExpressionType() ==
          ExpressionType::VALUE_TUPLE) {
        auto r_type = expr->GetChild(1)->GetExpressionType();
        if (r_type == ExpressionType::VALUE_CONSTANT ||
            r_type == ExpressionType::VALUE_PARAMETER) {
          tv_expr = expr->GetModifiableChild(0);
          value_expr = expr->GetModifiableChild(1);
        }
      } else if (expr->GetChild(1)->GetExpressionType() ==
                 ExpressionType::VALUE_TUPLE) {
        auto l_type = expr->GetChild(0)->GetExpressionType();
        if (l_type == ExpressionType::VALUE_CONSTANT ||
            l_type == ExpressionType::VALUE_PARAMETER) {
          tv_expr = expr->GetModifiableChild(1);
          value_expr = expr->GetModifiableChild(0);
          expr_type =
              expression::ExpressionUtil::ReverseComparisonExpressionType(
                  expr_type);
        }
      }

      // If found valid tv_expr and value_expr, update col_id_list,
      // expr_type_list and val_list
      if (tv_expr != nullptr) {
        auto column_ref = (expression::TupleValueExpression *)tv_expr;
        std::string col_name(column_ref->GetColumnName());
        LOG_TRACE("Column name: %s", col_name.c_str());
        auto column_id = get->table->GetColumnObject(col_name)->GetColumnId();
        key_column_id_list.push_back(column_id);
        expr_type_list.push_back(expr_type);

        if (value_expr->GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
          value_list.push_back(
              reinterpret_cast<expression::ConstantValueExpression *>(
                  value_expr)
                  ->GetValue());
          LOG_TRACE("Value Type: %d",
                    reinterpret_cast<expression::ConstantValueExpression *>(
                        expr->GetModifiableChild(1))
                        ->GetValueType());
        } else {
          value_list.push_back(
              type::ValueFactory::GetParameterOffsetValue(
                  reinterpret_cast<expression::ParameterValueExpression *>(
                      value_expr)
                      ->GetValueIdx())
                  .Copy());
          LOG_TRACE("Parameter offset: %s",
                    (*values.rbegin()).GetInfo().c_str());
        }
      }
    }  // Loop predicates end

    // Find match index for the predicates
    auto index_objects = get->table->GetIndexObjects();
    for (auto &index_id_object_pair : index_objects) {
      auto &index_id = index_id_object_pair.first;
      auto &index_object = index_id_object_pair.second;
      std::vector<oid_t> index_key_column_id_list;
      std::vector<ExpressionType> index_expr_type_list;
      std::vector<type::Value> index_value_list;
      std::unordered_set<oid_t> index_col_set(
          index_object->GetKeyAttrs().begin(),
          index_object->GetKeyAttrs().end());
      for (size_t offset = 0; offset < key_column_id_list.size(); offset++) {
        auto col_id = key_column_id_list[offset];
        if (index_col_set.find(col_id) != index_col_set.end()) {
          index_key_column_id_list.push_back(col_id);
          index_expr_type_list.push_back(expr_type_list[offset]);
          index_value_list.push_back(value_list[offset]);
        }
      }
      // Add transformed plan
      if (!index_key_column_id_list.empty()) {
        auto index_scan_op = PhysicalIndexScan::make(
            get->get_id, get->table, get->table_alias, get->predicates,
            get->is_for_update, index_id, index_key_column_id_list,
            index_expr_type_list, index_value_list);
        transformed.push_back(
            std::make_shared<OperatorExpression>(index_scan_op));
      }
    }
  }
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalQueryDerivedGetToPhysical
LogicalQueryDerivedGetToPhysical::LogicalQueryDerivedGetToPhysical() {
  type_ = RuleType::QUERY_DERIVED_GET_TO_PHYSICAL;
  match_pattern = std::make_shared<Pattern>(OpType::LogicalQueryDerivedGet);
  std::shared_ptr<Pattern> child(std::make_shared<Pattern>(OpType::Leaf));
  match_pattern->AddChild(child);
}

bool LogicalQueryDerivedGetToPhysical::Check(
    std::shared_ptr<OperatorExpression> expr, OptimizeContext *context) const {
  (void)context;
  (void)expr;
  return true;
}

void LogicalQueryDerivedGetToPhysical::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed,
    UNUSED_ATTRIBUTE OptimizeContext *context) const {
  const LogicalQueryDerivedGet *get = input->Op().As<LogicalQueryDerivedGet>();

  auto result_plan =
      std::make_shared<OperatorExpression>(QueryDerivedScan::make(
          get->get_id, get->table_alias, get->alias_to_expr_map));
  result_plan->PushChild(input->Children().at(0));

  transformed.push_back(result_plan);
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalDeleteToPhysical
LogicalDeleteToPhysical::LogicalDeleteToPhysical() {
  type_ = RuleType::DELETE_TO_PHYSICAL;
  match_pattern = std::make_shared<Pattern>(OpType::LogicalDelete);
  std::shared_ptr<Pattern> child(std::make_shared<Pattern>(OpType::Leaf));
  match_pattern->AddChild(child);
}

bool LogicalDeleteToPhysical::Check(std::shared_ptr<OperatorExpression> plan,
                                    OptimizeContext *context) const {
  (void)plan;
  (void)context;
  return true;
}

void LogicalDeleteToPhysical::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed,
    UNUSED_ATTRIBUTE OptimizeContext *context) const {
  const LogicalDelete *delete_op = input->Op().As<LogicalDelete>();
  auto result = std::make_shared<OperatorExpression>(
      PhysicalDelete::make(delete_op->target_table));
  PL_ASSERT(input->Children().size() == 1);
  result->PushChild(input->Children().at(0));
  transformed.push_back(result);
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalUpdateToPhysical
LogicalUpdateToPhysical::LogicalUpdateToPhysical() {
  type_ = RuleType::UPDATE_TO_PHYSICAL;
  match_pattern = std::make_shared<Pattern>(OpType::LogicalUpdate);
  std::shared_ptr<Pattern> child(std::make_shared<Pattern>(OpType::Leaf));
  match_pattern->AddChild(child);
}

bool LogicalUpdateToPhysical::Check(std::shared_ptr<OperatorExpression> plan,
                                    OptimizeContext *context) const {
  (void)plan;
  (void)context;
  return true;
}

void LogicalUpdateToPhysical::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed,
    UNUSED_ATTRIBUTE OptimizeContext *context) const {
  const LogicalUpdate *update_op = input->Op().As<LogicalUpdate>();
  auto result = std::make_shared<OperatorExpression>(
      PhysicalUpdate::make(update_op->target_table, update_op->updates));
  PL_ASSERT(input->Children().size() != 0);
  result->PushChild(input->Children().at(0));
  transformed.push_back(result);
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalInsertToPhysical
LogicalInsertToPhysical::LogicalInsertToPhysical() {
  type_ = RuleType::INSERT_TO_PHYSICAL;
  match_pattern = std::make_shared<Pattern>(OpType::LogicalInsert);
  //  std::shared_ptr<Pattern> child(std::make_shared<Pattern>(OpType::Leaf));
  //  match_pattern->AddChild(child);
}

bool LogicalInsertToPhysical::Check(std::shared_ptr<OperatorExpression> plan,
                                    OptimizeContext *context) const {
  (void)plan;
  (void)context;
  return true;
}

void LogicalInsertToPhysical::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed,
    UNUSED_ATTRIBUTE OptimizeContext *context) const {
  const LogicalInsert *insert_op = input->Op().As<LogicalInsert>();
  auto result = std::make_shared<OperatorExpression>(PhysicalInsert::make(
      insert_op->target_table, insert_op->columns, insert_op->values));
  PL_ASSERT(input->Children().size() == 0);
  //  result->PushChild(input->Children().at(0));
  transformed.push_back(result);
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalInsertSelectToPhysical
LogicalInsertSelectToPhysical::LogicalInsertSelectToPhysical() {
  type_ = RuleType::INSERT_SELECT_TO_PHYSICAL;
  match_pattern = std::make_shared<Pattern>(OpType::LogicalInsertSelect);
  std::shared_ptr<Pattern> child(std::make_shared<Pattern>(OpType::Leaf));
  match_pattern->AddChild(child);
}

bool LogicalInsertSelectToPhysical::Check(
    std::shared_ptr<OperatorExpression> plan, OptimizeContext *context) const {
  (void)plan;
  (void)context;
  return true;
}

void LogicalInsertSelectToPhysical::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed,
    UNUSED_ATTRIBUTE OptimizeContext *context) const {
  const LogicalInsertSelect *insert_op = input->Op().As<LogicalInsertSelect>();
  auto result = std::make_shared<OperatorExpression>(
      PhysicalInsertSelect::make(insert_op->target_table));
  PL_ASSERT(input->Children().size() == 1);
  result->PushChild(input->Children().at(0));
  transformed.push_back(result);
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalAggregateAndGroupByToHashGroupBy
LogicalGroupByToHashGroupBy::LogicalGroupByToHashGroupBy() {
  type_ = RuleType::AGGREGATE_TO_HASH_AGGREGATE;
  match_pattern = std::make_shared<Pattern>(OpType::LogicalAggregateAndGroupBy);
  std::shared_ptr<Pattern> child(std::make_shared<Pattern>(OpType::Leaf));
  match_pattern->AddChild(child);
}

bool LogicalGroupByToHashGroupBy::Check(
    UNUSED_ATTRIBUTE std::shared_ptr<OperatorExpression> plan,
    OptimizeContext *context) const {
  (void)context;
  const LogicalAggregateAndGroupBy *agg_op =
      plan->Op().As<LogicalAggregateAndGroupBy>();
  return !agg_op->columns.empty();
}

void LogicalGroupByToHashGroupBy::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed,
    UNUSED_ATTRIBUTE OptimizeContext *context) const {
  const LogicalAggregateAndGroupBy *agg_op =
      input->Op().As<LogicalAggregateAndGroupBy>();
  auto result = std::make_shared<OperatorExpression>(
      PhysicalHashGroupBy::make(agg_op->columns, agg_op->having));
  PL_ASSERT(input->Children().size() == 1);
  result->PushChild(input->Children().at(0));
  transformed.push_back(result);
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalAggregateToPhysical
LogicalAggregateToPhysical::LogicalAggregateToPhysical() {
  type_ = RuleType::AGGREGATE_TO_PLAIN_AGGREGATE;
  match_pattern = std::make_shared<Pattern>(OpType::LogicalAggregateAndGroupBy);
  std::shared_ptr<Pattern> child(std::make_shared<Pattern>(OpType::Leaf));
  match_pattern->AddChild(child);
}

bool LogicalAggregateToPhysical::Check(
    UNUSED_ATTRIBUTE std::shared_ptr<OperatorExpression> plan,
    OptimizeContext *context) const {
  (void)context;
  const LogicalAggregateAndGroupBy *agg_op =
      plan->Op().As<LogicalAggregateAndGroupBy>();
  return agg_op->columns.empty();
}

void LogicalAggregateToPhysical::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed,
    UNUSED_ATTRIBUTE OptimizeContext *context) const {
  auto result = std::make_shared<OperatorExpression>(PhysicalAggregate::make());
  PL_ASSERT(input->Children().size() == 1);
  result->PushChild(input->Children().at(0));
  transformed.push_back(result);
}

///////////////////////////////////////////////////////////////////////////////
/// InnerJoinToInnerNLJoin
InnerJoinToInnerNLJoin::InnerJoinToInnerNLJoin() {
  type_ = RuleType::INNER_JOIN_TO_NL_JOIN;

  // TODO NLJoin currently only support left deep tree
  std::shared_ptr<Pattern> left_child(std::make_shared<Pattern>(OpType::Leaf));
  std::shared_ptr<Pattern> right_child(std::make_shared<Pattern>(OpType::Leaf));

  // Initialize a pattern for optimizer to match
  match_pattern = std::make_shared<Pattern>(OpType::InnerJoin);

  // Add node - we match join relation R and S
  match_pattern->AddChild(left_child);
  match_pattern->AddChild(right_child);

  return;
}

bool InnerJoinToInnerNLJoin::Check(std::shared_ptr<OperatorExpression> plan,
                                   OptimizeContext *context) const {
  (void)context;
  (void)plan;
  return true;
}

void InnerJoinToInnerNLJoin::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed,
    UNUSED_ATTRIBUTE OptimizeContext *context) const {
  // first build an expression representing hash join
  const LogicalInnerJoin *inner_join = input->Op().As<LogicalInnerJoin>();

  auto children = input->Children();
  PL_ASSERT(children.size() == 2);
  auto left_group_id = children[0]->Op().As<LeafOperator>()->origin_group;
  auto right_group_id = children[1]->Op().As<LeafOperator>()->origin_group;
  auto &left_group_alias =
      context->metadata->memo.GetGroupByID(left_group_id)->GetTableAliases();
  auto &right_group_alias =
      context->metadata->memo.GetGroupByID(right_group_id)->GetTableAliases();
  std::vector<std::unique_ptr<expression::AbstractExpression>> left_keys;
  std::vector<std::unique_ptr<expression::AbstractExpression>> right_keys;

  util::ExtractEquiJoinKeys(inner_join->join_predicates, left_keys, right_keys,
                            left_group_alias, right_group_alias);

  PL_ASSERT(right_keys.size() == left_keys.size());
  auto result_plan =
      std::make_shared<OperatorExpression>(PhysicalInnerNLJoin::make(
          inner_join->join_predicates, left_keys, right_keys));

  // Then push all children into the child list of the new operator
  result_plan->PushChild(children[0]);
  result_plan->PushChild(children[1]);

  transformed.push_back(result_plan);

  return;
}

///////////////////////////////////////////////////////////////////////////////
/// InnerJoinToInnerHashJoin
InnerJoinToInnerHashJoin::InnerJoinToInnerHashJoin() {
  type_ = RuleType::INNER_JOIN_TO_HASH_JOIN;

  // Make three node types for pattern matching
  std::shared_ptr<Pattern> left_child(std::make_shared<Pattern>(OpType::Leaf));
  std::shared_ptr<Pattern> right_child(std::make_shared<Pattern>(OpType::Leaf));

  // Initialize a pattern for optimizer to match
  match_pattern = std::make_shared<Pattern>(OpType::InnerJoin);

  // Add node - we match join relation R and S as well as the predicate exp
  match_pattern->AddChild(left_child);
  match_pattern->AddChild(right_child);

  return;
}

bool InnerJoinToInnerHashJoin::Check(std::shared_ptr<OperatorExpression> plan,
                                     OptimizeContext *context) const {
  (void)context;
  (void)plan;
  return true;
}

void InnerJoinToInnerHashJoin::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed,
    UNUSED_ATTRIBUTE OptimizeContext *context) const {
  // first build an expression representing hash join
  const LogicalInnerJoin *inner_join = input->Op().As<LogicalInnerJoin>();

  auto children = input->Children();
  PL_ASSERT(children.size() == 2);
  auto left_group_id = children[0]->Op().As<LeafOperator>()->origin_group;
  auto right_group_id = children[1]->Op().As<LeafOperator>()->origin_group;
  auto &left_group_alias =
      context->metadata->memo.GetGroupByID(left_group_id)->GetTableAliases();
  auto &right_group_alias =
      context->metadata->memo.GetGroupByID(right_group_id)->GetTableAliases();
  std::vector<std::unique_ptr<expression::AbstractExpression>> left_keys;
  std::vector<std::unique_ptr<expression::AbstractExpression>> right_keys;

  util::ExtractEquiJoinKeys(inner_join->join_predicates, left_keys, right_keys,
                            left_group_alias, right_group_alias);

  PL_ASSERT(right_keys.size() == left_keys.size());
  if (!left_keys.empty()) {
    auto result_plan =
        std::make_shared<OperatorExpression>(PhysicalInnerHashJoin::make(
            inner_join->join_predicates, left_keys, right_keys));

    // Then push all children into the child list of the new operator
    result_plan->PushChild(children[0]);
    result_plan->PushChild(children[1]);

    transformed.push_back(result_plan);
  }
}

///////////////////////////////////////////////////////////////////////////////
/// ImplementDistinct
ImplementDistinct::ImplementDistinct() {
  type_ = RuleType::IMPLEMENT_DISTINCT;

  match_pattern = std::make_shared<Pattern>(OpType::LogicalDistinct);
  match_pattern->AddChild(std::make_shared<Pattern>(OpType::Leaf));
}

bool ImplementDistinct::Check(std::shared_ptr<OperatorExpression> plan,
                              OptimizeContext *context) const {
  (void)context;
  (void)plan;
  return true;
}

void ImplementDistinct::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed,
    OptimizeContext *context) const {
  (void)context;
  auto result_plan =
      std::make_shared<OperatorExpression>(PhysicalDistinct::make());
  std::vector<std::shared_ptr<OperatorExpression>> children = input->Children();
  PL_ASSERT(children.size() == 1);

  result_plan->PushChild(children[0]);

  transformed.push_back(result_plan);
}

///////////////////////////////////////////////////////////////////////////////
/// ImplementLimit
ImplementLimit::ImplementLimit() {
  type_ = RuleType::IMPLEMENT_LIMIT;

  match_pattern = std::make_shared<Pattern>(OpType::LogicalLimit);
  match_pattern->AddChild(std::make_shared<Pattern>(OpType::Leaf));
}

bool ImplementLimit::Check(std::shared_ptr<OperatorExpression> plan,
                           OptimizeContext *context) const {
  (void)context;
  (void)plan;
  return true;
}

void ImplementLimit::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed,
    OptimizeContext *context) const {
  (void)context;
  const LogicalLimit *limit_op = input->Op().As<LogicalLimit>();

  auto result_plan = std::make_shared<OperatorExpression>(
      PhysicalLimit::make(limit_op->offset, limit_op->limit));
  std::vector<std::shared_ptr<OperatorExpression>> children = input->Children();
  PL_ASSERT(children.size() == 1);

  result_plan->PushChild(children[0]);

  transformed.push_back(result_plan);
}

//===--------------------------------------------------------------------===//
// Rewrite rules
//===--------------------------------------------------------------------===//

///////////////////////////////////////////////////////////////////////////////
/// PushFilterThroughJoin
PushFilterThroughJoin::PushFilterThroughJoin() {
  type_ = RuleType::PUSH_FILTER_THROUGH_JOIN;

  // Make three node types for pattern matching
  std::shared_ptr<Pattern> child(std::make_shared<Pattern>(OpType::InnerJoin));
  child->AddChild(std::make_shared<Pattern>(OpType::Leaf));
  child->AddChild(std::make_shared<Pattern>(OpType::Leaf));

  // Initialize a pattern for optimizer to match
  match_pattern = std::make_shared<Pattern>(OpType::LogicalFilter);

  // Add node - we match join relation R and S as well as the predicate exp
  match_pattern->AddChild(child);
}

bool PushFilterThroughJoin::Check(std::shared_ptr<OperatorExpression>,
                                  OptimizeContext *) const {
  return true;
}

void PushFilterThroughJoin::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed,
    UNUSED_ATTRIBUTE OptimizeContext *context) const {
  auto &memo = context->metadata->memo;
  auto join_op_expr = input->Children().at(0);
  auto &join_children = join_op_expr->Children();
  auto left_group_id = join_children[0]->Op().As<LeafOperator>()->origin_group;
  auto right_group_id = join_children[1]->Op().As<LeafOperator>()->origin_group;
  const auto &left_group_aliases_set =
      memo.GetGroupByID(left_group_id)->GetTableAliases();
  const auto &right_group_aliases_set =
      memo.GetGroupByID(right_group_id)->GetTableAliases();

  auto &predicates = input->Op().As<LogicalFilter>()->predicates;
  std::vector<AnnotatedExpression> left_predicates;
  std::vector<AnnotatedExpression> right_predicates;
  std::vector<AnnotatedExpression> join_predicates;

  // Loop over all predicates, check each of them if they can be pushed down to
  // either the left child or the right child to be evaluated
  // All predicates in this loop follow conjunction relationship because we
  // already extract these predicates from the original.
  // E.g. An expression (test.a = test1.b and test.a = 5) would become
  // {test.a = test1.b, test.a = 5}
  for (auto &predicate : predicates) {
    if (util::IsSubset(left_group_aliases_set, predicate.table_alias_set))
      left_predicates.emplace_back(predicate);
    else if (util::IsSubset(right_group_aliases_set, predicate.table_alias_set))
      right_predicates.emplace_back(predicate);
    else
      join_predicates.emplace_back(predicate);
  }

  // Construct join operator
  auto pre_join_predicate =
      join_op_expr->Op().As<LogicalInnerJoin>()->join_predicates;
  join_predicates.insert(join_predicates.end(), pre_join_predicate.begin(),
                         pre_join_predicate.end());
  std::shared_ptr<OperatorExpression> output =
      std::make_shared<OperatorExpression>(
          LogicalInnerJoin::make(join_predicates));

  // Construct left filter if any
  if (!left_predicates.empty()) {
    auto left_filter = std::make_shared<OperatorExpression>(
        LogicalFilter::make(left_predicates));
    left_filter->PushChild(join_op_expr->Children()[0]);
    output->PushChild(left_filter);
  } else {
    output->PushChild(join_op_expr->Children()[0]);
  }

  // Construct left filter if any
  if (!right_predicates.empty()) {
    auto right_filter = std::make_shared<OperatorExpression>(
        LogicalFilter::make(right_predicates));
    right_filter->PushChild(join_op_expr->Children()[1]);
    output->PushChild(right_filter);
  } else {
    output->PushChild(join_op_expr->Children()[1]);
  }

  PL_ASSERT(output->Children().size() == 2);

  transformed.push_back(output);
}

///////////////////////////////////////////////////////////////////////////////
/// CombineConsecutiveFilter
CombineConsecutiveFilter::CombineConsecutiveFilter() {
  type_ = RuleType::COMBINE_CONSECUTIVE_FILTER;

  match_pattern = std::make_shared<Pattern>(OpType::LogicalFilter);
  std::shared_ptr<Pattern> child(
      std::make_shared<Pattern>(OpType::LogicalFilter));
  child->AddChild(std::make_shared<Pattern>(OpType::Leaf));

  match_pattern->AddChild(child);
}

bool CombineConsecutiveFilter::Check(std::shared_ptr<OperatorExpression> plan,
                                     OptimizeContext *context) const {
  (void)context;
  (void)plan;

  auto &children = plan->Children();
  (void)children;
  PL_ASSERT(children.size() == 1);
  auto &filter = children.at(0);
  (void)filter;
  PL_ASSERT(filter->Children().size() == 1);

  return true;
}

void CombineConsecutiveFilter::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed,
    UNUSED_ATTRIBUTE OptimizeContext *context) const {
  auto child_filter = input->Children()[0];

  auto root_predicates = input->Op().As<LogicalFilter>()->predicates;
  auto &child_predicates = child_filter->Op().As<LogicalFilter>()->predicates;

  root_predicates.insert(root_predicates.end(), child_predicates.begin(),
                         child_predicates.end());

  std::shared_ptr<OperatorExpression> output =
      std::make_shared<OperatorExpression>(
          LogicalFilter::make(root_predicates));

  output->PushChild(child_filter->Children()[0]);

  transformed.push_back(output);
}

///////////////////////////////////////////////////////////////////////////////
/// EmbedFilterIntoGet
EmbedFilterIntoGet::EmbedFilterIntoGet() {
  type_ = RuleType::EMBED_FILTER_INTO_GET;

  match_pattern = std::make_shared<Pattern>(OpType::LogicalFilter);
  std::shared_ptr<Pattern> child(std::make_shared<Pattern>(OpType::Get));

  match_pattern->AddChild(child);
}

bool EmbedFilterIntoGet::Check(std::shared_ptr<OperatorExpression> plan,
                               OptimizeContext *context) const {
  (void)context;
  (void)plan;
  return true;
}

void EmbedFilterIntoGet::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed,
    UNUSED_ATTRIBUTE OptimizeContext *context) const {
  auto get = input->Children()[0]->Op().As<LogicalGet>();

  auto predicates = input->Op().As<LogicalFilter>()->predicates;

  std::shared_ptr<OperatorExpression> output =
      std::make_shared<OperatorExpression>(
          LogicalGet::make(get->get_id, predicates, get->table,
                           get->table_alias, get->is_for_update));

  transformed.push_back(output);
}

///////////////////////////////////////////////////////////////////////////////
/// MarkJoinGetToInnerJoin
MarkJoinGetToInnerJoin::MarkJoinGetToInnerJoin() {
  type_ = RuleType::MARK_JOIN_GET_TO_INNER_JOIN;

  match_pattern = std::make_shared<Pattern>(OpType::LogicalMarkJoin);
  match_pattern->AddChild(std::make_shared<Pattern>(OpType::Leaf));
  match_pattern->AddChild(std::make_shared<Pattern>(OpType::Get));
}

bool MarkJoinGetToInnerJoin::Check(std::shared_ptr<OperatorExpression> plan,
                                   OptimizeContext *context) const {
  (void)context;
  (void)plan;

  UNUSED_ATTRIBUTE auto &children = plan->Children();
  PL_ASSERT(children.size() == 2);

  return true;
}

void MarkJoinGetToInnerJoin::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed,
    UNUSED_ATTRIBUTE OptimizeContext *context) const {
  UNUSED_ATTRIBUTE auto mark_join = input->Op().As<LogicalMarkJoin>();
  auto &join_children = input->Children();

  PL_ASSERT(mark_join->join_predicates.empty());

  std::shared_ptr<OperatorExpression> output =
      std::make_shared<OperatorExpression>(LogicalInnerJoin::make());

  output->PushChild(join_children[0]);
  output->PushChild(join_children[1]);

  transformed.push_back(output);
}

///////////////////////////////////////////////////////////////////////////////
/// MarkJoinInnerJoinToInnerJoin
MarkJoinInnerJoinToInnerJoin::MarkJoinInnerJoinToInnerJoin() {
  type_ = RuleType::MARK_JOIN_INNER_JOIN_TO_INNER_JOIN;

  match_pattern = std::make_shared<Pattern>(OpType::LogicalMarkJoin);
  match_pattern->AddChild(std::make_shared<Pattern>(OpType::Leaf));
  auto inner_join = std::make_shared<Pattern>(OpType::InnerJoin);
  inner_join->AddChild(std::make_shared<Pattern>(OpType::Leaf));
  inner_join->AddChild(std::make_shared<Pattern>(OpType::Leaf));
  match_pattern->AddChild(inner_join);
}

bool MarkJoinInnerJoinToInnerJoin::Check(
    std::shared_ptr<OperatorExpression> plan, OptimizeContext *context) const {
  (void)context;
  (void)plan;

  UNUSED_ATTRIBUTE auto &children = plan->Children();
  PL_ASSERT(children.size() == 2);

  return true;
}

void MarkJoinInnerJoinToInnerJoin::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed,
    UNUSED_ATTRIBUTE OptimizeContext *context) const {
  UNUSED_ATTRIBUTE auto mark_join = input->Op().As<LogicalMarkJoin>();
  auto &join_children = input->Children();

  PL_ASSERT(mark_join->join_predicates.empty());

  std::shared_ptr<OperatorExpression> output =
      std::make_shared<OperatorExpression>(LogicalInnerJoin::make());

  output->PushChild(join_children[0]);
  output->PushChild(join_children[1]);

  transformed.push_back(output);
}

///////////////////////////////////////////////////////////////////////////////
/// PullFilterThroughMarkJoin
PullFilterThroughMarkJoin::PullFilterThroughMarkJoin() {
  type_ = RuleType::PULL_FILTER_THROUGH_MARK_JOIN;

  match_pattern = std::make_shared<Pattern>(OpType::LogicalMarkJoin);
  match_pattern->AddChild(std::make_shared<Pattern>(OpType::Leaf));
  auto filter = std::make_shared<Pattern>(OpType::LogicalFilter);
  filter->AddChild(std::make_shared<Pattern>(OpType::Leaf));
  match_pattern->AddChild(filter);
}

bool PullFilterThroughMarkJoin::Check(std::shared_ptr<OperatorExpression> plan,
                                      OptimizeContext *context) const {
  (void)context;
  (void)plan;

  auto &children = plan->Children();
  PL_ASSERT(children.size() == 2);
  UNUSED_ATTRIBUTE auto &r_grandchildren = children[1]->Children();
  PL_ASSERT(r_grandchildren.size() == 1);

  return true;
}

void PullFilterThroughMarkJoin::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed,
    UNUSED_ATTRIBUTE OptimizeContext *context) const {
  UNUSED_ATTRIBUTE auto mark_join = input->Op().As<LogicalMarkJoin>();
  auto &join_children = input->Children();
  auto filter = join_children[1]->Op();
  auto &filter_children = join_children[1]->Children();

  PL_ASSERT(mark_join->join_predicates.empty());

  std::shared_ptr<OperatorExpression> output =
      std::make_shared<OperatorExpression>(filter);

  std::shared_ptr<OperatorExpression> join =
      std::make_shared<OperatorExpression>(input->Op());

  join->PushChild(join_children[0]);
  join->PushChild(filter_children[0]);

  output->PushChild(join);

  transformed.push_back(output);
}

}  // namespace optimizer
}  // namespace peloton
