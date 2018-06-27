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

#include "catalog/column_catalog.h"
#include "catalog/index_catalog.h"
#include "catalog/table_catalog.h"
#include "optimizer/operators.h"
#include "optimizer/optimizer_metadata.h"
#include "optimizer/properties.h"
#include "optimizer/rule_impls.h"
#include "optimizer/util.h"
#include "storage/data_table.h"

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
  PELOTON_ASSERT(children.size() == 2);
  LOG_TRACE(
      "Reorder left child with op %s and right child with op %s for inner join",
      children[0]->Op().GetName().c_str(), children[1]->Op().GetName().c_str());
  result_plan->PushChild(children[1]);
  result_plan->PushChild(children[0]);

  transformed.push_back(result_plan);
}

///////////////////////////////////////////////////////////////////////////////
/// InnerJoinAssociativity
InnerJoinAssociativity::InnerJoinAssociativity() {
  type_ = RuleType::INNER_JOIN_ASSOCIATE;

  // Create left nested join
  auto left_child = std::make_shared<Pattern>(OpType::InnerJoin);
  left_child->AddChild(std::make_shared<Pattern>(OpType::Leaf));
  left_child->AddChild(std::make_shared<Pattern>(OpType::Leaf));

  std::shared_ptr<Pattern> right_child(std::make_shared<Pattern>(OpType::Leaf));

  match_pattern = std::make_shared<Pattern>(OpType::InnerJoin);
  match_pattern->AddChild(left_child);
  match_pattern->AddChild(right_child);
}

// TODO: As far as I know, theres nothing else that needs to be checked
bool InnerJoinAssociativity::Check(std::shared_ptr<OperatorExpression> expr,
                                   OptimizeContext *context) const {
  (void)context;
  (void)expr;
  return true;
}

void InnerJoinAssociativity::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed,
    OptimizeContext *context) const {
  // NOTE: Transforms (left JOIN middle) JOIN right -> left JOIN (middle JOIN
  // right) Variables are named accordingly to above transformation
  auto parent_join = input->Op().As<LogicalInnerJoin>();
  std::vector<std::shared_ptr<OperatorExpression>> children = input->Children();
  PELOTON_ASSERT(children.size() == 2);
  PELOTON_ASSERT(children[0]->Op().GetType() == OpType::InnerJoin);
  PELOTON_ASSERT(children[0]->Children().size() == 2);
  auto child_join = children[0]->Op().As<LogicalInnerJoin>();
  auto left = children[0]->Children()[0];
  auto middle = children[0]->Children()[1];
  auto right = children[1];

  LOG_DEBUG("Reordered join structured: (%s JOIN %s) JOIN %s",
            left->Op().GetName().c_str(), middle->Op().GetName().c_str(),
            right->Op().GetName().c_str());

  // Get Alias sets
  auto &memo = context->metadata->memo;
  auto middle_group_id = middle->Op().As<LeafOperator>()->origin_group;
  auto right_group_id = right->Op().As<LeafOperator>()->origin_group;

  const auto &middle_group_aliases_set =
      memo.GetGroupByID(middle_group_id)->GetTableAliases();
  const auto &right_group_aliases_set =
      memo.GetGroupByID(right_group_id)->GetTableAliases();

  // Union Predicates into single alias set for new child join
  std::unordered_set<std::string> right_join_aliases_set;
  right_join_aliases_set.insert(middle_group_aliases_set.begin(),
                                middle_group_aliases_set.end());
  right_join_aliases_set.insert(right_group_aliases_set.begin(),
                                right_group_aliases_set.end());

  // Redistribute predicates
  auto parent_join_predicates =
      std::vector<AnnotatedExpression>(parent_join->join_predicates);
  auto child_join_predicates =
      std::vector<AnnotatedExpression>(child_join->join_predicates);

  std::vector<AnnotatedExpression> predicates;
  predicates.insert(predicates.end(), parent_join_predicates.begin(),
                    parent_join_predicates.end());
  predicates.insert(predicates.end(), child_join_predicates.begin(),
                    child_join_predicates.end());

  std::vector<AnnotatedExpression> new_child_join_predicates;
  std::vector<AnnotatedExpression> new_parent_join_predicates;

  for (auto predicate : predicates) {
    if (util::IsSubset(right_join_aliases_set, predicate.table_alias_set)) {
      new_child_join_predicates.emplace_back(predicate);
    } else {
      new_parent_join_predicates.emplace_back(predicate);
    }
  }

  // Construct new child join operator
  std::shared_ptr<OperatorExpression> new_child_join =
      std::make_shared<OperatorExpression>(
          LogicalInnerJoin::make(new_child_join_predicates));
  new_child_join->PushChild(middle);
  new_child_join->PushChild(right);

  // Construct new parent join operator
  std::shared_ptr<OperatorExpression> new_parent_join =
      std::make_shared<OperatorExpression>(
          LogicalInnerJoin::make(new_parent_join_predicates));
  new_parent_join->PushChild(left);
  new_parent_join->PushChild(new_child_join);

  transformed.push_back(new_parent_join);
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
  PELOTON_ASSERT(children.size() == 0);

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
      !get->table->GetIndexCatalogEntries().empty()) {
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
  PELOTON_ASSERT(children.size() == 0);

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
      auto bound_oids = reinterpret_cast<expression::TupleValueExpression *>(
                            expr)->GetBoundOid();
      sort_col_ids.push_back(std::get<2>(bound_oids));
    }
    // Check whether any index can fulfill sort property
    if (sort_by_asc_base_column) {
      for (auto &index_id_object_pair : get->table->GetIndexCatalogEntries()) {
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
        auto column_id = get->table->GetColumnCatalogEntry(col_name)->GetColumnId();
        key_column_id_list.push_back(column_id);
        expr_type_list.push_back(expr_type);

        if (value_expr->GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
          value_list.push_back(
              reinterpret_cast<expression::ConstantValueExpression *>(
                  value_expr)->GetValue());
          LOG_TRACE("Value Type: %d",
                    static_cast<int>(
                        reinterpret_cast<expression::ConstantValueExpression *>(
                            expr->GetModifiableChild(1))->GetValueType()));
        } else {
          value_list.push_back(
              type::ValueFactory::GetParameterOffsetValue(
                  reinterpret_cast<expression::ParameterValueExpression *>(
                      value_expr)->GetValueIdx()).Copy());
          LOG_TRACE("Parameter offset: %s",
                    (*value_list.rbegin()).GetInfo().c_str());
        }
      }
    }  // Loop predicates end

    // Find match index for the predicates
    auto index_objects = get->table->GetIndexCatalogEntries();
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
/// LogicalExternalFileGetToPhysical
LogicalExternalFileGetToPhysical::LogicalExternalFileGetToPhysical() {
  type_ = RuleType::EXTERNAL_FILE_GET_TO_PHYSICAL;
  match_pattern = std::make_shared<Pattern>(OpType::LogicalExternalFileGet);
}

bool LogicalExternalFileGetToPhysical::Check(
    UNUSED_ATTRIBUTE std::shared_ptr<OperatorExpression> plan,
    UNUSED_ATTRIBUTE OptimizeContext *context) const {
  return true;
}

void LogicalExternalFileGetToPhysical::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed,
    UNUSED_ATTRIBUTE OptimizeContext *context) const {
  const auto *get = input->Op().As<LogicalExternalFileGet>();

  auto result_plan = std::make_shared<OperatorExpression>(
      ExternalFileScan::make(get->get_id, get->format, get->file_name,
                             get->delimiter, get->quote, get->escape));

  PELOTON_ASSERT(input->Children().empty());

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
  PELOTON_ASSERT(input->Children().size() == 1);
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
  PELOTON_ASSERT(input->Children().size() != 0);
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
  PELOTON_ASSERT(input->Children().size() == 0);
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
  PELOTON_ASSERT(input->Children().size() == 1);
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
  PELOTON_ASSERT(input->Children().size() == 1);
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
  PELOTON_ASSERT(input->Children().size() == 1);
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
  PELOTON_ASSERT(children.size() == 2);
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

  PELOTON_ASSERT(right_keys.size() == left_keys.size());
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
  PELOTON_ASSERT(children.size() == 2);
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

  PELOTON_ASSERT(right_keys.size() == left_keys.size());
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
  PELOTON_ASSERT(children.size() == 1);

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
      PhysicalLimit::make(limit_op->offset, limit_op->limit,
                          limit_op->sort_exprs, limit_op->sort_ascending));
  std::vector<std::shared_ptr<OperatorExpression>> children = input->Children();
  PELOTON_ASSERT(children.size() == 1);

  result_plan->PushChild(children[0]);

  transformed.push_back(result_plan);
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalExport to Physical Export
LogicalExportToPhysicalExport::LogicalExportToPhysicalExport() {
  type_ = RuleType::EXPORT_EXTERNAL_FILE_TO_PHYSICAL;
  match_pattern = std::make_shared<Pattern>(OpType::LogicalExportExternalFile);
  match_pattern->AddChild(std::make_shared<Pattern>(OpType::Leaf));
}

bool LogicalExportToPhysicalExport::Check(
    UNUSED_ATTRIBUTE std::shared_ptr<OperatorExpression> plan,
    UNUSED_ATTRIBUTE OptimizeContext *context) const {
  return true;
}

void LogicalExportToPhysicalExport::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed,
    UNUSED_ATTRIBUTE OptimizeContext *context) const {
  const auto *export_op = input->Op().As<LogicalExportExternalFile>();

  auto result_plan =
      std::make_shared<OperatorExpression>(PhysicalExportExternalFile::make(
          export_op->format, export_op->file_name, export_op->delimiter,
          export_op->quote, export_op->escape));

  std::vector<std::shared_ptr<OperatorExpression>> children = input->Children();
  PELOTON_ASSERT(children.size() == 1);
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
  LOG_TRACE("PushFilterThroughJoin::Transform");
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
    if (util::IsSubset(left_group_aliases_set, predicate.table_alias_set)) {
      left_predicates.emplace_back(predicate);
    } else if (util::IsSubset(right_group_aliases_set,
                              predicate.table_alias_set)) {
      right_predicates.emplace_back(predicate);
    } else {
      join_predicates.emplace_back(predicate);
    }
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

  PELOTON_ASSERT(output->Children().size() == 2);

  transformed.push_back(output);
}

///////////////////////////////////////////////////////////////////////////////
/// PushFilterThroughAggregation
PushFilterThroughAggregation::PushFilterThroughAggregation() {
  type_ = RuleType::PUSH_FILTER_THROUGH_JOIN;

  std::shared_ptr<Pattern> child(
      std::make_shared<Pattern>(OpType::LogicalAggregateAndGroupBy));
  child->AddChild(std::make_shared<Pattern>(OpType::Leaf));

  // Initialize a pattern for optimizer to match
  match_pattern = std::make_shared<Pattern>(OpType::LogicalFilter);

  // Add node - we match (filter)->(aggregation)->(leaf)
  match_pattern->AddChild(child);
}

bool PushFilterThroughAggregation::Check(std::shared_ptr<OperatorExpression>,
                                         OptimizeContext *) const {
  return true;
}

void PushFilterThroughAggregation::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed,
    UNUSED_ATTRIBUTE OptimizeContext *context) const {
  LOG_TRACE("PushFilterThroughAggregation::Transform");
  auto aggregation_op =
      input->Children().at(0)->Op().As<LogicalAggregateAndGroupBy>();

  auto &predicates = input->Op().As<LogicalFilter>()->predicates;
  std::vector<AnnotatedExpression> embedded_predicates;
  std::vector<AnnotatedExpression> pushdown_predicates;

  for (auto &predicate : predicates) {
    std::vector<expression::AggregateExpression *> aggr_exprs;
    expression::ExpressionUtil::GetAggregateExprs(aggr_exprs,
                                                  predicate.expr.get());
    // No aggr_expr in the predicate -- pushdown to evaluate
    if (aggr_exprs.empty()) {
      pushdown_predicates.emplace_back(predicate);
    } else {
      embedded_predicates.emplace_back(predicate);
    }
  }

  // Add original having predicates
  for (auto &predicate : aggregation_op->having) {
    embedded_predicates.emplace_back(predicate);
  }
  auto groupby_cols = aggregation_op->columns;
  std::shared_ptr<OperatorExpression> output =
      std::make_shared<OperatorExpression>(
          LogicalAggregateAndGroupBy::make(groupby_cols, embedded_predicates));

  auto bottom_operator = output;
  // Construct left filter if any
  if (!pushdown_predicates.empty()) {
    auto filter = std::make_shared<OperatorExpression>(
        LogicalFilter::make(pushdown_predicates));
    output->PushChild(filter);
    bottom_operator = filter;
  }

  // Add leaf
  bottom_operator->PushChild(input->Children()[0]->Children()[0]);
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
  PELOTON_ASSERT(children.size() == 1);
  auto &filter = children.at(0);
  (void)filter;
  PELOTON_ASSERT(filter->Children().size() == 1);

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
/// MarkJoinToInnerJoin
MarkJoinToInnerJoin::MarkJoinToInnerJoin() {
  type_ = RuleType::MARK_JOIN_GET_TO_INNER_JOIN;

  match_pattern = std::make_shared<Pattern>(OpType::LogicalMarkJoin);
  match_pattern->AddChild(std::make_shared<Pattern>(OpType::Leaf));
  match_pattern->AddChild(std::make_shared<Pattern>(OpType::Leaf));
}

int MarkJoinToInnerJoin::Promise(GroupExpression *group_expr,
                                 OptimizeContext *context) const {
  (void)context;
  auto root_type = match_pattern->Type();
  // This rule is not applicable
  if (root_type != OpType::Leaf && root_type != group_expr->Op().GetType()) {
    return 0;
  }
  return static_cast<int>(UnnestPromise::Low);
}

bool MarkJoinToInnerJoin::Check(std::shared_ptr<OperatorExpression> plan,
                                OptimizeContext *context) const {
  (void)context;
  (void)plan;

  UNUSED_ATTRIBUTE auto &children = plan->Children();
  PELOTON_ASSERT(children.size() == 2);

  return true;
}

void MarkJoinToInnerJoin::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed,
    UNUSED_ATTRIBUTE OptimizeContext *context) const {
  LOG_TRACE("MarkJoinToInnerJoin::Transform");
  UNUSED_ATTRIBUTE auto mark_join = input->Op().As<LogicalMarkJoin>();
  auto &join_children = input->Children();

  PELOTON_ASSERT(mark_join->join_predicates.empty());

  std::shared_ptr<OperatorExpression> output =
      std::make_shared<OperatorExpression>(LogicalInnerJoin::make());

  output->PushChild(join_children[0]);
  output->PushChild(join_children[1]);

  transformed.push_back(output);
}

///////////////////////////////////////////////////////////////////////////////
/// SingleJoinGetToInnerJoin
SingleJoinToInnerJoin::SingleJoinToInnerJoin() {
  type_ = RuleType::MARK_JOIN_GET_TO_INNER_JOIN;

  match_pattern = std::make_shared<Pattern>(OpType::LogicalSingleJoin);
  match_pattern->AddChild(std::make_shared<Pattern>(OpType::Leaf));
  match_pattern->AddChild(std::make_shared<Pattern>(OpType::Leaf));
}

int SingleJoinToInnerJoin::Promise(GroupExpression *group_expr,
                                   OptimizeContext *context) const {
  (void)context;
  auto root_type = match_pattern->Type();
  // This rule is not applicable
  if (root_type != OpType::Leaf && root_type != group_expr->Op().GetType()) {
    return 0;
  }
  return static_cast<int>(UnnestPromise::Low);
}

bool SingleJoinToInnerJoin::Check(std::shared_ptr<OperatorExpression> plan,
                                  OptimizeContext *context) const {
  (void)context;
  (void)plan;

  UNUSED_ATTRIBUTE auto &children = plan->Children();
  PELOTON_ASSERT(children.size() == 2);

  return true;
}

void SingleJoinToInnerJoin::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed,
    UNUSED_ATTRIBUTE OptimizeContext *context) const {
  LOG_TRACE("SingleJoinToInnerJoin::Transform");
  UNUSED_ATTRIBUTE auto single_join = input->Op().As<LogicalSingleJoin>();
  auto &join_children = input->Children();

  PELOTON_ASSERT(single_join->join_predicates.empty());

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

int PullFilterThroughMarkJoin::Promise(GroupExpression *group_expr,
                                       OptimizeContext *context) const {
  (void)context;
  auto root_type = match_pattern->Type();
  // This rule is not applicable
  if (root_type != OpType::Leaf && root_type != group_expr->Op().GetType()) {
    return 0;
  }
  return static_cast<int>(UnnestPromise::High);
}

bool PullFilterThroughMarkJoin::Check(std::shared_ptr<OperatorExpression> plan,
                                      OptimizeContext *context) const {
  (void)context;
  (void)plan;

  auto &children = plan->Children();
  PELOTON_ASSERT(children.size() == 2);
  UNUSED_ATTRIBUTE auto &r_grandchildren = children[1]->Children();
  PELOTON_ASSERT(r_grandchildren.size() == 1);

  return true;
}

void PullFilterThroughMarkJoin::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed,
    UNUSED_ATTRIBUTE OptimizeContext *context) const {
  LOG_TRACE("PullFilterThroughMarkJoin::Transform");
  UNUSED_ATTRIBUTE auto mark_join = input->Op().As<LogicalMarkJoin>();
  auto &join_children = input->Children();
  auto filter = join_children[1]->Op();
  auto &filter_children = join_children[1]->Children();

  PELOTON_ASSERT(mark_join->join_predicates.empty());

  std::shared_ptr<OperatorExpression> output =
      std::make_shared<OperatorExpression>(filter);

  std::shared_ptr<OperatorExpression> join =
      std::make_shared<OperatorExpression>(input->Op());

  join->PushChild(join_children[0]);
  join->PushChild(filter_children[0]);

  output->PushChild(join);

  transformed.push_back(output);
}

///////////////////////////////////////////////////////////////////////////////
/// PullFilterThroughAggregation
PullFilterThroughAggregation::PullFilterThroughAggregation() {
  type_ = RuleType::PULL_FILTER_THROUGH_AGGREGATION;

  auto filter = std::make_shared<Pattern>(OpType::LogicalFilter);
  filter->AddChild(std::make_shared<Pattern>(OpType::Leaf));
  match_pattern = std::make_shared<Pattern>(OpType::LogicalAggregateAndGroupBy);
  match_pattern->AddChild(filter);
}

int PullFilterThroughAggregation::Promise(GroupExpression *group_expr,
                                          OptimizeContext *context) const {
  (void)context;
  auto root_type = match_pattern->Type();
  // This rule is not applicable
  if (root_type != OpType::Leaf && root_type != group_expr->Op().GetType()) {
    return 0;
  }
  return static_cast<int>(UnnestPromise::High);
}

bool PullFilterThroughAggregation::Check(
    std::shared_ptr<OperatorExpression> plan, OptimizeContext *context) const {
  (void)context;
  (void)plan;

  auto &children = plan->Children();
  PELOTON_ASSERT(children.size() == 1);
  UNUSED_ATTRIBUTE auto &r_grandchildren = children[1]->Children();
  PELOTON_ASSERT(r_grandchildren.size() == 1);

  return true;
}

void PullFilterThroughAggregation::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed,
    UNUSED_ATTRIBUTE OptimizeContext *context) const {
  LOG_TRACE("PullFilterThroughAggregation::Transform");
  auto &memo = context->metadata->memo;
  auto &filter_expr = input->Children()[0];
  auto child_group_id =
      filter_expr->Children()[0]->Op().As<LeafOperator>()->origin_group;
  const auto &child_group_aliases_set =
      memo.GetGroupByID(child_group_id)->GetTableAliases();

  auto &predicates = filter_expr->Op().As<LogicalFilter>()->predicates;

  std::vector<AnnotatedExpression> correlated_predicates;
  std::vector<AnnotatedExpression> normal_predicates;
  std::vector<std::shared_ptr<expression::AbstractExpression>> new_groupby_cols;
  for (auto &predicate : predicates) {
    if (util::IsSubset(child_group_aliases_set, predicate.table_alias_set)) {
      normal_predicates.emplace_back(predicate);
    } else {
      // Correlated predicate, already in the form of
      // (outer_relation.a = (expr))
      correlated_predicates.emplace_back(predicate);
      auto &root_expr = predicate.expr;
      if (root_expr->GetChild(0)->GetDepth() < root_expr->GetDepth()) {
        new_groupby_cols.emplace_back(root_expr->GetChild(1)->Copy());
      } else {
        new_groupby_cols.emplace_back(root_expr->GetChild(0)->Copy());
      }
    }
  }

  if (correlated_predicates.empty()) {
    // No need to pull
    return;
  }

  auto aggregation = input->Op().As<LogicalAggregateAndGroupBy>();
  for (auto &col : aggregation->columns) {
    new_groupby_cols.emplace_back(col->Copy());
  }
  std::shared_ptr<OperatorExpression> output =
      std::make_shared<OperatorExpression>(
          LogicalFilter::make(correlated_predicates));
  std::vector<AnnotatedExpression> new_having(aggregation->having);
  std::shared_ptr<OperatorExpression> new_aggregation =
      std::make_shared<OperatorExpression>(
          LogicalAggregateAndGroupBy::make(new_groupby_cols, new_having));
  output->PushChild(new_aggregation);
  auto bottom_operator = new_aggregation;

  // Construct child filter if any
  if (!normal_predicates.empty()) {
    std::shared_ptr<OperatorExpression> new_filter =
        std::make_shared<OperatorExpression>(
            LogicalFilter::make(normal_predicates));
    new_aggregation->PushChild(new_filter);
    bottom_operator = new_filter;
  }
  bottom_operator->PushChild(filter_expr->Children()[0]);

  transformed.push_back(output);
}
}  // namespace optimizer
}  // namespace peloton
