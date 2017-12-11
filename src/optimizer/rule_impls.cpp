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

#include "optimizer/rule_impls.h"
#include "optimizer/util.h"
#include "optimizer/operators.h"
#include "storage/data_table.h"

#include <memory>

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
                                   Memo *memo) const {
  (void)memo;
  (void)expr;
  return true;
}

void InnerJoinCommutativity::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed, UNUSED_ATTRIBUTE Memo* memo) const {
  auto result_plan =
      std::make_shared<OperatorExpression>(LogicalInnerJoin::make());
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
                           Memo *memo) const {
  (void)memo;
  const LogicalGet *get = plan->Op().As<LogicalGet>();
  return get->table == nullptr;
}

void GetToDummyScan::Transform(
    UNUSED_ATTRIBUTE std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed, UNUSED_ATTRIBUTE Memo* memo) const {
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
                         Memo *memo) const {
  (void)memo;
  const LogicalGet *get = plan->Op().As<LogicalGet>();
  return get->table != nullptr;
}

void GetToSeqScan::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed, UNUSED_ATTRIBUTE Memo* memo) const {
  const LogicalGet *get = input->Op().As<LogicalGet>();

  auto result_plan = std::make_shared<OperatorExpression>(
      PhysicalSeqScan::make(get->get_id, get->table, get->table_alias,
                            get->predicate, get->is_for_update));

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
                           Memo *memo) const {
  // If there is a index for the table, return true,
  // else return false
  (void)memo;
  bool index_exist = false;
  const LogicalGet *get = plan->Op().As<LogicalGet>();
  if (get != nullptr && get->table != nullptr &&
      !get->table->GetIndexColumns().empty())
    index_exist = true;
  return index_exist;
}

void GetToIndexScan::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed, UNUSED_ATTRIBUTE Memo* memo) const {
  const LogicalGet *get = input->Op().As<LogicalGet>();

  auto result_plan = std::make_shared<OperatorExpression>(
      PhysicalIndexScan::make(get->get_id, get->table, get->table_alias,
                              get->predicate, get->is_for_update));

  UNUSED_ATTRIBUTE std::vector<std::shared_ptr<OperatorExpression>> children =
      input->Children();
  PL_ASSERT(children.size() == 0);

  transformed.push_back(result_plan);
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
    std::shared_ptr<OperatorExpression> expr, Memo *memo) const {
  (void)memo;
  (void)expr;
  return true;
}

void LogicalQueryDerivedGetToPhysical::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed, UNUSED_ATTRIBUTE Memo* memo) const {
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
                                    Memo *memo) const {
  (void)plan;
  (void)memo;
  return true;
}

void LogicalDeleteToPhysical::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed, UNUSED_ATTRIBUTE Memo* memo) const {
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
                                    Memo *memo) const {
  (void)plan;
  (void)memo;
  return true;
}

void LogicalUpdateToPhysical::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed, UNUSED_ATTRIBUTE Memo* memo) const {
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
                                    Memo *memo) const {
  (void)plan;
  (void)memo;
  return true;
}

void LogicalInsertToPhysical::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed, UNUSED_ATTRIBUTE Memo* memo) const {
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
    std::shared_ptr<OperatorExpression> plan, Memo *memo) const {
  (void)plan;
  (void)memo;
  return true;
}

void LogicalInsertSelectToPhysical::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed, UNUSED_ATTRIBUTE Memo* memo) const {
  const LogicalInsertSelect *insert_op = input->Op().As<LogicalInsertSelect>();
  auto result = std::make_shared<OperatorExpression>(
      PhysicalInsertSelect::make(insert_op->target_table));
  PL_ASSERT(input->Children().size() == 1);
  result->PushChild(input->Children().at(0));
  transformed.push_back(result);
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalGroupByToHashGroupBy
LogicalGroupByToHashGroupBy::LogicalGroupByToHashGroupBy() {
  type_ = RuleType::AGGREGATE_TO_HASH_AGGREGATE;
  match_pattern = std::make_shared<Pattern>(OpType::LogicalGroupBy);
  std::shared_ptr<Pattern> child(std::make_shared<Pattern>(OpType::Leaf));
  match_pattern->AddChild(child);
}

bool LogicalGroupByToHashGroupBy::Check(
    UNUSED_ATTRIBUTE std::shared_ptr<OperatorExpression> plan,
    Memo *memo) const {
  (void)memo;
  return true;
}

void LogicalGroupByToHashGroupBy::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed, UNUSED_ATTRIBUTE Memo* memo) const {
  const LogicalGroupBy *agg_op = input->Op().As<LogicalGroupBy>();
  auto result = std::make_shared<OperatorExpression>(
      PhysicalHashGroupBy::make(agg_op->columns, agg_op->having.get()));
  PL_ASSERT(input->Children().size() == 1);
  result->PushChild(input->Children().at(0));
  transformed.push_back(result);
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalAggregateToPhysical
LogicalAggregateToPhysical::LogicalAggregateToPhysical() {
  type_ = RuleType::AGGREGATE_TO_PLAIN_AGGREGATE;
  match_pattern = std::make_shared<Pattern>(OpType::LogicalAggregate);
  std::shared_ptr<Pattern> child(std::make_shared<Pattern>(OpType::Leaf));
  match_pattern->AddChild(child);
}

bool LogicalAggregateToPhysical::Check(
    UNUSED_ATTRIBUTE std::shared_ptr<OperatorExpression> plan,
    Memo *memo) const {
  (void)memo;
  return true;
}

void LogicalAggregateToPhysical::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed, UNUSED_ATTRIBUTE Memo* memo) const {
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
                                   Memo *memo) const {
  (void)memo;
  (void)plan;
  return true;
}

void InnerJoinToInnerNLJoin::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed, UNUSED_ATTRIBUTE Memo* memo) const {
  // first build an expression representing hash join
  const LogicalInnerJoin *inner_join = input->Op().As<LogicalInnerJoin>();
  auto result_plan = std::make_shared<OperatorExpression>(
      PhysicalInnerNLJoin::make(inner_join->join_predicates));
  std::vector<std::shared_ptr<OperatorExpression>> children = input->Children();
  PL_ASSERT(children.size() == 2);

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
                                     Memo *memo) const {
  (void)memo;
  (void)plan;
  // TODO(abpoms): Figure out how to determine if the join condition is hashable
  // If join column != empty then hashable
  auto children = plan->Children();
  PL_ASSERT(children.size() == 2);
  auto left_group_id = children[0]->Op().As<LeafOperator>()->origin_group;
  auto right_group_id = children[1]->Op().As<LeafOperator>()->origin_group;
  const auto &left_group_alias =
      memo->GetGroupByID(left_group_id)->GetTableAliases();
  const auto &right_group_alias =
      memo->GetGroupByID(right_group_id)->GetTableAliases();

  auto predicates = plan->Op().As<LogicalInnerJoin>()->join_predicates;

  for (auto& expr : predicates) {
    if (util::ContainsJoinColumns(left_group_alias, right_group_alias, expr.get()))
      return true;
  }
  return false;
}

void InnerJoinToInnerHashJoin::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed, UNUSED_ATTRIBUTE Memo* memo) const {
  // first build an expression representing hash join
  const LogicalInnerJoin *inner_join = input->Op().As<LogicalInnerJoin>();
  auto result_plan = std::make_shared<OperatorExpression>(
      PhysicalInnerHashJoin::make(inner_join->join_predicates));
  std::vector<std::shared_ptr<OperatorExpression>> children = input->Children();
  PL_ASSERT(children.size() == 2);

  // Then push all children into the child list of the new operator
  result_plan->PushChild(children[0]);
  result_plan->PushChild(children[1]);

  transformed.push_back(result_plan);

  return;
}

//===--------------------------------------------------------------------===//
// Rewrite rules
//===--------------------------------------------------------------------===//
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

bool PushFilterThroughJoin::Check(std::shared_ptr<OperatorExpression> plan, Memo *memo) const {
  (void)memo;
  (void)plan;

  auto& children = plan->Children();
  PL_ASSERT(children.size() == 1);
  auto& join = children.at(0);
  PL_ASSERT(join->Op().type() == OpType::InnerJoin);
  PL_ASSERT(join->Children().size() == 2);

  return true;
}

void PushFilterThroughJoin::Transform(std::shared_ptr<OperatorExpression> input,
                                      std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                                      UNUSED_ATTRIBUTE Memo* memo) const {
  auto join_op_expr = input->Children().at(0);
  auto& join_children = join_op_expr->Children();
  auto left_group_id = join_children[0]->Op().As<LeafOperator>()->origin_group;
  auto right_group_id = join_children[1]->Op().As<LeafOperator>()->origin_group;
  const auto &left_group_alias =
      memo->GetGroupByID(left_group_id)->GetTableAliases();
  const auto &right_group_alias =
      memo->GetGroupByID(right_group_id)->GetTableAliases();

  auto& predicates = input->Op().As<LogicalFilter>()->predicates;
  std::vector<AnnotatedExpression> left_predicates;
  std::vector<AnnotatedExpression> right_predicates;
  std::vector<std::shared_ptr<expression::AbstractExpression>> join_predicates;

  for (auto& predicate : predicates) {
    if (util::IsSubset(left_group_alias, predicate.table_alias_set))
      left_predicates.emplace_back(predicate);
    else if (util::IsSubset(right_group_alias, predicate.table_alias_set))
      right_predicates.emplace_back(predicate);
    else
      join_predicates.emplace_back(predicate.expr);
  }

  std::shared_ptr<OperatorExpression> output;
  // Construct join operator
  if (join_predicates.empty())
    output = join_op_expr;
  else {
    auto pre_join_predicate = join_op_expr->Op().As<LogicalInnerJoin>()->join_predicates;
    join_predicates.insert(join_predicates.end(), pre_join_predicate.begin(), pre_join_predicate.end());
    output = std::make_shared<OperatorExpression>(LogicalInnerJoin::make(join_predicates));
  }

  // Construct left filter if any
  if (!left_predicates.empty()) {
    auto left_filter = std::make_shared<OperatorExpression>(LogicalFilter::make(left_predicates));
    left_filter->PushChild(join_op_expr->Children()[0]);
    output->PushChild(left_filter);
  }
  else
    output->PushChild(join_op_expr->Children()[0]);

  // Construct left filter if any
  if (!right_predicates.empty()) {
    auto right_filter = std::make_shared<OperatorExpression>(LogicalFilter::make(right_predicates));
    right_filter->PushChild(join_op_expr->Children()[1]);
    output->PushChild(right_filter);
  }
  else
    output->PushChild(join_op_expr->Children()[1]);

  transformed.push_back(output);
}

}  // namespace optimizer
}  // namespace peloton
