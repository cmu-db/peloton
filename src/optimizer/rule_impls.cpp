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

///////////////////////////////////////////////////////////////////////////////
/// InnerJoinCommutativity
InnerJoinCommutativity::InnerJoinCommutativity() {
  logical = true;

  std::shared_ptr<Pattern> left_child(std::make_shared<Pattern>(OpType::Leaf));
  std::shared_ptr<Pattern> right_child(std::make_shared<Pattern>(OpType::Leaf));
  std::shared_ptr<Pattern> predicate(std::make_shared<Pattern>(OpType::Leaf));
  match_pattern = std::make_shared<Pattern>(OpType::InnerJoin);
  match_pattern->AddChild(left_child);
  match_pattern->AddChild(right_child);
  match_pattern->AddChild(predicate);
}

bool InnerJoinCommutativity::Check(std::shared_ptr<OperatorExpression> expr,
                                   Memo *memo) const {
  (void)memo;
  (void)expr;
  return true;
}

void InnerJoinCommutativity::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed) const {
  auto result_plan =
      std::make_shared<OperatorExpression>(LogicalInnerJoin::make());
  std::vector<std::shared_ptr<OperatorExpression>> children = input->Children();
  PL_ASSERT(children.size() == 2);
  result_plan->PushChild(children[1]);
  result_plan->PushChild(children[0]);

  transformed.push_back(result_plan);
}

///////////////////////////////////////////////////////////////////////////////
/// GetToDummyScan
GetToDummyScan::GetToDummyScan() {
  physical = true;

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
    std::vector<std::shared_ptr<OperatorExpression>> &transformed) const {
  auto result_plan = std::make_shared<OperatorExpression>(DummyScan::make());

  transformed.push_back(result_plan);
}

///////////////////////////////////////////////////////////////////////////////
/// GetToSeqScan
GetToSeqScan::GetToSeqScan() {
  physical = true;

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
    std::vector<std::shared_ptr<OperatorExpression>> &transformed) const {
  const LogicalGet *get = input->Op().As<LogicalGet>();

  auto result_plan =
      std::make_shared<OperatorExpression>(
          PhysicalSeqScan::make(get->table, get->table_alias, get->is_for_update));

  UNUSED_ATTRIBUTE std::vector<std::shared_ptr<OperatorExpression>> children =
      input->Children();
  PL_ASSERT(children.size() == 0);

  transformed.push_back(result_plan);
}

///////////////////////////////////////////////////////////////////////////////
/// GetToIndexScan
GetToIndexScan::GetToIndexScan() {
  physical = true;

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
    std::vector<std::shared_ptr<OperatorExpression>> &transformed) const {
  const LogicalGet *get = input->Op().As<LogicalGet>();

  auto result_plan = std::make_shared<OperatorExpression>(
      PhysicalIndexScan::make(get->table, get->table_alias, get->is_for_update));

  UNUSED_ATTRIBUTE std::vector<std::shared_ptr<OperatorExpression>> children =
      input->Children();
  PL_ASSERT(children.size() == 0);

  transformed.push_back(result_plan);
}

///////////////////////////////////////////////////////////////////////////////
/// SelectToFilter
LogicalFilterToPhysical::LogicalFilterToPhysical() {
  physical = true;

  std::shared_ptr<Pattern> child(std::make_shared<Pattern>(OpType::Leaf));
  std::shared_ptr<Pattern> predicate(std::make_shared<Pattern>(OpType::Leaf));
  match_pattern = std::make_shared<Pattern>(OpType::LogicalFilter);
  match_pattern->AddChild(child);
  match_pattern->AddChild(predicate);
}

bool LogicalFilterToPhysical::Check(std::shared_ptr<OperatorExpression> plan,
                                    Memo *memo) const {
  (void)plan;
  (void)memo;
  return true;
}

void LogicalFilterToPhysical::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed) const {
  auto result = std::make_shared<OperatorExpression>(PhysicalFilter::make());
  std::vector<std::shared_ptr<OperatorExpression>> children = input->Children();
  PL_ASSERT(children.size() == 2);
  result->PushChild(children[0]);
  result->PushChild(children[1]);

  transformed.push_back(result);
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalDeleteToPhysical
LogicalDeleteToPhysical::LogicalDeleteToPhysical() {
  physical = true;
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
    std::vector<std::shared_ptr<OperatorExpression>> &transformed) const {
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
  physical = true;
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
    std::vector<std::shared_ptr<OperatorExpression>> &transformed) const {
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
  physical = true;
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
    std::vector<std::shared_ptr<OperatorExpression>> &transformed) const {
  const LogicalInsert *insert_op = input->Op().As<LogicalInsert>();
  auto result = std::make_shared<OperatorExpression>(PhysicalInsert::make(
      insert_op->target_table, insert_op->columns, insert_op->values));
  PL_ASSERT(input->Children().size() == 0);
  //  result->PushChild(input->Children().at(0));
  transformed.push_back(result);
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalGroupByToHashGroupBy
LogicalGroupByToHashGroupBy::LogicalGroupByToHashGroupBy() {
  physical = true;
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
    std::vector<std::shared_ptr<OperatorExpression>> &transformed) const {
  const LogicalGroupBy *agg_op = input->Op().As<LogicalGroupBy>();
  auto result = std::make_shared<OperatorExpression>(
      PhysicalHashGroupBy::make(agg_op->columns, agg_op->having));
  PL_ASSERT(input->Children().size() == 1);
  result->PushChild(input->Children().at(0));
  transformed.push_back(result);
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalGroupByToSortGroupBy
LogicalGroupByToSortGroupBy::LogicalGroupByToSortGroupBy() {
  physical = true;
  match_pattern = std::make_shared<Pattern>(OpType::LogicalGroupBy);
  std::shared_ptr<Pattern> child(std::make_shared<Pattern>(OpType::Leaf));
  match_pattern->AddChild(child);
}

bool LogicalGroupByToSortGroupBy::Check(
    UNUSED_ATTRIBUTE std::shared_ptr<OperatorExpression> plan,
    Memo *memo) const {
  (void)memo;
  return true;
}

void LogicalGroupByToSortGroupBy::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed) const {
  const LogicalGroupBy *agg_op = input->Op().As<LogicalGroupBy>();
  auto result = std::make_shared<OperatorExpression>(
      PhysicalSortGroupBy::make(agg_op->columns, agg_op->having));
  PL_ASSERT(input->Children().size() == 1);
  result->PushChild(input->Children().at(0));
  transformed.push_back(result);
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalAggregateToPhysical
LogicalAggregateToPhysical::LogicalAggregateToPhysical() {
  physical = true;
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
    std::vector<std::shared_ptr<OperatorExpression>> &transformed) const {
  auto result = std::make_shared<OperatorExpression>(PhysicalAggregate::make());
  PL_ASSERT(input->Children().size() == 1);
  result->PushChild(input->Children().at(0));
  transformed.push_back(result);
}

///////////////////////////////////////////////////////////////////////////////
/// InnerJoinToInnerNLJoin
InnerJoinToInnerNLJoin::InnerJoinToInnerNLJoin() {
  physical = true;

  // TODO NLJoin currently only support left deep tree
  std::shared_ptr<Pattern> left_child(std::make_shared<Pattern>(OpType::Leaf));
  std::shared_ptr<Pattern> right_child(std::make_shared<Pattern>(OpType::Get));

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
    std::vector<std::shared_ptr<OperatorExpression>> &transformed) const {
  // first build an expression representing hash join
  const LogicalInnerJoin *inner_join = input->Op().As<LogicalInnerJoin>();
  auto result_plan = std::make_shared<OperatorExpression>(
      PhysicalInnerNLJoin::make(inner_join->join_predicate));
  std::vector<std::shared_ptr<OperatorExpression>> children = input->Children();
  PL_ASSERT(children.size() == 2);

  // Then push all children into the child list of the new operator
  result_plan->PushChild(children[0]);
  result_plan->PushChild(children[1]);

  transformed.push_back(result_plan);

  return;
}

///////////////////////////////////////////////////////////////////////////////
/// LeftJoinToLeftNLJoin
LeftJoinToLeftNLJoin::LeftJoinToLeftNLJoin() {
  physical = true;

  std::shared_ptr<Pattern> left_child(std::make_shared<Pattern>(OpType::Leaf));
  std::shared_ptr<Pattern> right_child(std::make_shared<Pattern>(OpType::Leaf));
  std::shared_ptr<Pattern> predicate(std::make_shared<Pattern>(OpType::Leaf));

  match_pattern = std::make_shared<Pattern>(OpType::LeftJoin);

  match_pattern->AddChild(left_child);
  match_pattern->AddChild(right_child);
  match_pattern->AddChild(predicate);

  return;
}

bool LeftJoinToLeftNLJoin::Check(std::shared_ptr<OperatorExpression> plan,
                                 Memo *memo) const {
  (void)memo;
  (void)plan;
  return true;
}

void LeftJoinToLeftNLJoin::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed) const {
  const LogicalLeftJoin *left_join = input->Op().As<LogicalLeftJoin>();
  auto result_plan = std::make_shared<OperatorExpression>(
      PhysicalLeftNLJoin::make(left_join->join_predicate));
  std::vector<std::shared_ptr<OperatorExpression>> children = input->Children();
  PL_ASSERT(children.size() == 3);

  result_plan->PushChild(children[0]);
  result_plan->PushChild(children[1]);
  result_plan->PushChild(children[2]);

  transformed.push_back(result_plan);

  return;
}

///////////////////////////////////////////////////////////////////////////////
/// RightJoinToRightNLJoin
RightJoinToRightNLJoin::RightJoinToRightNLJoin() {
  physical = true;

  std::shared_ptr<Pattern> left_child(std::make_shared<Pattern>(OpType::Leaf));
  std::shared_ptr<Pattern> right_child(std::make_shared<Pattern>(OpType::Leaf));
  std::shared_ptr<Pattern> predicate(std::make_shared<Pattern>(OpType::Leaf));

  match_pattern = std::make_shared<Pattern>(OpType::RightJoin);

  match_pattern->AddChild(left_child);
  match_pattern->AddChild(right_child);
  match_pattern->AddChild(predicate);

  return;
}

bool RightJoinToRightNLJoin::Check(std::shared_ptr<OperatorExpression> plan,
                                   Memo *memo) const {
  (void)memo;
  (void)plan;
  return true;
}

void RightJoinToRightNLJoin::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed) const {
  const LogicalRightJoin *right_join = input->Op().As<LogicalRightJoin>();
  auto result_plan = std::make_shared<OperatorExpression>(
      PhysicalRightNLJoin::make(right_join->join_predicate));
  std::vector<std::shared_ptr<OperatorExpression>> children = input->Children();
  PL_ASSERT(children.size() == 3);

  result_plan->PushChild(children[0]);
  result_plan->PushChild(children[1]);
  result_plan->PushChild(children[2]);

  transformed.push_back(result_plan);

  return;
}

///////////////////////////////////////////////////////////////////////////////
/// OuterJoinToOuterNLJoin
OuterJoinToOuterNLJoin::OuterJoinToOuterNLJoin() {
  physical = true;

  std::shared_ptr<Pattern> left_child(std::make_shared<Pattern>(OpType::Leaf));
  std::shared_ptr<Pattern> right_child(std::make_shared<Pattern>(OpType::Leaf));
  std::shared_ptr<Pattern> predicate(std::make_shared<Pattern>(OpType::Leaf));

  match_pattern = std::make_shared<Pattern>(OpType::OuterJoin);

  match_pattern->AddChild(left_child);
  match_pattern->AddChild(right_child);
  match_pattern->AddChild(predicate);

  return;
}

bool OuterJoinToOuterNLJoin::Check(std::shared_ptr<OperatorExpression> plan,
                                   Memo *memo) const {
  (void)memo;
  (void)plan;
  return true;
}

void OuterJoinToOuterNLJoin::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed) const {
  const LogicalOuterJoin *outer_join = input->Op().As<LogicalOuterJoin>();
  auto result_plan = std::make_shared<OperatorExpression>(
      PhysicalOuterNLJoin::make(outer_join->join_predicate));
  std::vector<std::shared_ptr<OperatorExpression>> children = input->Children();
  PL_ASSERT(children.size() == 3);

  result_plan->PushChild(children[0]);
  result_plan->PushChild(children[1]);
  result_plan->PushChild(children[2]);

  transformed.push_back(result_plan);

  return;
}

///////////////////////////////////////////////////////////////////////////////
/// InnerJoinToInnerHashJoin
InnerJoinToInnerHashJoin::InnerJoinToInnerHashJoin() {
  physical = true;

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

  auto expr = plan->Op().As<LogicalInnerJoin>()->join_predicate.get();

  if (util::ContainsJoinColumns(left_group_alias, right_group_alias, expr))
    return true;
  return false;
}

void InnerJoinToInnerHashJoin::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed) const {
  // first build an expression representing hash join
  const LogicalInnerJoin *inner_join = input->Op().As<LogicalInnerJoin>();
  auto result_plan = std::make_shared<OperatorExpression>(
      PhysicalInnerHashJoin::make(inner_join->join_predicate));
  std::vector<std::shared_ptr<OperatorExpression>> children = input->Children();
  PL_ASSERT(children.size() == 2);

  // Then push all children into the child list of the new operator
  result_plan->PushChild(children[0]);
  result_plan->PushChild(children[1]);

  transformed.push_back(result_plan);

  return;
}

///////////////////////////////////////////////////////////////////////////////
/// LeftJoinToLeftHashJoin
LeftJoinToLeftHashJoin::LeftJoinToLeftHashJoin() {
  physical = true;

  std::shared_ptr<Pattern> left_child(std::make_shared<Pattern>(OpType::Leaf));
  std::shared_ptr<Pattern> right_child(std::make_shared<Pattern>(OpType::Leaf));
  std::shared_ptr<Pattern> predicate(std::make_shared<Pattern>(OpType::Leaf));

  match_pattern = std::make_shared<Pattern>(OpType::LeftJoin);

  match_pattern->AddChild(left_child);
  match_pattern->AddChild(right_child);
  match_pattern->AddChild(predicate);

  return;
}

bool LeftJoinToLeftHashJoin::Check(std::shared_ptr<OperatorExpression> plan,
                                   Memo *memo) const {
  (void)memo;
  (void)plan;
  return false;
}

void LeftJoinToLeftHashJoin::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed) const {
  const LogicalLeftJoin *left_join = input->Op().As<LogicalLeftJoin>();
  auto result_plan = std::make_shared<OperatorExpression>(
      PhysicalLeftHashJoin::make(left_join->join_predicate));
  std::vector<std::shared_ptr<OperatorExpression>> children = input->Children();
  PL_ASSERT(children.size() == 3);

  result_plan->PushChild(children[0]);
  result_plan->PushChild(children[1]);
  result_plan->PushChild(children[2]);

  transformed.push_back(result_plan);

  return;
}

///////////////////////////////////////////////////////////////////////////////
/// RightJoinToRightHashJoin
RightJoinToRightHashJoin::RightJoinToRightHashJoin() {
  physical = true;

  std::shared_ptr<Pattern> left_child(std::make_shared<Pattern>(OpType::Leaf));
  std::shared_ptr<Pattern> right_child(std::make_shared<Pattern>(OpType::Leaf));
  std::shared_ptr<Pattern> predicate(std::make_shared<Pattern>(OpType::Leaf));

  match_pattern = std::make_shared<Pattern>(OpType::RightJoin);

  match_pattern->AddChild(left_child);
  match_pattern->AddChild(right_child);
  match_pattern->AddChild(predicate);

  return;
}

bool RightJoinToRightHashJoin::Check(std::shared_ptr<OperatorExpression> plan,
                                     Memo *memo) const {
  (void)memo;
  (void)plan;
  return false;
}

void RightJoinToRightHashJoin::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed) const {
  const LogicalRightJoin *right_join = input->Op().As<LogicalRightJoin>();
  auto result_plan = std::make_shared<OperatorExpression>(
      PhysicalRightHashJoin::make(right_join->join_predicate));
  std::vector<std::shared_ptr<OperatorExpression>> children = input->Children();
  PL_ASSERT(children.size() == 3);

  result_plan->PushChild(children[0]);
  result_plan->PushChild(children[1]);
  result_plan->PushChild(children[2]);

  transformed.push_back(result_plan);

  return;
}

///////////////////////////////////////////////////////////////////////////////
/// OuterJoinToOuterHashJoin
OuterJoinToOuterHashJoin::OuterJoinToOuterHashJoin() {
  physical = true;

  std::shared_ptr<Pattern> left_child(std::make_shared<Pattern>(OpType::Leaf));
  std::shared_ptr<Pattern> right_child(std::make_shared<Pattern>(OpType::Leaf));
  std::shared_ptr<Pattern> predicate(std::make_shared<Pattern>(OpType::Leaf));

  match_pattern = std::make_shared<Pattern>(OpType::OuterJoin);

  match_pattern->AddChild(left_child);
  match_pattern->AddChild(right_child);
  match_pattern->AddChild(predicate);

  return;
}

bool OuterJoinToOuterHashJoin::Check(std::shared_ptr<OperatorExpression> plan,
                                     Memo *memo) const {
  (void)memo;
  (void)plan;
  return false;
}

void OuterJoinToOuterHashJoin::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed) const {
  const LogicalOuterJoin *outer_join = input->Op().As<LogicalOuterJoin>();
  auto result_plan = std::make_shared<OperatorExpression>(
      PhysicalOuterHashJoin::make(outer_join->join_predicate));
  std::vector<std::shared_ptr<OperatorExpression>> children = input->Children();
  PL_ASSERT(children.size() == 3);

  result_plan->PushChild(children[0]);
  result_plan->PushChild(children[1]);
  result_plan->PushChild(children[2]);

  transformed.push_back(result_plan);

  return;
}

} /* namespace optimizer */
} /* namespace peloton */
