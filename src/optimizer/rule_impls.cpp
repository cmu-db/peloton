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
#include "optimizer/operators.h"

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

bool InnerJoinCommutativity::Check(
    std::shared_ptr<OperatorExpression> expr) const {
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
/// GetToScan
GetToScan::GetToScan() {
  physical = true;

  match_pattern = std::make_shared<Pattern>(OpType::Get);
}

bool GetToScan::Check(std::shared_ptr<OperatorExpression> plan) const {
  (void)plan;
  return true;
}

void GetToScan::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed) const {
  const LogicalGet *get = input->Op().As<LogicalGet>();

  auto result_plan =
      std::make_shared<OperatorExpression>(PhysicalScan::make(get->table));

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

bool LogicalFilterToPhysical::Check(
    std::shared_ptr<OperatorExpression> plan) const {
  (void)plan;
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
/// LogicalLimitToPhysical
LogicalLimitToPhysical::LogicalLimitToPhysical() {
  physical = true;
  match_pattern = std::make_shared<Pattern>(OpType::Limit);
  std::shared_ptr<Pattern> child(std::make_shared<Pattern>(OpType::Leaf));
  match_pattern->AddChild(child);
}

bool LogicalLimitToPhysical::Check(
    std::shared_ptr<OperatorExpression> plan) const {
  (void)plan;
  return true;
}

void LogicalLimitToPhysical::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed) const {
  const LogicalLimit *limit = input->Op().As<LogicalLimit>();
  auto result = std::make_shared<OperatorExpression>(
      PhysicalLimit::make(limit->limit, limit->offset));
  PL_ASSERT(input->Children().size() == 1);
  result->PushChild(input->Children().at(0));
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

bool LogicalDeleteToPhysical::Check(
    std::shared_ptr<OperatorExpression> plan) const {
  (void)plan;
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

bool LogicalUpdateToPhysical::Check(
    std::shared_ptr<OperatorExpression> plan) const {
  (void)plan;
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

bool LogicalInsertToPhysical::Check(
    std::shared_ptr<OperatorExpression> plan) const {
  (void)plan;
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
    UNUSED_ATTRIBUTE std::shared_ptr<OperatorExpression> plan) const {
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
    UNUSED_ATTRIBUTE std::shared_ptr<OperatorExpression> plan) const {
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
    UNUSED_ATTRIBUTE std::shared_ptr<OperatorExpression> plan) const {
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

  // Make three node types for pattern matching
  std::shared_ptr<Pattern> left_child(std::make_shared<Pattern>(OpType::Leaf));
  std::shared_ptr<Pattern> right_child(std::make_shared<Pattern>(OpType::Leaf));
  std::shared_ptr<Pattern> predicate(std::make_shared<Pattern>(OpType::Leaf));

  // Initialize a pattern for optimizer to match
  match_pattern = std::make_shared<Pattern>(OpType::InnerJoin);

  // Add node - we match join relation R and S as well as the predicate exp
  match_pattern->AddChild(left_child);
  match_pattern->AddChild(right_child);
  match_pattern->AddChild(predicate);

  return;
}

bool InnerJoinToInnerNLJoin::Check(
    std::shared_ptr<OperatorExpression> plan) const {
  (void)plan;
  return true;
}

void InnerJoinToInnerNLJoin::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed) const {
  // first build an expression representing hash join
  auto result_plan =
      std::make_shared<OperatorExpression>(PhysicalInnerNLJoin::make());
  std::vector<std::shared_ptr<OperatorExpression>> children = input->Children();
  PL_ASSERT(children.size() == 3);

  // Then push all children into the child list of the new operator
  result_plan->PushChild(children[0]);
  result_plan->PushChild(children[1]);
  result_plan->PushChild(children[2]);

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

bool LeftJoinToLeftNLJoin::Check(
    std::shared_ptr<OperatorExpression> plan) const {
  (void)plan;
  return true;
}

void LeftJoinToLeftNLJoin::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed) const {
  auto result_plan =
      std::make_shared<OperatorExpression>(PhysicalLeftNLJoin::make());
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

bool RightJoinToRightNLJoin::Check(
    std::shared_ptr<OperatorExpression> plan) const {
  (void)plan;
  return true;
}

void RightJoinToRightNLJoin::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed) const {
  auto result_plan =
      std::make_shared<OperatorExpression>(PhysicalRightNLJoin::make());
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

bool OuterJoinToOuterNLJoin::Check(
    std::shared_ptr<OperatorExpression> plan) const {
  (void)plan;
  return true;
}

void OuterJoinToOuterNLJoin::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed) const {
  auto result_plan =
      std::make_shared<OperatorExpression>(PhysicalOuterNLJoin::make());
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
  std::shared_ptr<Pattern> predicate(std::make_shared<Pattern>(OpType::Leaf));

  // Initialize a pattern for optimizer to match
  match_pattern = std::make_shared<Pattern>(OpType::InnerJoin);

  // Add node - we match join relation R and S as well as the predicate exp
  match_pattern->AddChild(left_child);
  match_pattern->AddChild(right_child);
  match_pattern->AddChild(predicate);

  return;
}

bool InnerJoinToInnerHashJoin::Check(
    std::shared_ptr<OperatorExpression> plan) const {
  (void)plan;
  // TODO(abpoms): Figure out how to determine if the join condition is hashable
  return false;
}

void InnerJoinToInnerHashJoin::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed) const {
  // first build an expression representing hash join
  auto result_plan =
      std::make_shared<OperatorExpression>(PhysicalInnerHashJoin::make());
  std::vector<std::shared_ptr<OperatorExpression>> children = input->Children();
  PL_ASSERT(children.size() == 3);

  // Then push all children into the child list of the new operator
  result_plan->PushChild(children[0]);
  result_plan->PushChild(children[1]);
  result_plan->PushChild(children[2]);

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

bool LeftJoinToLeftHashJoin::Check(
    std::shared_ptr<OperatorExpression> plan) const {
  (void)plan;
  return false;
}

void LeftJoinToLeftHashJoin::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed) const {
  auto result_plan =
      std::make_shared<OperatorExpression>(PhysicalLeftHashJoin::make());
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

bool RightJoinToRightHashJoin::Check(
    std::shared_ptr<OperatorExpression> plan) const {
  (void)plan;
  return false;
}

void RightJoinToRightHashJoin::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed) const {
  auto result_plan =
      std::make_shared<OperatorExpression>(PhysicalRightHashJoin::make());
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

bool OuterJoinToOuterHashJoin::Check(
    std::shared_ptr<OperatorExpression> plan) const {
  (void)plan;
  return false;
}

void OuterJoinToOuterHashJoin::Transform(
    std::shared_ptr<OperatorExpression> input,
    std::vector<std::shared_ptr<OperatorExpression>> &transformed) const {
  auto result_plan =
      std::make_shared<OperatorExpression>(PhysicalOuterHashJoin::make());
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
