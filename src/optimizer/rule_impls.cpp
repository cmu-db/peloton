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

bool InnerJoinCommutativity::Check(std::shared_ptr<OpExpression> expr) const {
  (void)expr;
  return true;
}

void InnerJoinCommutativity::Transform(
  std::shared_ptr<OpExpression> input,
  std::vector<std::shared_ptr<OpExpression>> &transformed) const
{
  auto result_plan = std::make_shared<OpExpression>(LogicalInnerJoin::make());
  std::vector<std::shared_ptr<OpExpression>> children = input->Children();
  assert(children.size() == 3);
  result_plan->PushChild(children[1]);
  result_plan->PushChild(children[0]);
  result_plan->PushChild(children[2]);

  transformed.push_back(result_plan);
}

///////////////////////////////////////////////////////////////////////////////
/// GetToScan
GetToScan::GetToScan() {
  physical = true;

  match_pattern = std::make_shared<Pattern>(OpType::Get);
}

bool GetToScan::Check(std::shared_ptr<OpExpression> plan) const {
  (void) plan;
  return true;
}

void GetToScan::Transform(
  std::shared_ptr<OpExpression> input,
  std::vector<std::shared_ptr<OpExpression>> &transformed) const
{
  const LogicalGet *get = input->Op().as<LogicalGet>();

  auto result_plan = std::make_shared<OpExpression>(
    PhysicalScan::make(get->table, get->columns));

  transformed.push_back(result_plan);
}

///////////////////////////////////////////////////////////////////////////////
/// ProjectToComputeExprs
ProjectToComputeExprs::ProjectToComputeExprs() {
  physical = true;

  std::shared_ptr<Pattern> child(std::make_shared<Pattern>(OpType::Leaf));
  std::shared_ptr<Pattern> project_list(
    std::make_shared<Pattern>(OpType::Leaf));
  match_pattern = std::make_shared<Pattern>(OpType::Project);
  match_pattern->AddChild(child);
  match_pattern->AddChild(project_list);
}

bool ProjectToComputeExprs::Check(std::shared_ptr<OpExpression> plan) const {
  (void) plan;
  return true;
}

void ProjectToComputeExprs::Transform(
    std::shared_ptr<OpExpression> input,
    std::vector<std::shared_ptr<OpExpression>> &transformed) const
{
  auto result =
    std::make_shared<OpExpression>(PhysicalComputeExprs::make());
  std::vector<std::shared_ptr<OpExpression>> children = input->Children();
  assert(children.size() == 2);
  result->PushChild(children[0]);
  result->PushChild(children[1]);

  transformed.push_back(result);
}

///////////////////////////////////////////////////////////////////////////////
/// SelectToFilter
SelectToFilter::SelectToFilter() {
  physical = true;

  std::shared_ptr<Pattern> child(std::make_shared<Pattern>(OpType::Leaf));
  std::shared_ptr<Pattern> predicate(std::make_shared<Pattern>(OpType::Leaf));
  match_pattern = std::make_shared<Pattern>(OpType::Select);
  match_pattern->AddChild(child);
  match_pattern->AddChild(predicate);
}

bool SelectToFilter::Check(std::shared_ptr<OpExpression> plan) const {
  (void) plan;
  return true;
}

void SelectToFilter::Transform(
  std::shared_ptr<OpExpression> input,
  std::vector<std::shared_ptr<OpExpression>> &transformed) const
{
  auto result =
    std::make_shared<OpExpression>(PhysicalFilter::make());
  std::vector<std::shared_ptr<OpExpression>> children = input->Children();
  assert(children.size() == 2);
  result->PushChild(children[0]);
  result->PushChild(children[1]);

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

bool InnerJoinToInnerNLJoin::Check(std::shared_ptr<OpExpression> plan) const {
  (void) plan;
  return true;
}

void InnerJoinToInnerNLJoin::Transform(
  std::shared_ptr<OpExpression> input,
  std::vector<std::shared_ptr<OpExpression>> &transformed) const
{
  // first build an expression representing hash join
  auto result_plan = std::make_shared<OpExpression>(
    PhysicalInnerNLJoin::make());
  std::vector<std::shared_ptr<OpExpression>> children = input->Children();
  assert(children.size() == 3);

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

bool LeftJoinToLeftNLJoin::Check(std::shared_ptr<OpExpression> plan) const {
  (void) plan;
  return true;
}

void LeftJoinToLeftNLJoin::Transform(
  std::shared_ptr<OpExpression> input,
  std::vector<std::shared_ptr<OpExpression>> &transformed) const
{
  auto result_plan = std::make_shared<OpExpression>(PhysicalLeftNLJoin::make());
  std::vector<std::shared_ptr<OpExpression>> children = input->Children();
  assert(children.size() == 3);

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

bool RightJoinToRightNLJoin::Check(std::shared_ptr<OpExpression> plan) const {
  (void) plan;
  return true;
}

void RightJoinToRightNLJoin::Transform(
  std::shared_ptr<OpExpression> input,
  std::vector<std::shared_ptr<OpExpression>> &transformed) const
{
  auto result_plan = std::make_shared<OpExpression>(
    PhysicalRightNLJoin::make());
  std::vector<std::shared_ptr<OpExpression>> children = input->Children();
  assert(children.size() == 3);

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

bool OuterJoinToOuterNLJoin::Check(std::shared_ptr<OpExpression> plan) const {
  (void) plan;
  return true;
}

void OuterJoinToOuterNLJoin::Transform(
  std::shared_ptr<OpExpression> input,
  std::vector<std::shared_ptr<OpExpression>> &transformed) const
{
  auto result_plan = std::make_shared<OpExpression>(
    PhysicalOuterNLJoin::make());
  std::vector<std::shared_ptr<OpExpression>> children = input->Children();
  assert(children.size() == 3);

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

bool InnerJoinToInnerHashJoin::Check(std::shared_ptr<OpExpression> plan) const {
  (void) plan;
  // TODO(abpoms): Figure out how to determine if the join condition is hashable
  return false;
}

void InnerJoinToInnerHashJoin::Transform(
  std::shared_ptr<OpExpression> input,
  std::vector<std::shared_ptr<OpExpression>> &transformed) const
{
  // first build an expression representing hash join
  auto result_plan = std::make_shared<OpExpression>(
    PhysicalInnerHashJoin::make());
  std::vector<std::shared_ptr<OpExpression>> children = input->Children();
  assert(children.size() == 3);

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

bool LeftJoinToLeftHashJoin::Check(std::shared_ptr<OpExpression> plan) const {
  (void) plan;
  return false;
}

void LeftJoinToLeftHashJoin::Transform(
  std::shared_ptr<OpExpression> input,
  std::vector<std::shared_ptr<OpExpression>> &transformed) const
{
  auto result_plan = std::make_shared<OpExpression>(
    PhysicalLeftHashJoin::make());
  std::vector<std::shared_ptr<OpExpression>> children = input->Children();
  assert(children.size() == 3);

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

bool RightJoinToRightHashJoin::Check(std::shared_ptr<OpExpression> plan) const {
  (void) plan;
  return false;
}

void RightJoinToRightHashJoin::Transform(
  std::shared_ptr<OpExpression> input,
  std::vector<std::shared_ptr<OpExpression>> &transformed) const
{
  auto result_plan = std::make_shared<OpExpression>(
    PhysicalRightHashJoin::make());
  std::vector<std::shared_ptr<OpExpression>> children = input->Children();
  assert(children.size() == 3);

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

bool OuterJoinToOuterHashJoin::Check(std::shared_ptr<OpExpression> plan) const {
  (void) plan;
  return false;
}

void OuterJoinToOuterHashJoin::Transform(
  std::shared_ptr<OpExpression> input,
  std::vector<std::shared_ptr<OpExpression>> &transformed) const
{
  auto result_plan = std::make_shared<OpExpression>(
    PhysicalOuterHashJoin::make());
  std::vector<std::shared_ptr<OpExpression>> children = input->Children();
  assert(children.size() == 3);

  result_plan->PushChild(children[0]);
  result_plan->PushChild(children[1]);
  result_plan->PushChild(children[2]);

  transformed.push_back(result_plan);

  return;
}

} /* namespace optimizer */
} /* namespace peloton */
