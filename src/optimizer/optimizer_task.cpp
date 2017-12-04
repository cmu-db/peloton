//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rule.h
//
// Identification: src/optimizer/optimizer_task.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/optimizer_task.h"
#include "optimizer/optimize_context.h"
#include "optimizer/binding.h"

namespace peloton {
namespace optimizer {
//===--------------------------------------------------------------------===//
// Base class
//===--------------------------------------------------------------------===//
void OptimizerTask::PushTask(OptimizerTask *task) {
  context_->task_pool->Push(task);
}

Memo &OptimizerTask::GetMemo() const { return context_->metadata->memo; }

RuleSet &OptimizerTask::GetRuleSet() const { return context_->metadata->rule_set; }

//===--------------------------------------------------------------------===//
// OptimizeGroup
//===--------------------------------------------------------------------===//
void OptimizeGroup::execute() {
  if (group_->GetCostLB() > context_->cost_upper_bound ||  // Cost LB > Cost UB
      group_->GetBestExpression(*(context_->required_prop.get())) != nullptr)  // Has optimized given the context
    return;

  // Push explore task first for logical expressions if the group has not been explored
  if (!group_->HasExplored()) {
    for (auto &logical_expr : group_->GetLogicalExpressions())
      PushTask(new OptimizeExpression(logical_expr.get(), context_));
  }

  // Push implement tasks to ensure that they are run first (for early pruning)
  for (auto &physical_expr : group_->GetPhysicalExpressions())
    PushTask(new OptimizeInputs(physical_expr.get(), context_));


  // Since there is no cycle in the tree, it is safe to set the flag even before all expressions are explored
  group_->SetExplorationFlag();
}

//===--------------------------------------------------------------------===//
// OptimizeExpression
//===--------------------------------------------------------------------===//
void OptimizeExpression::execute() {
  std::vector<RuleWithPromise> valid_rules;

  for (auto &rule : GetRuleSet().GetRules()) {
    if (group_expr_->HasRuleExplored(rule.get()) ||     // Rule has been applied
        group_expr_->GetChildrenGroupsSize()
            != rule->GetMatchPattern()->GetChildPatternsSize()) // Children size does not math
      continue;

    auto promise = rule->Promise(group_expr_, context_.get());
    if (promise > 0)
      valid_rules.emplace_back(rule.get(), promise);
  }

  std::sort(valid_rules.begin(), valid_rules.end());

  // Apply rule
  for (auto &r : valid_rules) {
    PushTask(new ApplyRule(group_expr_, r.rule, context_));
    int child_group_idx = 0;
    for (auto &child_pattern : r.rule->GetMatchPattern()->Children()) {
      // Only need to explore non-leaf children before applying rule to the current group
      // this condition is important for early-pruning
      if (child_pattern->GetChildPatternsSize() > 0) {
        PushTask(new ExploreGroup(
            GetMemo().GetGroupByID(group_expr_->GetChildGroupIDs()[child_group_idx]), context_));
      }
      child_group_idx++;
    }
  }

}

//===--------------------------------------------------------------------===//
// ExploreGroup
//===--------------------------------------------------------------------===//
void ExploreGroup::execute() {
  if (group_->HasExplored())
    return;

  for (auto &logical_expr : group_->GetLogicalExpressions()) {
    PushTask(new ExploreExpression(logical_expr.get(), context_));
  }

  // Since there is no cycle in the tree, it is safe to set the flag even before all expressions are explored
  group_->SetExplorationFlag();
}

//===--------------------------------------------------------------------===//
// ExploreExpression
//===--------------------------------------------------------------------===//
void ExploreExpression::execute() {
  std::vector<RuleWithPromise> valid_rules;

  for (auto &rule : GetRuleSet().GetRules()) {
    if (rule->IsPhysical() || // It is a physical rule
        group_expr_->HasRuleExplored(rule.get()) ||     // Rule has been applied
        group_expr_->GetChildrenGroupsSize()
            != rule->GetMatchPattern()->GetChildPatternsSize()) // Children size does not math
      continue;

    auto promise = rule->Promise(group_expr_, context_.get());
    if (promise > 0)
      valid_rules.emplace_back(rule.get(), promise);
  }

  std::sort(valid_rules.begin(), valid_rules.end());

  // Apply rule
  for (auto &r : valid_rules) {
    PushTask(new ApplyRule(group_expr_, r.rule, context_));
    int child_group_idx = 0;
    for (auto &child_pattern : r.rule->GetMatchPattern()->Children()) {
      // Only need to explore non-leaf children before applying rule to the current group
      // this condition is important for early-pruning
      if (child_pattern->GetChildPatternsSize() > 0) {
        PushTask(
            new ExploreGroup(GetMemo().GetGroupByID(group_expr_->GetChildGroupIDs()[child_group_idx]),
                             context_));
      }
      child_group_idx++;
    }
  }
}

//===--------------------------------------------------------------------===//
// ApplyRule
//===--------------------------------------------------------------------===//
void ApplyRule::execute() {
  if (group_expr_->HasRuleExplored(rule_))
    return;

  ItemBindingIterator iterator(nullptr, group_expr_, rule_->GetMatchPattern());
  while (iterator.HasNext()) {
    auto before = iterator.Next();
    if (!rule_->Check(before, &GetMemo()))
      continue;

    std::vector<std::shared_ptr<OperatorExpression>> after;
    rule_->Transform(before, after);
    for (auto &new_expr : after) {
      std::shared_ptr<GroupExpression> new_gexpr;
      if (context_->metadata->RecordTransformedExpression(new_expr, new_gexpr, group_expr_->GetGroupID())) {
        // A new group expression is generated
        if (new_gexpr->Op().IsLogical()) {
          // Optimize this logical expression
          PushTask(new OptimizeExpression(new_gexpr.get(), context_));
        } else {
          // Cost this physical expression and optimize its inputs
          PushTask(new OptimizeInputs(new_gexpr.get(), context_));
        }
      }
    }
  }

  group_expr_->SetRuleExplored(rule_);
}

//===--------------------------------------------------------------------===//
// OptimizeInputs
//===--------------------------------------------------------------------===//
void OptimizeInputs::execute() {
  // TODO: We can init input cost using non-zero value for pruning

  // Pruning
  if (CostSoFar() > context_->cost_upper_bound)
    return;


  for (int child_idx = current_child_no_; child_idx < group_expr_->GetChildrenGroupsSize(); child_idx++) {

  }

}

} // namespace optimizer
} // namespace peloton