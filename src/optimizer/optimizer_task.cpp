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
#include "optimizer/optimizer_metadata.h"

namespace peloton {
namespace optimizer {
//===--------------------------------------------------------------------===//
// Base class
//===--------------------------------------------------------------------===//
void OptimizerTask::PushTask(OptimizerTask *task) {
  context_->task_pool->Push(task);
}

Memo& OptimizerTask::GetMemo() const {return context_->metadata->memo;}

RuleSet& OptimizerTask::GetRuleSet() const { return context_->metadata->rule_set;}

//===--------------------------------------------------------------------===//
// OptimizeGroup
//===--------------------------------------------------------------------===//
void OptimizeGroup::execute() {
  if (group_->GetCostLB() > context_->cost_upper_boun ||  // Cost LB > Cost UB
      group_->GetBestExpression(*(context_->required_prop.get())) != nullptr)  // Has optimized given the context
    return;

  // Push explore task first for logical expressions
  for(auto& logical_expr : group_->GetLogicalExpressions())
    PushTask(new OptimizeExpression(logical_expr.get(), context_));

  // Push implement tasks to ensure that they are run first (for early pruning)
  for(auto& physical_expr : group_->GetPhysicalExpressions())
    PushTask(new OptimizeInputs(physical_expr.get(), context_));
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

    auto promise = rule->Promise(group_expr_, context_);
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
        PushTask(new ExploreGroup(GetMemo().GetGroupByID(group_expr_->GetChildGroupIDs()[child_group_idx]), metadata_));
      }
      child_group_idx++;
    }
  }
}

//===--------------------------------------------------------------------===//
// ExploreGroup
//===--------------------------------------------------------------------===//

} // namespace optimizer
} // namespace peloton