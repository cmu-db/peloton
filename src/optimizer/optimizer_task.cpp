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
#include "optimizer/child_property_deriver.h"

namespace peloton {
namespace optimizer {
//===--------------------------------------------------------------------===//
// Base class
//===--------------------------------------------------------------------===//
void OptimizerTask::PushTask(OptimizerTask *task) {
  context_->metadata->task_pool.Push(task);
}

Memo &OptimizerTask::GetMemo() const { return context_->metadata->memo; }

RuleSet &OptimizerTask::GetRuleSet() const { return context_->metadata->rule_set; }

//===--------------------------------------------------------------------===//
// OptimizeGroup
//===--------------------------------------------------------------------===//
void OptimizeGroup::execute() {
  if (group_->GetCostLB() > context_->cost_upper_bound ||  // Cost LB > Cost UB
      group_->GetBestExpression(context_->required_prop) != nullptr)  // Has optimized given the context
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
  // Init logic: only run once per task
  if (cur_child_idx_ == -1) {
    // TODO(patrick):
    // 1. We can init input cost using non-zero value for pruning
    // 2. We can calculate the current operator cost if we have maintain
    //    logical properties in group (e.g. stats, schema, cardinality)
    cur_total_cost_ = 0;

    // Pruning
    if (cur_total_cost_ > context_->cost_upper_bound)
      return;

    // Derive output and input properties
    ChildPropertyDeriver prop_deriver;
    output_input_properties_ = std::move(prop_deriver.GetProperties(
        group_expr_, context_->required_prop.get(), &context_->metadata->memo));
    cur_child_idx_ = 0;
  }

  // Loop over (output prop, input props) pair
  for (;cur_prop_pair_idx_ < output_input_properties_.size(); cur_prop_pair_idx_++) {
    auto &output_prop = output_input_properties_[cur_prop_pair_idx_].first;
    auto &input_props = output_input_properties_[cur_prop_pair_idx_].second;
    for (; cur_child_idx_  < group_expr_->GetChildrenGroupsSize(); cur_child_idx_++) {
      auto &i_prop = input_props[cur_child_idx_];
      auto child_group = context_->metadata->memo.GetGroupByID(
      group_expr_->GetChildGroupId(cur_child_idx_));

      // Check whether the child group is already optimized for the prop
      auto child_best_expr = child_group->GetBestExpression(i_prop);
      if (child_best_expr != nullptr) { // Directly get back the best expr if the child group is optimized
        cur_total_cost_ += child_best_expr->GetCost(i_prop);
        // Pruning
        if (cur_total_cost_ > context_->cost_upper_bound)
          break;
      } else if (pre_child_idx_ != cur_child_idx_) { // First time to optimize child group
        pre_child_idx_ = cur_child_idx_;
        PushTask(new OptimizeInputs(this));
        PushTask(new OptimizeGroup(child_group, std::make_shared<OptimizeContext>(
            context_->metadata, i_prop, context_->cost_upper_bound - cur_total_cost_)));
        return;
      } else { // If we return from OptimizeGroup, then there is no expr for the context
        break;
      }

    }
    // Check whether we successfully optimize all child group
    if (cur_child_idx_ == output_input_properties_.size()) {
      // Not need to do pruning here because it has been done when we get the best expr from the child group

      // Add this group expression to the hash tables
      group_expr_->SetLocalHashTable(output_prop, input_props, cur_total_cost_);
       if (GetMemo().GetGroupByID(group_expr_->GetGroupID())->SetExpressionCost(
           group_expr_, cur_total_cost_, output_prop)) {
         // If the cost is smaller than the winner, update the context upper bound
         context_->cost_upper_bound -= cur_total_cost_;
       }
    }

    // Reset child idx and total cost
    pre_child_idx_ = -1;
    cur_child_idx_ = 0;
    cur_total_cost_ = 0;
  }

}

} // namespace optimizer
} // namespace peloton