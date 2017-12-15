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
#include <include/optimizer/property_enforcer.h>
#include <include/optimizer/optimizer_metadata.h>
#include "optimizer/binding.h"
#include "optimizer/child_property_deriver.h"
#include "optimizer/cost_calculator.h"

namespace peloton {
namespace optimizer {
//===--------------------------------------------------------------------===//
// Base class
//===--------------------------------------------------------------------===//
void OptimizerTask::ConstructValidRules(
    GroupExpression *group_expr, OptimizeContext *context,
    std::vector<std::unique_ptr<Rule>> &rules,
    std::vector<RuleWithPromise> &valid_rules) {
  //  LOG_DEBUG("Child size %lu", group_expr->GetChildrenGroupsSize());
  for (auto &rule : rules) {
    // LOG_DEBUG("Op %d, Pattern Op %d",
    // static_cast<int>(group_expr->Op().type()),
    //           static_cast<int>(rule->GetMatchPattern()->Type()));
    if (group_expr->Op().type() !=
            rule->GetMatchPattern()->Type() ||  // Root pattern type mismatch
        group_expr->HasRuleExplored(rule.get()) ||  // Rule has been applied
        group_expr->GetChildrenGroupsSize() !=
            rule->GetMatchPattern()
                ->GetChildPatternsSize())  // Children size does not math
      continue;

    auto promise = rule->Promise(group_expr, context);
    if (promise > 0) valid_rules.emplace_back(rule.get(), promise);
  }
}

void OptimizerTask::PushTask(OptimizerTask *task) {
  context_->metadata->task_pool->Push(task);
}

Memo &OptimizerTask::GetMemo() const { return context_->metadata->memo; }

RuleSet &OptimizerTask::GetRuleSet() const {
  return context_->metadata->rule_set;
}

//===--------------------------------------------------------------------===//
// OptimizeGroup
//===--------------------------------------------------------------------===//
void OptimizeGroup::execute() {
  // LOG_DEBUG("OptimizeGroup::Execute() group %d", group_->GetID());
  if (group_->GetCostLB() > context_->cost_upper_bound ||  // Cost LB > Cost UB
      group_->GetBestExpression(context_->required_prop) !=
          nullptr)  // Has optimized given the context
    return;

  // Push explore task first for logical expressions if the group has not been
  // explored
  if (!group_->HasExplored()) {
    for (auto &logical_expr : group_->GetLogicalExpressions())
      PushTask(new OptimizeExpression(logical_expr.get(), context_));
  }

  // Push implement tasks to ensure that they are run first (for early pruning)
  for (auto &physical_expr : group_->GetPhysicalExpressions())
    PushTask(new OptimizeInputs(physical_expr.get(), context_));

  // Since there is no cycle in the tree, it is safe to set the flag even before
  // all expressions are explored
  group_->SetExplorationFlag();
}

//===--------------------------------------------------------------------===//
// OptimizeExpression
//===--------------------------------------------------------------------===//
void OptimizeExpression::execute() {
  std::vector<RuleWithPromise> valid_rules;

  // Construct valid transformation rules from rule set
  ConstructValidRules(group_expr_, context_.get(),
                      GetRuleSet().GetTransformationRules(), valid_rules);
  // Construct valid implementation rules from rule set
  ConstructValidRules(group_expr_, context_.get(),
                      GetRuleSet().GetImplementationRules(), valid_rules);

  std::sort(valid_rules.begin(), valid_rules.end());
  // LOG_DEBUG("OptimizeExpression::execute() op %d, valid rules : %lu",
  //           static_cast<int>(group_expr_->Op().type()), valid_rules.size());

  // Apply rule
  for (auto &r : valid_rules) {
    PushTask(new ApplyRule(group_expr_, r.rule, context_));
    int child_group_idx = 0;
    for (auto &child_pattern : r.rule->GetMatchPattern()->Children()) {
      // Only need to explore non-leaf children before applying rule to the
      // current group
      // this condition is important for early-pruning
      if (child_pattern->GetChildPatternsSize() > 0) {
        PushTask(new ExploreGroup(
            GetMemo().GetGroupByID(
                group_expr_->GetChildGroupIDs()[child_group_idx]),
            context_));
      }
      child_group_idx++;
    }
  }
}

//===--------------------------------------------------------------------===//
// ExploreGroup
//===--------------------------------------------------------------------===//
void ExploreGroup::execute() {
  if (group_->HasExplored()) return;
  LOG_DEBUG("ExploreGroup::execute() ");

  for (auto &logical_expr : group_->GetLogicalExpressions()) {
    PushTask(new ExploreExpression(logical_expr.get(), context_));
  }

  // Since there is no cycle in the tree, it is safe to set the flag even before
  // all expressions are explored
  group_->SetExplorationFlag();
}

//===--------------------------------------------------------------------===//
// ExploreExpression
//===--------------------------------------------------------------------===//
void ExploreExpression::execute() {
  LOG_DEBUG("ExploreExpression::execute() ");
  std::vector<RuleWithPromise> valid_rules;

  // Construct valid transformation rules from rule set
  ConstructValidRules(group_expr_, context_.get(),
                      GetRuleSet().GetTransformationRules(), valid_rules);

  std::sort(valid_rules.begin(), valid_rules.end());

  // Apply rule
  for (auto &r : valid_rules) {
    PushTask(new ApplyRule(group_expr_, r.rule, context_, true));
    int child_group_idx = 0;
    for (auto &child_pattern : r.rule->GetMatchPattern()->Children()) {
      // Only need to explore non-leaf children before applying rule to the
      // current group
      // this condition is important for early-pruning
      if (child_pattern->GetChildPatternsSize() > 0) {
        PushTask(new ExploreGroup(
            GetMemo().GetGroupByID(
                group_expr_->GetChildGroupIDs()[child_group_idx]),
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
  // LOG_DEBUG("ApplyRule::execute() ");
  if (group_expr_->HasRuleExplored(rule_)) return;

  GroupExprBindingIterator iterator(GetMemo(), group_expr_,
                                    rule_->GetMatchPattern());
  while (iterator.HasNext()) {
    auto before = iterator.Next();
    if (!rule_->Check(before, context_.get())) continue;

    std::vector<std::shared_ptr<OperatorExpression>> after;
    rule_->Transform(before, after, context_.get());
    for (auto &new_expr : after) {
      std::shared_ptr<GroupExpression> new_gexpr;
      if (context_->metadata->RecordTransformedExpression(
              new_expr, new_gexpr, group_expr_->GetGroupID())) {
        // A new group expression is generated
        if (new_gexpr->Op().IsLogical()) {
          if (explore_only)
            // Explore this logical expression
            PushTask(new ExploreExpression(new_gexpr.get(), context_));
          else
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
  // LOG_DEBUG("OptimizeInputs::execute() ");
  if (cur_child_idx_ == -1) {
    // TODO(patrick):
    // 1. We can init input cost using non-zero value for pruning
    // 2. We can calculate the current operator cost if we have maintain
    //    logical properties in group (e.g. stats, schema, cardinality)
    cur_total_cost_ = 0;

    // Pruning
    if (cur_total_cost_ > context_->cost_upper_bound) return;

    // Derive output and input properties
    ChildPropertyDeriver prop_deriver;
    output_input_properties_ = prop_deriver.GetProperties(
        group_expr_, context_->required_prop, &context_->metadata->memo);
    cur_child_idx_ = 0;

    // TODO: If later on we support properties that may not be enforced in some
    // cases,
    // we can check whether it is the case here to do the pruning
  }

  // Loop over (output prop, input props) pair
  for (; cur_prop_pair_idx_ < (int)output_input_properties_.size();
       cur_prop_pair_idx_++) {
    auto &output_prop = output_input_properties_[cur_prop_pair_idx_].first;
    auto &input_props = output_input_properties_[cur_prop_pair_idx_].second;

    // Calculate local cost and update total cost
    if (cur_child_idx_ == 0) {
      CostCalculator cost_calculator;
      cur_total_cost_ +=
          cost_calculator.CalculatorCost(group_expr_, output_prop.get());
    }

    for (; cur_child_idx_ < (int)group_expr_->GetChildrenGroupsSize();
         cur_child_idx_++) {
      auto &i_prop = input_props[cur_child_idx_];
      auto child_group = context_->metadata->memo.GetGroupByID(
          group_expr_->GetChildGroupId(cur_child_idx_));

      // Check whether the child group is already optimized for the prop
      auto child_best_expr = child_group->GetBestExpression(i_prop);
      if (child_best_expr != nullptr) {  // Directly get back the best expr if
                                         // the child group is optimized
        cur_total_cost_ += child_best_expr->GetCost(i_prop);
        // Pruning
        if (cur_total_cost_ > context_->cost_upper_bound) break;
      } else if (pre_child_idx_ !=
                 cur_child_idx_) {  // First time to optimize child group
        pre_child_idx_ = cur_child_idx_;
        PushTask(new OptimizeInputs(this));
        PushTask(new OptimizeGroup(
            child_group, std::make_shared<OptimizeContext>(
                             context_->metadata, i_prop,
                             context_->cost_upper_bound - cur_total_cost_)));
        return;
      } else {  // If we return from OptimizeGroup, then there is no expr for
                // the context
        break;
      }
    }
    // Check whether we successfully optimize all child group
    if (cur_child_idx_ == (int)group_expr_->GetChildrenGroupsSize()) {
      // Not need to do pruning here because it has been done when we get the
      // best expr from the child group

      // Add this group expression to group expression hash table
      group_expr_->SetLocalHashTable(output_prop, input_props, cur_total_cost_);
      auto cur_group = GetMemo().GetGroupByID(group_expr_->GetGroupID());
      cur_group->SetExpressionCost(group_expr_, cur_total_cost_, output_prop);

      // Enforce property if the requirement does not meet
      PropertyEnforcer prop_enforcer;
      auto extended_output_properties = output_prop->Properties();
      GroupExpression *memo_enforced_expr = nullptr;
      bool meet_requirement = true;
      // TODO: For now, we enforce the missing properties in the order of how we
      // find them. This may
      // miss the opportunity to enforce them or may lead to sub-optimal plan.
      // This is fine now
      // because we only have one physical property (sort). If more properties
      // are added, we should
      // add some heuristics to derive the optimal enforce order or perform a
      // cost-based full enumeration.
      for (auto &prop : context_->required_prop->Properties()) {
        if (!output_prop->HasProperty(*prop)) {
          auto enforced_expr =
              prop_enforcer.EnforceProperty(group_expr_, prop.get());
          // Cannot enforce the missing property
          if (enforced_expr == nullptr) {
            meet_requirement = false;
            break;
          }
          memo_enforced_expr = GetMemo().InsertExpression(
              enforced_expr, group_expr_->GetGroupID(), true);

          // Extend the output properties after enforcement
          auto pre_output_prop_set =
              std::make_shared<PropertySet>(extended_output_properties);
          extended_output_properties.push_back(prop);

          // Cost the enforced expression
          auto extended_prop_set =
              std::make_shared<PropertySet>(extended_output_properties);
          CostCalculator cost_calculator;
          cur_total_cost_ += cost_calculator.CalculatorCost(
              memo_enforced_expr, extended_prop_set.get());

          // Update hash tables for group and group expression
          memo_enforced_expr->SetLocalHashTable(
              extended_prop_set, {pre_output_prop_set}, cur_total_cost_);
          cur_group->SetExpressionCost(memo_enforced_expr, cur_total_cost_,
                                       output_prop);
        }
      }

      // Can meet the requirement
      if (meet_requirement) {
        // If the cost is smaller than the winner, update the context upper
        // bound
        context_->cost_upper_bound -= cur_total_cost_;
        if (memo_enforced_expr != nullptr) {  // Enforcement takes place
          cur_group->SetExpressionCost(memo_enforced_expr, cur_total_cost_,
                                       context_->required_prop);
        } else if (output_prop->Properties().size() !=
                   context_->required_prop->Properties().size()) {
          // The original output property is a super set of the requirement
          cur_group->SetExpressionCost(group_expr_, cur_total_cost_,
                                       context_->required_prop);
        }
      }
    }

    // Reset child idx and total cost
    pre_child_idx_ = -1;
    cur_child_idx_ = 0;
    cur_total_cost_ = 0;
  }
}

//===--------------------------------------------------------------------===//
// RewriteExpression
//===--------------------------------------------------------------------===//
void RewriteExpression::execute() {
  // LOG_DEBUG("RewriteExpression::execute(), with group %d",
  //           parent_group_expr_->GetChildGroupId(parent_group_offset_));

  std::vector<RuleWithPromise> valid_rules;

  auto cur_group = GetMemo().GetGroupByID(
      parent_group_expr_->GetChildGroupId(parent_group_offset_));
  auto cur_group_exprs = cur_group->GetLogicalExpressions();

  // Ensure that this group only has one logical expression
  PL_ASSERT(cur_group_exprs.size() == 1);
  auto cur_group_expr = cur_group_exprs.at(0).get();

  // Construct valid transformation rules from rule set
  ConstructValidRules(cur_group_expr, context_.get(),
                      GetRuleSet().GetRewriteRules(), valid_rules);

  std::sort(valid_rules.begin(), valid_rules.end());

  // Apply rule
  for (auto &r : valid_rules) {
    PushTask(new ApplyRewriteRule(parent_group_expr_, parent_group_offset_,
                                  r.rule, context_));
  }

  for (int child_group_idx = 0;
       child_group_idx < (int)cur_group_expr->GetChildrenGroupsSize();
       child_group_idx++) {
    // Need to rewrite all sub trees first
    PushTask(new RewriteExpression(cur_group_expr, child_group_idx, context_));
  }
}

//===--------------------------------------------------------------------===//
// ApplyRewriteRule
//===--------------------------------------------------------------------===//
void ApplyRewriteRule::execute() {
  // LOG_DEBUG("ApplyRewriteRule::execute(), with group %d",
  //           parent_group_expr_->GetChildGroupId(parent_group_offset_));
  auto cur_group = GetMemo().GetGroupByID(
      parent_group_expr_->GetChildGroupId(parent_group_offset_));
  auto cur_group_expr = cur_group->GetLogicalExpressions().at(0).get();

  if (cur_group_expr->HasRuleExplored(rule_)) return;

  GroupExprBindingIterator iterator(GetMemo(), cur_group_expr,
                                    rule_->GetMatchPattern());
  while (iterator.HasNext()) {
    auto before = iterator.Next();
    if (!rule_->Check(before, context_.get())) continue;

    std::vector<std::shared_ptr<OperatorExpression>> after;
    rule_->Transform(before, after, context_.get());
    for (auto &new_expr : after) {
      std::shared_ptr<GroupExpression> new_gexpr;
      if (context_->metadata->RecordTransformedExpression(
              new_expr, new_gexpr, cur_group_expr->GetGroupID())) {
        // Update parent group expression
        GetMemo().RemoveParExpressionForRewirte(parent_group_expr_);
        parent_group_expr_->SetChildGroupID(parent_group_offset_,
                                            new_gexpr->GetGroupID());
        GetMemo().AddParExpressionForRewrite(parent_group_expr_);

        cur_group->RemoveLogicalExpression(0);

        PushTask(new RewriteExpression(parent_group_expr_, parent_group_offset_,
                                       context_));
      }
    }
  }

  cur_group_expr->SetRuleExplored(rule_);
}

}  // namespace optimizer
}  // namespace peloton
