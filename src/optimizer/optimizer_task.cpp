//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// optimizer_task.cpp
//
// Identification: src/optimizer/optimizer_task.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/optimizer_task.h"

#include "optimizer/property_enforcer.h"
#include "optimizer/optimizer_metadata.h"
#include "optimizer/binding.h"
#include "optimizer/child_property_deriver.h"
#include "optimizer/stats/stats_calculator.h"
#include "optimizer/stats/child_stats_deriver.h"
#include "optimizer/absexpr_expression.h"

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Base class
//===--------------------------------------------------------------------===//
void OptimizerTask::ConstructValidRules(
    GroupExpression *group_expr, OptimizeContext *context,
    std::vector<std::unique_ptr<Rule>> &rules,
    std::vector<RuleWithPromise> &valid_rules) {
  for (auto &rule : rules) {
    // Check if we can apply the rule
    bool root_pattern_mismatch = group_expr->Node()->GetOpType() != rule->GetMatchPattern()->GetOpType() ||
                                 group_expr->Node()->GetExpType() != rule->GetMatchPattern()->GetExpType();

    bool already_explored = group_expr->HasRuleExplored(rule.get());
    bool child_pattern_mismatch =
        group_expr->GetChildrenGroupsSize() !=
        rule->GetMatchPattern()->GetChildPatternsSize();
    if (root_pattern_mismatch || already_explored || child_pattern_mismatch) {
      continue;
    }
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
  LOG_TRACE("OptimizeGroup::Execute() group %d", group_->GetID());
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
  for (auto &physical_expr : group_->GetPhysicalExpressions()) {
    PushTask(new OptimizeInputs(physical_expr.get(), context_));
  }

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
  this->ConstructValidRules(group_expr_, context_.get(),
                            GetRuleSet().GetTransformationRules(), valid_rules);
  // Construct valid implementation rules from rule set
  this->ConstructValidRules(group_expr_, context_.get(),
                            GetRuleSet().GetImplementationRules(), valid_rules);

  std::sort(valid_rules.begin(), valid_rules.end());
  LOG_DEBUG("OptimizeExpression::execute() op %d, valid rules : %lu",
            static_cast<int>(group_expr_->Node()->GetOpType()), valid_rules.size());
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
  LOG_TRACE("ExploreGroup::execute() ");

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
  LOG_TRACE("ExploreExpression::execute() ");
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
  LOG_TRACE("ApplyRule::execute() for rule: %d", rule_->GetRuleIdx());
  if (group_expr_->HasRuleExplored(rule_)) return;

  GroupExprBindingIterator iterator(GetMemo(), group_expr_, rule_->GetMatchPattern());
  while (iterator.HasNext()) {
    auto before = iterator.Next();
    if (!rule_->Check(before, context_.get())) {
      continue;
    }

    std::vector<std::shared_ptr<AbstractNodeExpression>> after;
    rule_->Transform(before, after, context_.get());
    for (auto &new_expr : after) {
      std::shared_ptr<GroupExpression> new_gexpr;
      if (context_->metadata->RecordTransformedExpression(
              new_expr, new_gexpr, group_expr_->GetGroupID())) {
        // A new group expression is generated
        if (new_gexpr->Node()->IsLogical()) {
          // Derive stats for the *logical expression*
          PushTask(new DeriveStats(new_gexpr.get(), ExprSet{}, context_));
          if (explore_only) {
            // Explore this logical expression
            PushTask(new ExploreExpression(new_gexpr.get(), context_));
          } else {
            // Optimize this logical expression
            PushTask(new OptimizeExpression(new_gexpr.get(), context_));
          }
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
// DeriveStats
//===--------------------------------------------------------------------===//
void DeriveStats::execute() {
  // First do a top-down pass to get stats for required columns, then do a
  // bottom-up pass to calculate the stats
  ChildStatsDeriver deriver;
  auto children_required_stats = deriver.DeriveInputStats(
      gexpr_, required_cols_, &context_->metadata->memo);
  bool derive_children = false;
  // If we haven't got enough stats to compute the current stats, derive them
  // from the child first
  PELOTON_ASSERT(children_required_stats.size() == gexpr_->GetChildrenGroupsSize());
  for (size_t idx = 0; idx < children_required_stats.size(); ++idx) {
    auto &child_required_stats = children_required_stats[idx];
    auto child_group_id = gexpr_->GetChildGroupId(idx);
    // TODO(boweic): currently we pick the first child expression in the child
    // group to derive stats, in the future we may want to pick the one with
    // the highest confidence
    auto child_group_gexpr = GetMemo()
                                 .GetGroupByID(child_group_id)
                                 ->GetLogicalExpressions()[0]
                                 .get();
    if (!child_required_stats.empty() ||
        !child_group_gexpr->HasDerivedStats()) {
      // The child group has not derived stats could happen when we do top-down
      // stats derivation for the first time or a new child group is just
      // generated by join order enumeration
      if (!derive_children) {
        derive_children = true;
        // Derive stats for root later
        PushTask(new DeriveStats(this));
      }
      PushTask(
          new DeriveStats(child_group_gexpr, child_required_stats, context_));
    }
  }
  if (derive_children) {
    // We'll derive for the current group after deriving stats of children
    return;
  }

  StatsCalculator calculator;
  calculator.CalculateStats(gexpr_, required_cols_, &context_->metadata->memo,
                            context_->metadata->txn);
  gexpr_->SetDerivedStats();
}
//===--------------------------------------------------------------------===//
// OptimizeInputs
//===--------------------------------------------------------------------===//
void OptimizeInputs::execute() {
  // Init logic: only run once per task
  LOG_TRACE("OptimizeInputs::execute() ");
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
      // Compute the cost of the root operator
      // 1. Collect stats needed and cache them in the group
      // 2. Calculate cost based on children's stats
      cur_total_cost_ += context_->metadata->cost_model->CalculateCost(
          group_expr_, &context_->metadata->memo, context_->metadata->txn);
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
      } else if (prev_child_idx_ !=
                 cur_child_idx_) {  // We haven't optimized child group
        prev_child_idx_ = cur_child_idx_;
        PushTask(new OptimizeInputs(this));
        PushTask(new OptimizeGroup(
            child_group, std::make_shared<OptimizeContext>(
                context_->metadata, i_prop, context_->cost_upper_bound - cur_total_cost_)));
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
          cur_total_cost_ += context_->metadata->cost_model->CalculateCost(
              memo_enforced_expr, &context_->metadata->memo,
              context_->metadata->txn);

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
    prev_child_idx_ = -1;
    cur_child_idx_ = 0;
    cur_total_cost_ = 0;
  }
}

// ===================================================================
//
// RewriteTask related implementations
//
// ===================================================================
std::set<GroupID> RewriteTask::GetUniqueChildGroupIDs() {
  // Get current group and logical expressions
  auto cur_group = this->GetMemo().GetGroupByID(group_id_);
  auto cur_group_exprs = cur_group->GetLogicalExpressions();
  PELOTON_ASSERT(cur_group_exprs.size() >= 1);

  // Generate unique group ID numbers so we don't repeat work
  std::set<GroupID> child_groups;
  for (auto cur_group_expr : cur_group_exprs) {
    for (size_t child = 0; child < cur_group_expr->GetChildrenGroupsSize(); child++) {
      child_groups.insert(cur_group_expr->GetChildGroupId(child));
    }
  }

  return child_groups;
}

bool RewriteTask::OptimizeCurrentGroup(bool replace_on_match) {
  std::vector<RuleWithPromise> valid_rules;

  // Get current group and logical expressions
  auto cur_group = this->GetMemo().GetGroupByID(group_id_);
  auto cur_group_exprs = cur_group->GetLogicalExpressions();
  PELOTON_ASSERT(cur_group_exprs.size() >= 1);

  // Try to optimize all the logical group expressions.
  // If one gets optimized, then the group is collapsed.
  for (auto cur_group_expr_ptr : cur_group_exprs) {
    auto cur_group_expr = cur_group_expr_ptr.get();

    // Construct valid transformation rules from rule set
    this->ConstructValidRules(cur_group_expr, this->context_.get(),
                              this->GetRuleSet().GetRewriteRulesByName(rule_set_name_),
                              valid_rules);

    // Sort so that we apply rewrite rules with higher promise first
    std::sort(valid_rules.begin(), valid_rules.end(),
              std::greater<RuleWithPromise>());

    // Try applying each rule
    for (auto &r : valid_rules) {
      GroupExprBindingIterator iterator(this->GetMemo(), cur_group_expr, r.rule->GetMatchPattern());
      // Keep trying to apply until we exhaust all the bindings.
      // This could possibly be sub-optimal since the first binding that results
      // in a transformation by a rule will be applied and become the group's
      // "new" rewritten expression.
      while (iterator.HasNext()) {
        // Binding succeeded to a given expression structure
        auto before = iterator.Next();

        // Attempt to apply the transformation
        std::vector<std::shared_ptr<AbstractNodeExpression>> after;
        r.rule->Transform(before, after, this->context_.get());

        // Rewrite rule should provide at most 1 expression
        PELOTON_ASSERT(after.size() <= 1);
        if (!after.empty()) {
          // The transformation produced another expression
          auto &new_expr = after[0];
          if (replace_on_match) {
            // Replace entire group. We do not need to generate logically equivalent
            // because rewriting expressions will not generate new AND or OR clauses.
            this->context_->metadata->ReplaceRewritedExpression(new_expr, group_id_);

            // Return true to indicate optimize succeeded and the caller should try again
            return true;
          } else {
            // Insert as a new logical equivalent expression
            std::shared_ptr<GroupExpression> new_gexpr;
            GroupID group = cur_group_expr->GetGroupID();

            // Try again only if we succeeded in recording a new expression
            return this->context_->metadata->RecordTransformedExpression(new_expr, new_gexpr, group);
          }
        }
      }

      cur_group_expr->SetRuleExplored(r.rule);
    }
  }

  return false;
}

void TopDownRewrite::execute() {
  bool did_optimize = this->OptimizeCurrentGroup(replace_on_transform_);

  // Optimize succeeded and by the design, there will ever be only 1
  // that is logically equivalent, so we do not need to perform
  // any extra passes. Equivalence generating rules will not be repeatedly
  // applied to expression trees.
  //
  // This is definitely sub-optimal and is a missed opportunity for rewrite.
  // However, this requires AbstractExpression to support strict equality
  // in its post-binding state.
  if (did_optimize && replace_on_transform_) {
    auto top = new TopDownRewrite(this->group_id_, this->context_, this->rule_set_name_);
    top->SetReplaceOnTransform(replace_on_transform_);
    this->PushTask(top);
    return;
  }

  // This group has been optimized, so move on to the children
  std::set<GroupID> child_groups = this->GetUniqueChildGroupIDs();
  for (auto g_id : child_groups) {
    auto top = new TopDownRewrite(g_id, this->context_, this->rule_set_name_);
    top->SetReplaceOnTransform(replace_on_transform_);
    this->PushTask(top);
  }
}

void BottomUpRewrite::execute() {
  if (!has_optimized_child_) {
    this->PushTask(new BottomUpRewrite(this->group_id_, this->context_, this->rule_set_name_, true));

    // Get all unique GroupIDs to minimize repeated work
    // Need to rewrite all sub trees first
    std::set<GroupID> child_groups = this->GetUniqueChildGroupIDs();
    for (auto g_id : child_groups) {
      this->PushTask(new BottomUpRewrite(g_id, this->context_, this->rule_set_name_, false));
    }

    return;
  }

  // Keep rewriting until we finish
  if (this->OptimizeCurrentGroup(true)) {
    this->PushTask(new BottomUpRewrite(this->group_id_, this->context_, this->rule_set_name_, false));
  }
}

}  // namespace optimizer
}  // namespace peloton
