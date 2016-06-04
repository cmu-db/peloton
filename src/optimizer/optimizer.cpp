//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// optimizer.cpp
//
// Identification: src/optimizer/optimizer.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/optimizer.h"
#include "optimizer/operator_visitor.h"
#include "optimizer/convert_query_to_op.h"
#include "optimizer/convert_op_to_plan.h"
#include "optimizer/rule_impls.h"

#include "planner/projection_plan.h"
#include "planner/seq_scan_plan.h"
#include "planner/order_by_plan.h"

#include "catalog/manager.h"

#include <memory>

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Optimizer
//===--------------------------------------------------------------------===//
Optimizer::Optimizer() {
  rules.emplace_back(new InnerJoinCommutativity());
  rules.emplace_back(new GetToScan());
  rules.emplace_back(new SelectToFilter());
  rules.emplace_back(new ProjectToComputeExprs());
  rules.emplace_back(new InnerJoinToInnerNLJoin());
  rules.emplace_back(new LeftJoinToLeftNLJoin());
  rules.emplace_back(new RightJoinToRightNLJoin());
  rules.emplace_back(new OuterJoinToOuterNLJoin());
  //rules.emplace_back(new InnerJoinToInnerHashJoin());
}

Optimizer &Optimizer::GetInstance() {
  thread_local static Optimizer optimizer;
  return optimizer;
}

std::shared_ptr<planner::AbstractPlan> Optimizer::GeneratePlan(
  std::shared_ptr<Select> select_tree)
{
  // Generate initial operator tree from query tree
  std::shared_ptr<GroupExpression> gexpr = InsertQueryTree(select_tree);
  GroupID root_id = gexpr->GetGroupID();

  // Get the physical properties the final plan must output
  PropertySet properties = GetQueryTreeRequiredProperties(select_tree);

  // Find least cost plan for root group
  OptimizeExpression(gexpr, properties);

  std::shared_ptr<OpExpression> best_plan = ChooseBestPlan(root_id, properties);

  if (best_plan == nullptr) return nullptr;

  planner::AbstractPlan* top_plan = OptimizerPlanToPlannerPlan(best_plan);

  std::shared_ptr<planner::AbstractPlan> final_plan(top_plan);

  return final_plan;
}

std::shared_ptr<GroupExpression> Optimizer::InsertQueryTree(
  std::shared_ptr<Select> tree)
{
  std::shared_ptr<OpExpression> initial =
    ConvertQueryToOpExpression(column_manager, tree);
  std::shared_ptr<GroupExpression> gexpr;
  assert(RecordTransformedExpression(initial, gexpr));
  return gexpr;
}

PropertySet Optimizer::GetQueryTreeRequiredProperties(
  std::shared_ptr<Select> tree)
{
  (void) tree;
  return PropertySet();
}

planner::AbstractPlan *Optimizer::OptimizerPlanToPlannerPlan(
  std::shared_ptr<OpExpression> plan)
{
  return ConvertOpExpressionToPlan(plan);
}

std::shared_ptr<OpExpression> Optimizer::ChooseBestPlan(
  GroupID id,
  PropertySet requirements)
{
  LOG_TRACE("Choosing best plan for group %d", id);

  Group *group = memo.GetGroupByID(id);
  std::shared_ptr<GroupExpression> gexpr =
    group->GetBestExpression(requirements);

  std::vector<GroupID> child_groups = gexpr->GetChildGroupIDs();
  std::vector<PropertySet> required_input_props =
    gexpr->Op().RequiredInputProperties();
  if (required_input_props.empty()) {
    required_input_props.resize(child_groups.size(), PropertySet());
  }

  std::shared_ptr<OpExpression> op =
    std::make_shared<OpExpression>(gexpr->Op());

  for (size_t i = 0; i < child_groups.size(); ++i) {
    std::shared_ptr<OpExpression> child_op =
      ChooseBestPlan(child_groups[i], required_input_props[i]);
    op->PushChild(child_op);
  }

  return op;
}

void Optimizer::OptimizeGroup(GroupID id, PropertySet requirements) {
  LOG_TRACE("Optimizing group %d", id);
  Group *group = memo.GetGroupByID(id);
  const std::vector<std::shared_ptr<GroupExpression>> exprs =
    group->GetExpressions();
  for (size_t i = 0; i < exprs.size(); ++i) {
    OptimizeExpression(exprs[i], requirements);
  }
}

void Optimizer::OptimizeExpression(std::shared_ptr<GroupExpression> gexpr,
                                   PropertySet requirements)
{
  LOG_TRACE("Optimizing expression of group %d with op %s",
            gexpr->GetGroupID(), gexpr->Op().name().c_str());

  // Helper function for costing follow up expressions
  auto CostCandidate =
    [this](std::shared_ptr<GroupExpression> candidate,
           PropertySet requirements)
    {
      // Cost the expression
      this->CostExpression(candidate);

      // Only include cost if it meets the property requirements
      if (requirements.IsSubset(candidate->Op().ProvidedOutputProperties())) {
        // Add to group as potential best cost
        Group *group = this->memo.GetGroupByID(candidate->GetGroupID());
        LOG_TRACE("Adding expression cost on group %d with op %s",
                  candidate->GetGroupID(), candidate->Op().name().c_str());
        group->SetExpressionCost(candidate,
                                 candidate->GetCost(),
                                 requirements);
      }
    };

  // Cost the root expression
  if (gexpr->Op().IsPhysical()) {
    CostCandidate(gexpr, requirements);
    // Transformation rules shouldn't match on physical operators so don't apply
    // rules
  } else {
    // Apply transformations and cost those which match the requirements
    for (const std::unique_ptr<Rule> &rule : rules) {
      // Apply all rules to operator which match. We apply all rules to one
      // operator before moving on to the next operator in the group because
      // then we avoid missing the application of a rule e.g. an application
      // of some rule creates a match for a previously applied rule, but it is
      // missed because the prev rule was already checked
      std::vector<std::shared_ptr<GroupExpression>> candidates =
        TransformExpression(gexpr, *(rule.get()));

      for (std::shared_ptr<GroupExpression> candidate : candidates) {
        // If logical...
        if (candidate->Op().IsLogical()) {
          // Optimize the expression
          OptimizeExpression(candidate, requirements);
        }
        if (candidate->Op().IsPhysical()) {
          CostCandidate(candidate, requirements);
        }
      }
    }
  }
}

void Optimizer::CostExpression(std::shared_ptr<GroupExpression> gexpr) {
  LOG_TRACE("Costing expression of group %d with op %s",
            gexpr->GetGroupID(), gexpr->Op().name().c_str());
  // Get required properties of children
  std::vector<PropertySet> required_child_properties =
    gexpr->Op().RequiredInputProperties();
  std::vector<GroupID> child_group_ids = gexpr->GetChildGroupIDs();

  if (required_child_properties.empty()) {
    required_child_properties.resize(child_group_ids.size(), PropertySet());
  }

  std::vector<std::shared_ptr<GroupExpression>> best_child_expressions;
  std::vector<std::shared_ptr<Stats>> best_child_stats;
  std::vector<double> best_child_costs;
  for (size_t i = 0; i < child_group_ids.size(); ++i) {
    GroupID child_group_id = child_group_ids[i];
    const PropertySet &input_properties = required_child_properties[i];
    // Optimize child
    OptimizeGroup(child_group_id, input_properties);
    // Find best child expression
    std::shared_ptr<GroupExpression> best_expression =
      memo.GetGroupByID(child_group_id)->GetBestExpression(input_properties);
    // TODO(abpoms): we should allow for failure in the case where no expression
    // can provide the required properties
    assert(best_expression != nullptr);
    best_child_expressions.push_back(best_expression);
    best_child_stats.push_back(best_expression->GetStats());
    best_child_costs.push_back(best_expression->GetCost());
  }

  // Perform costing
  gexpr->DeriveStatsAndCost(best_child_stats, best_child_costs);
}

void Optimizer::ExploreGroup(GroupID id) {
  LOG_TRACE("Exploring group %d", id);
  for (std::shared_ptr<GroupExpression> gexpr :
         memo.GetGroupByID(id)->GetExpressions())
  {
    ExploreExpression(gexpr);
  }
}

void Optimizer::ExploreExpression(std::shared_ptr<GroupExpression> gexpr) {
  LOG_TRACE("Exploring expression of group %d with op %s",
            gexpr->GetGroupID(), gexpr->Op().name().c_str());

  for (const std::unique_ptr<Rule> &rule : rules) {
    // See comment in OptimizeExpression
    std::vector<std::shared_ptr<GroupExpression>> candidates =
      TransformExpression(gexpr, *(rule.get()));

    for (std::shared_ptr<GroupExpression> candidate : candidates) {
      // If logical...
      if (candidate->Op().IsLogical()) {
        // Explore the expression
        ExploreExpression(candidate);
      }
    }
  }
}

//////////////////////////////////////////////////////////////////////////////
/// Rule application
std::vector<std::shared_ptr<GroupExpression>>
Optimizer::TransformExpression(std::shared_ptr<GroupExpression> gexpr,
                               const Rule &rule)
{
  std::shared_ptr<Pattern> pattern = rule.GetMatchPattern();

  std::vector<std::shared_ptr<GroupExpression>> output_plans;
  ItemBindingIterator iterator(*this, gexpr, pattern);
  while (iterator.HasNext()) {
    std::shared_ptr<OpExpression> plan = iterator.Next();
    // Check rule condition function
    if (rule.Check(plan)) {
      LOG_TRACE("Rule matched expression of group %d with op %s",
                gexpr->GetGroupID(), gexpr->Op().name().c_str());
      // Apply rule transformations
      // We need to be able to analyze the transformations performed by this
      // rule in order to perform deduplication and launch an exploration of
      // the newly applied rule
      std::vector<std::shared_ptr<OpExpression>> transformed_plans;
      rule.Transform(plan, transformed_plans);

      // Integrate transformed plans back into groups and explore/cost if new
      for (std::shared_ptr<OpExpression> plan : transformed_plans) {
        LOG_TRACE("Trying to integrate expression with op %s",
                  plan->Op().name().c_str());
        std::shared_ptr<GroupExpression> new_gexpr;
        bool new_expression =
          RecordTransformedExpression(plan, new_gexpr, gexpr->GetGroupID());
        if (new_expression) {
          LOG_TRACE("Expression with op %s was inserted into group %d",
                    plan->Op().name().c_str(),
                    new_gexpr->GetGroupID());
          output_plans.push_back(new_gexpr);
        }
      }
    }
  }
  return output_plans;
}

//////////////////////////////////////////////////////////////////////////////
/// Memo insertion
std::shared_ptr<GroupExpression> Optimizer::MakeGroupExpression(
  std::shared_ptr<OpExpression> expr)
{
  std::vector<GroupID> child_groups = MemoTransformedChildren(expr);
  return std::make_shared<GroupExpression>(expr->Op(), child_groups);
}

std::vector<GroupID> Optimizer::MemoTransformedChildren(
  std::shared_ptr<OpExpression> expr)
{
  std::vector<GroupID> child_groups;
  for (std::shared_ptr<OpExpression> child : expr->Children()) {
    child_groups.push_back(MemoTransformedExpression(child));
  }

  return child_groups;
}

GroupID Optimizer::MemoTransformedExpression(
  std::shared_ptr<OpExpression> expr)
{
  std::shared_ptr<GroupExpression> gexpr = MakeGroupExpression(expr);
  // Ignore whether this expression is new or not as we only care about that
  // at the top level
  (void) memo.InsertExpression(gexpr);
  return gexpr->GetGroupID();
}

bool Optimizer::RecordTransformedExpression(
  std::shared_ptr<OpExpression> expr,
  std::shared_ptr<GroupExpression> &gexpr)
{
  return RecordTransformedExpression(expr, gexpr, UNDEFINED_GROUP);
}

bool Optimizer::RecordTransformedExpression(
  std::shared_ptr<OpExpression> expr,
  std::shared_ptr<GroupExpression> &gexpr,
  GroupID target_group)
{
  gexpr = MakeGroupExpression(expr);
  return memo.InsertExpression(gexpr, target_group);
}

}  // namespace optimizer
}  // namespace peloton
