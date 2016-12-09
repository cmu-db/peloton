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

#include <memory>

#include "catalog/manager.h"

#include "optimizer/child_property_generator.h"
#include "optimizer/operator_to_plan_transformer.h"
#include "optimizer/operator_visitor.h"
#include "optimizer/optimizer.h"
#include "optimizer/property_enforcer.h"
#include "optimizer/query_property_extractor.h"
#include "optimizer/query_to_operator_transformer.h"
#include "optimizer/rule_impls.h"

#include "parser/sql_statement.h"

#include "planner/order_by_plan.h"
#include "planner/projection_plan.h"
#include "planner/seq_scan_plan.h"

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Optimizer
//===--------------------------------------------------------------------===//
Optimizer::Optimizer() {
  logical_transformation_rules_.emplace_back(new InnerJoinCommutativity());
  physical_implementation_rules_.emplace_back(new GetToScan());
  physical_implementation_rules_.emplace_back(new LogicalFilterToPhysical());
  physical_implementation_rules_.emplace_back(new ProjectToComputeExprs());
  physical_implementation_rules_.emplace_back(new InnerJoinToInnerNLJoin());
  physical_implementation_rules_.emplace_back(new LeftJoinToLeftNLJoin());
  physical_implementation_rules_.emplace_back(new RightJoinToRightNLJoin());
  physical_implementation_rules_.emplace_back(new OuterJoinToOuterNLJoin());
  // rules.emplace_back(new InnerJoinToInnerHashJoin());
}

Optimizer &Optimizer::GetInstance() {
  thread_local static Optimizer optimizer;
  return optimizer;
}

std::shared_ptr<planner::AbstractPlan> Optimizer::BuildPelotonPlanTree(
    const std::unique_ptr<parser::SQLStatementList> &parse_tree_list) {
  // Base Case
  if (parse_tree_list->GetStatements().size() == 0) return nullptr;

  std::unique_ptr<planner::AbstractPlan> child_plan = nullptr;

  auto parse_tree = parse_tree_list->GetStatements().at(0);

  // Generate initial operator tree from query tree
  std::shared_ptr<GroupExpression> gexpr = InsertQueryTree(parse_tree);
  GroupID root_id = gexpr->GetGroupID();

  // Get the physical properties the final plan must output
  PropertySet properties = GetQueryRequiredProperties(parse_tree);

  // Explore the logically equivalent plans from the root group
  ExploreGroup(root_id);

  // Implement all the physical operators
  ImplementGroup(root_id);

  // Find least cost plan for root group
  OptimizeExpression(gexpr, properties);

  std::shared_ptr<OperatorExpression> best_plan =
      ChooseBestPlan(root_id, properties);

  if (best_plan == nullptr) return nullptr;

  planner::AbstractPlan *top_plan = OptimizerPlanToPlannerPlan(best_plan);

  std::shared_ptr<planner::AbstractPlan> final_plan(top_plan);

  return final_plan;
}

std::shared_ptr<GroupExpression> Optimizer::InsertQueryTree(
    parser::SQLStatement *tree) {
  QueryToOperatorTransformer converter(column_manager_);
  std::shared_ptr<OperatorExpression> initial =
      converter.ConvertToOpExpression(tree);
  std::shared_ptr<GroupExpression> gexpr;
  assert(RecordTransformedExpression(initial, gexpr));
  return gexpr;
}

PropertySet Optimizer::GetQueryRequiredProperties(parser::SQLStatement *tree) {
  QueryPropertyExtractor converter(column_manager_);
  return std::move(converter.GetProperties(tree));
}

planner::AbstractPlan *Optimizer::OptimizerPlanToPlannerPlan(
    std::shared_ptr<OperatorExpression> plan) {
  OperatorToPlanTransformer transformer;
  return transformer.ConvertOpExpression(plan);
}

std::shared_ptr<OperatorExpression> Optimizer::ChooseBestPlan(
    GroupID id, PropertySet requirements) {
  LOG_TRACE("Choosing best plan for group %d", id);

  Group *group = memo_.GetGroupByID(id);
  std::shared_ptr<GroupExpression> gexpr =
      group->GetBestExpression(requirements);

  std::vector<GroupID> child_groups = gexpr->GetChildGroupIDs();
  std::vector<PropertySet> required_input_props =
      gexpr->Op().RequiredInputProperties();
  if (required_input_props.empty()) {
    required_input_props.resize(child_groups.size(), PropertySet());
  }

  std::shared_ptr<OperatorExpression> op =
      std::make_shared<OperatorExpression>(gexpr->Op());

  for (size_t i = 0; i < child_groups.size(); ++i) {
    std::shared_ptr<OperatorExpression> child_op =
        ChooseBestPlan(child_groups[i], required_input_props[i]);
    op->PushChild(child_op);
  }

  return op;
}

void Optimizer::OptimizeGroup(GroupID id, PropertySet requirements) {
  LOG_TRACE("Optimizing group %d", id);
  Group *group = memo_.GetGroupByID(id);

  // Whether required properties have already been optimized for the group
  if (group->GetBestExpression(requirements) != nullptr) return;

  const std::vector<std::shared_ptr<GroupExpression>> exprs =
      group->GetExpressions();
  for (size_t i = 0; i < exprs.size(); ++i) {
    if (exprs[i]->Op().IsPhysical()) OptimizeExpression(exprs[i], requirements);
  }
}

void Optimizer::OptimizeExpression(std::shared_ptr<GroupExpression> gexpr,
                                   PropertySet requirements) {
  LOG_TRACE("Optimizing expression of group %d with op %s", gexpr->GetGroupID(),
            gexpr->Op().name().c_str());

  // Only optimize and cost physical expression
  PL_ASSERT(gexpr->Op().IsPhysical());

  std::vector<std::pair<PropertySet, std::vector<PropertySet>>>
      output_input_property_pairs =
          std::move(DeriveChildProperties(gexpr, requirements));

  size_t num_property_pairs = output_input_property_pairs.size();

  auto child_group_ids = gexpr->GetChildGroupIDs();

  for (size_t pair_offset = 0; pair_offset < num_property_pairs;
       ++pair_offset) {
    auto output_properties = output_input_property_pairs[pair_offset].first;
    const auto &input_properties_list =
        output_input_property_pairs[pair_offset].second;

    std::vector<std::shared_ptr<Stats>> best_child_stats;
    std::vector<double> best_child_costs;
    for (size_t i = 0; i < child_group_ids.size(); ++i) {
      GroupID child_group_id = child_group_ids[i];
      const PropertySet &input_properties = input_properties_list[i];
      // Optimize child
      OptimizeGroup(child_group_id, input_properties);

      // Find best child expression
      std::shared_ptr<GroupExpression> best_expression =
          memo_.GetGroupByID(child_group_id)
              ->GetBestExpression(input_properties);
      // TODO(abpoms): we should allow for failure in the case where no
      // expression
      // can provide the required properties
      PL_ASSERT(best_expression != nullptr);
      best_child_stats.push_back(best_expression->GetStats(input_properties));
      best_child_costs.push_back(best_expression->GetCost(input_properties));
    }

    // Perform costing
    gexpr->DeriveStatsAndCost(output_properties, input_properties_list,
                              best_child_stats, best_child_costs);

    Group *group = this->memo_.GetGroupByID(gexpr->GetGroupID());
    // Add to group as potential best cost
    group->SetExpressionCost(gexpr, gexpr->GetCost(output_properties),
                             output_properties);

    // enforce missing properties
    for (auto property : requirements.Properties()) {
      if (output_properties.HasProperty(*property) == false) {
        gexpr = EnforceProperty(gexpr, output_properties, property);
        group->SetExpressionCost(gexpr, gexpr->GetCost(output_properties),
                                 output_properties);
      }
    }

    // After the enforcement it must have met the property requirements, so
    // notice here we set the best cost plan for 'requirements' instead of
    // 'output_properties'
    group->SetExpressionCost(gexpr, gexpr->GetCost(output_properties),
                             requirements);
  }
}

std::shared_ptr<GroupExpression> Optimizer::EnforceProperty(
    std::shared_ptr<GroupExpression> gexpr, PropertySet &output_properties,
    const std::shared_ptr<Property> property) {
  // new child input is the old output
  auto child_input_properties = std::vector<PropertySet>();
  child_input_properties.push_back(output_properties);

  auto child_stats = std::vector<std::shared_ptr<Stats>>();
  child_stats.push_back(gexpr->GetStats(output_properties));
  auto child_costs = std::vector<double>();
  child_costs.push_back(gexpr->GetCost(output_properties));

  PropertyEnforcer enforcer(column_manager_);
  auto enforced_gexpr =
      enforcer.EnforceProperty(gexpr, &output_properties, property);

  // new output property would have the enforced Property
  output_properties.AddProperty(std::shared_ptr<Property>(property));

  enforced_gexpr->DeriveStatsAndCost(output_properties, child_input_properties,
                                     child_stats, child_costs);
  return enforced_gexpr;
}

std::vector<std::pair<PropertySet, std::vector<PropertySet>>>
Optimizer::DeriveChildProperties(std::shared_ptr<GroupExpression> gexpr,
                                 PropertySet requirements) {
  ChildPropertyGenerator converter(column_manager_);
  return std::move(converter.GetProperties(gexpr, requirements));
}

void Optimizer::ExploreGroup(GroupID id) {
  LOG_TRACE("Exploring group %d", id);
  for (std::shared_ptr<GroupExpression> gexpr :
       memo_.GetGroupByID(id)->GetExpressions()) {
    ExploreExpression(gexpr);
  }
  memo_.GetGroupByID(id)->SetExplorationFlag();
}

void Optimizer::ExploreExpression(std::shared_ptr<GroupExpression> gexpr) {
  LOG_TRACE("Exploring expression of group %d with op %s", gexpr->GetGroupID(),
            gexpr->Op().name().c_str());

  PL_ASSERT(gexpr->Op().IsLogical());

  // Explore logically equivalent plans by applying transformation rules
  for (const std::unique_ptr<Rule> &rule : logical_transformation_rules_) {
    // Apply all rules to operator which match. We apply all rules to one
    // operator before moving on to the next operator in the group because
    // then we avoid missing the application of a rule e.g. an application
    // of some rule creates a match for a previously applied rule, but it is
    // missed because the prev rule was already checked
    std::vector<std::shared_ptr<GroupExpression>> candidates =
        TransformExpression(gexpr, *(rule.get()));

    for (std::shared_ptr<GroupExpression> candidate : candidates) {
      // Explore the expression
      ExploreExpression(candidate);
    }
  }

  // Explore child groups
  for (auto child_id : gexpr->GetChildGroupIDs()) {
    if (!memo_.GetGroupByID(child_id)->HasExplored()) ExploreGroup(child_id);
  }
}

void Optimizer::ImplementGroup(GroupID id) {
  LOG_TRACE("Implementing group %d", id);
  for (std::shared_ptr<GroupExpression> gexpr :
       memo_.GetGroupByID(id)->GetExpressions()) {
    if (gexpr->Op().IsPhysical()) ExploreExpression(gexpr);
  }
  memo_.GetGroupByID(id)->SetImplementationFlag();
}

void Optimizer::ImplementExpression(std::shared_ptr<GroupExpression> gexpr) {
  LOG_TRACE("Implementing expression of group %d with op %s",
            gexpr->GetGroupID(), gexpr->Op().name().c_str());

  // Explore implement physical expressions
  for (const std::unique_ptr<Rule> &rule : physical_implementation_rules_) {
    TransformExpression(gexpr, *(rule.get()));
  }

  // Explore child groups
  for (auto child_id : gexpr->GetChildGroupIDs()) {
    if (!memo_.GetGroupByID(child_id)->HasImplemented())
      ImplementGroup(child_id);
  }
}

//////////////////////////////////////////////////////////////////////////////
/// Rule application
std::vector<std::shared_ptr<GroupExpression>> Optimizer::TransformExpression(
    std::shared_ptr<GroupExpression> gexpr, const Rule &rule) {
  std::shared_ptr<Pattern> pattern = rule.GetMatchPattern();

  std::vector<std::shared_ptr<GroupExpression>> output_plans;
  ItemBindingIterator iterator(*this, gexpr, pattern);
  while (iterator.HasNext()) {
    std::shared_ptr<OperatorExpression> plan = iterator.Next();
    // Check rule condition function
    if (rule.Check(plan)) {
      LOG_TRACE("Rule matched expression of group %d with op %s",
                gexpr->GetGroupID(), gexpr->Op().name().c_str());
      // Apply rule transformations
      // We need to be able to analyze the transformations performed by this
      // rule in order to perform deduplication and launch an exploration of
      // the newly applied rule
      std::vector<std::shared_ptr<OperatorExpression>> transformed_plans;
      rule.Transform(plan, transformed_plans);

      // Integrate transformed plans back into groups and explore/cost if new
      for (std::shared_ptr<OperatorExpression> plan : transformed_plans) {
        LOG_TRACE("Trying to integrate expression with op %s",
                  plan->Op().name().c_str());
        std::shared_ptr<GroupExpression> new_gexpr;
        bool new_expression =
            RecordTransformedExpression(plan, new_gexpr, gexpr->GetGroupID());
        if (new_expression) {
          LOG_TRACE("Expression with op %s was inserted into group %d",
                    plan->Op().name().c_str(), new_gexpr->GetGroupID());
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
    std::shared_ptr<OperatorExpression> expr) {
  std::vector<GroupID> child_groups = MemoTransformedChildren(expr);
  return std::make_shared<GroupExpression>(expr->Op(), child_groups);
}

std::vector<GroupID> Optimizer::MemoTransformedChildren(
    std::shared_ptr<OperatorExpression> expr) {
  std::vector<GroupID> child_groups;
  for (std::shared_ptr<OperatorExpression> child : expr->Children()) {
    child_groups.push_back(MemoTransformedExpression(child));
  }

  return child_groups;
}

GroupID Optimizer::MemoTransformedExpression(
    std::shared_ptr<OperatorExpression> expr) {
  std::shared_ptr<GroupExpression> gexpr = MakeGroupExpression(expr);
  // Ignore whether this expression is new or not as we only care about that
  // at the top level
  (void)memo_.InsertExpression(gexpr);
  return gexpr->GetGroupID();
}

bool Optimizer::RecordTransformedExpression(
    std::shared_ptr<OperatorExpression> expr,
    std::shared_ptr<GroupExpression> &gexpr) {
  return RecordTransformedExpression(expr, gexpr, UNDEFINED_GROUP);
}

bool Optimizer::RecordTransformedExpression(
    std::shared_ptr<OperatorExpression> expr,
    std::shared_ptr<GroupExpression> &gexpr, GroupID target_group) {
  gexpr = MakeGroupExpression(expr);
  return memo_.InsertExpression(gexpr, target_group);
}

}  // namespace optimizer
}  // namespace peloton
