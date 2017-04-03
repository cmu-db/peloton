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

#include "optimizer/binding.h"
#include "optimizer/child_property_generator.h"
#include "optimizer/cost_and_stats_calculator.h"
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
#include "planner/create_plan.h"
#include "planner/drop_plan.h"
#include "binder/bind_node_visitor.h"

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Optimizer
//===--------------------------------------------------------------------===//
Optimizer::Optimizer() {
  logical_transformation_rules_.emplace_back(new InnerJoinCommutativity());
  physical_implementation_rules_.emplace_back(new LogicalLimitToPhysical());
  physical_implementation_rules_.emplace_back(new LogicalDeleteToPhysical());
  physical_implementation_rules_.emplace_back(new LogicalUpdateToPhysical());
  physical_implementation_rules_.emplace_back(new LogicalInsertToPhysical());
  physical_implementation_rules_.emplace_back(new GetToScan());
  physical_implementation_rules_.emplace_back(new LogicalFilterToPhysical());
  physical_implementation_rules_.emplace_back(new InnerJoinToInnerNLJoin());
  physical_implementation_rules_.emplace_back(new LeftJoinToLeftNLJoin());
  physical_implementation_rules_.emplace_back(new RightJoinToRightNLJoin());
  physical_implementation_rules_.emplace_back(new OuterJoinToOuterNLJoin());
  // rules.emplace_back(new InnerJoinToInnerHashJoin());
}

std::shared_ptr<planner::AbstractPlan> Optimizer::BuildPelotonPlanTree(
    const std::unique_ptr<parser::SQLStatementList> &parse_tree_list) {

  LOG_DEBUG("Enter new optimizer...");

  // Base Case
  if (parse_tree_list->GetStatements().size() == 0) return nullptr;

  std::unique_ptr<planner::AbstractPlan> child_plan = nullptr;

  auto parse_tree = parse_tree_list->GetStatements().at(0);

  // Handle ddl statement
  bool is_ddl_stmt;
  auto ddl_plan = HandleDDLStatement(parse_tree, is_ddl_stmt);
  if (is_ddl_stmt) {
    return std::move(ddl_plan);
  }

  // Run binder
  auto bind_node_visitor = std::make_shared<binder::BindNodeVisitor>();
  bind_node_visitor->BindNameToNode(parse_tree);

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
  OptimizeGroup(root_id, properties);

  auto best_plan = ChooseBestPlan(root_id, properties);

  if (best_plan == nullptr) return nullptr;

  // Reset memo after finishing the optimization
  Reset();

  LOG_DEBUG("Exit new optimizer...");

  //  return std::shared_ptr<planner::AbstractPlan>(best_plan.release());
  return std::move(best_plan);
}

void Optimizer::Reset() {
  memo_ = std::move(Memo());
  column_manager_ = std::move(ColumnManager());
}

std::unique_ptr<planner::AbstractPlan> Optimizer::HandleDDLStatement(
    parser::SQLStatement *tree, bool& is_ddl_stmt) {
  std::unique_ptr<planner::AbstractPlan> ddl_plan;
  is_ddl_stmt = true;
  auto stmt_type = tree->GetType();
  switch (stmt_type) {
    case StatementType::DROP: {
      LOG_TRACE("Adding Drop plan...");
      std::unique_ptr<planner::AbstractPlan> drop_plan(
          new planner::DropPlan((parser::DropStatement *) tree));
      ddl_plan = std::move(drop_plan);
    }
      break;

    case StatementType::CREATE: {
      LOG_TRACE("Adding Create plan...");
      std::unique_ptr<planner::AbstractPlan> create_plan(
          new planner::CreatePlan((parser::CreateStatement *) tree));
      ddl_plan = std::move(create_plan);
    }
      break;
    default:is_ddl_stmt = false;
  }

  return std::move(ddl_plan);

}

std::shared_ptr<GroupExpression> Optimizer::InsertQueryTree(
    parser::SQLStatement *tree) {
  QueryToOperatorTransformer converter(column_manager_);
  std::shared_ptr<OperatorExpression> initial =
      converter.ConvertToOpExpression(tree);
  std::shared_ptr<GroupExpression> gexpr;
  RecordTransformedExpression(initial, gexpr);
  return gexpr;
}

PropertySet Optimizer::GetQueryRequiredProperties(parser::SQLStatement *tree) {
  QueryPropertyExtractor converter(column_manager_);
  return std::move(converter.GetProperties(tree));
}

std::unique_ptr<planner::AbstractPlan> Optimizer::OptimizerPlanToPlannerPlan(
    std::shared_ptr<OperatorExpression> plan, PropertySet &requirements,
    std::vector<PropertySet> &required_input_props,
    std::vector<std::unique_ptr<planner::AbstractPlan>> &children_plans,
    std::vector<std::vector<std::tuple<oid_t, oid_t, oid_t>>> &
        children_output_columns,
    std::vector<std::tuple<oid_t, oid_t, oid_t>> *output_columns) {
  OperatorToPlanTransformer transformer;
  return transformer.ConvertOpExpression(
      plan, &requirements, &required_input_props, children_plans,
      children_output_columns, output_columns);
}

std::unique_ptr<planner::AbstractPlan> Optimizer::ChooseBestPlan(
    GroupID id, PropertySet requirements,
    std::vector<std::tuple<oid_t, oid_t, oid_t>> *output_columns) {
  LOG_TRACE("Choosing best plan for group %d", id);

  Group *group = memo_.GetGroupByID(id);
  std::shared_ptr<GroupExpression> gexpr =
      group->GetBestExpression(requirements);

  LOG_TRACE("Choosing best plan for group %d with op %s", gexpr->GetGroupID(),
            gexpr->Op().name().c_str());

  std::vector<GroupID> child_groups = gexpr->GetChildGroupIDs();
  std::vector<PropertySet> required_input_props =
      std::move(gexpr->GetInputProperties(requirements));
  PL_ASSERT(required_input_props.size() == child_groups.size());

  // Derive chidren plans first
  // because they may be useful
  // in the derivation of root plan
  // Also keep propagate global column id
  // for column id to column offset mapping
  std::vector<std::unique_ptr<planner::AbstractPlan>> children_plans;
  std::vector<std::vector<std::tuple<oid_t, oid_t, oid_t>>>
      children_output_columns;
  for (size_t i = 0; i < child_groups.size(); ++i) {
    std::vector<std::tuple<oid_t, oid_t, oid_t>> child_output_columns = {};
    children_plans.push_back(ChooseBestPlan(
        child_groups[i], required_input_props[i], &child_output_columns));
    children_output_columns.push_back(std::move(child_output_columns));
  }

  // Derive root plan
  std::shared_ptr<OperatorExpression> op =
      std::make_shared<OperatorExpression>(gexpr->Op());

  auto plan = OptimizerPlanToPlannerPlan(
      op, requirements, required_input_props, children_plans,
      children_output_columns, output_columns);

  return plan;
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
    DeriveCostAndStats(gexpr, output_properties, input_properties_list,
                       best_child_stats, best_child_costs);

    Group *group = this->memo_.GetGroupByID(gexpr->GetGroupID());
    // Add to group as potential best cost
    group->SetExpressionCost(gexpr, gexpr->GetCost(output_properties),
                             output_properties);

    // enforce missing properties
    for (auto property : requirements.Properties()) {
      if (output_properties.HasProperty(*property) == false) {
        gexpr =
            EnforceProperty(gexpr, output_properties, property, requirements);
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
    const std::shared_ptr<Property> property, PropertySet &requirements) {
  // new child input is the old output
  auto child_input_properties = std::vector<PropertySet>();
  child_input_properties.push_back(output_properties);

  auto child_stats = std::vector<std::shared_ptr<Stats>>();
  child_stats.push_back(gexpr->GetStats(output_properties));
  auto child_costs = std::vector<double>();
  child_costs.push_back(gexpr->GetCost(output_properties));

  // if (property->Type() == PropertyType::SORT) {
  //  LOG_DEBUG("Enforcing Sort");
  //}

  PropertyEnforcer enforcer(column_manager_);
  auto enforced_expr =
      enforcer.EnforceProperty(gexpr, &output_properties, property);

  std::shared_ptr<GroupExpression> enforced_gexpr;
  RecordTransformedExpression(enforced_expr, enforced_gexpr,
                              gexpr->GetGroupID());
  // LOG_DEBUG("Leaving Enforce");
  // new output property would have the enforced Property
  output_properties.AddProperty(std::shared_ptr<Property>(property));

  DeriveCostAndStats(enforced_gexpr, output_properties, child_input_properties,
                     child_stats, child_costs);

  // If requirements is fulfilled
  // set cost and stats for it
  if (output_properties >= requirements) {
    DeriveCostAndStats(enforced_gexpr, requirements, child_input_properties,
                       child_stats, child_costs);
  }

  return enforced_gexpr;
}

std::vector<std::pair<PropertySet, std::vector<PropertySet>>>
Optimizer::DeriveChildProperties(std::shared_ptr<GroupExpression> gexpr,
                                 PropertySet requirements) {
  ChildPropertyGenerator converter(column_manager_);
  return std::move(converter.GetProperties(gexpr, requirements));
}

void Optimizer::DeriveCostAndStats(
    std::shared_ptr<GroupExpression> gexpr,
    const PropertySet &output_properties,
    const std::vector<PropertySet> &input_properties_list,
    std::vector<std::shared_ptr<Stats>> child_stats,
    std::vector<double> child_costs) {
  CostAndStatsCalculator calculator(column_manager_);
  calculator.CalculateCostAndStats(gexpr, &output_properties,
                                   &input_properties_list, child_stats,
                                   child_costs);
  gexpr->SetLocalHashTable(output_properties, input_properties_list,
                           calculator.GetOutputCost(),
                           calculator.GetOutputStats());
}

void Optimizer::ExploreGroup(GroupID id) {
  LOG_TRACE("Exploring group %d", id);
  if (memo_.GetGroupByID(id)->HasExplored()) return;

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
  if (memo_.GetGroupByID(id)->HasImplemented()) return;

  for (std::shared_ptr<GroupExpression> gexpr :
       memo_.GetGroupByID(id)->GetExpressions()) {
    if (gexpr->Op().IsLogical()) ImplementExpression(gexpr);
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
