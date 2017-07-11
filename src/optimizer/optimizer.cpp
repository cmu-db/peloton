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

#include "optimizer/optimizer.h"

#include "catalog/manager.h"

#include "parser/create_statement.h"
#include "optimizer/binding.h"
#include "optimizer/child_property_generator.h"
#include "optimizer/cost_and_stats_calculator.h"
#include "optimizer/operator_to_plan_transformer.h"
#include "optimizer/operator_visitor.h"
#include "optimizer/property_enforcer.h"
#include "optimizer/query_property_extractor.h"
#include "optimizer/query_to_operator_transformer.h"
#include "optimizer/rule_impls.h"
#include "optimizer/properties.h"

#include "planner/order_by_plan.h"
#include "planner/projection_plan.h"
#include "planner/seq_scan_plan.h"
#include "planner/create_plan.h"
#include "planner/drop_plan.h"
#include "planner/populate_index_plan.h"
#include "planner/analyze_plan.h"

#include "binder/bind_node_visitor.h"

using std::vector;
using std::unordered_map;
using std::shared_ptr;
using std::unique_ptr;
using std::move;
using std::pair;
using std::make_shared;

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Optimizer
//===--------------------------------------------------------------------===//
Optimizer::Optimizer() {
  //  logical_transformation_rules_.emplace_back(new InnerJoinCommutativity());
  physical_implementation_rules_.emplace_back(new LogicalDeleteToPhysical());
  physical_implementation_rules_.emplace_back(new LogicalUpdateToPhysical());
  physical_implementation_rules_.emplace_back(new LogicalInsertToPhysical());
  physical_implementation_rules_.emplace_back(
      new LogicalGroupByToHashGroupBy());
  physical_implementation_rules_.emplace_back(
      new LogicalGroupByToSortGroupBy());
  physical_implementation_rules_.emplace_back(new LogicalAggregateToPhysical());
  physical_implementation_rules_.emplace_back(new GetToDummyScan());
  physical_implementation_rules_.emplace_back(new GetToSeqScan());
  physical_implementation_rules_.emplace_back(new GetToIndexScan());
  physical_implementation_rules_.emplace_back(new LogicalFilterToPhysical());
  physical_implementation_rules_.emplace_back(new InnerJoinToInnerNLJoin());
  physical_implementation_rules_.emplace_back(new LeftJoinToLeftNLJoin());
  physical_implementation_rules_.emplace_back(new RightJoinToRightNLJoin());
  physical_implementation_rules_.emplace_back(new OuterJoinToOuterNLJoin());
  physical_implementation_rules_.emplace_back(new InnerJoinToInnerHashJoin());
}

shared_ptr<planner::AbstractPlan> Optimizer::BuildPelotonPlanTree(
    const unique_ptr<parser::SQLStatementList> &parse_tree_list) {
  // Base Case
  if (parse_tree_list->GetStatements().size() == 0) return nullptr;

  unique_ptr<planner::AbstractPlan> child_plan = nullptr;

  auto parse_tree = parse_tree_list->GetStatements().at(0);

  // Handle ddl statement
  bool is_ddl_stmt;
  auto ddl_plan = HandleDDLStatement(parse_tree, is_ddl_stmt);
  if (is_ddl_stmt) {
    return move(ddl_plan);
  }

  // Run binder
  auto bind_node_visitor = make_shared<binder::BindNodeVisitor>();
  bind_node_visitor->BindNameToNode(parse_tree);
  
  // Generate initial operator tree from query tree
  shared_ptr<GroupExpression> gexpr = InsertQueryTree(parse_tree);
  GroupID root_id = gexpr->GetGroupID();

  // Get the physical properties the final plan must output
  PropertySet properties = GetQueryRequiredProperties(parse_tree);

  // Explore the logically equivalent plans from the root group
  ExploreGroup(root_id);

  // Implement all the physical operators
  ImplementGroup(root_id);

  // Find least cost plan for root group
  OptimizeGroup(root_id, properties);

  ExprMap output_expr_map;
  auto best_plan = ChooseBestPlan(root_id, properties, &output_expr_map);
  if (best_plan == nullptr) return nullptr;

  // Reset memo after finishing the optimization
  Reset();
  
  //  return shared_ptr<planner::AbstractPlan>(best_plan.release());
  return move(best_plan);
}

void Optimizer::Reset() {
  memo_ = Memo();
  column_manager_ = move(ColumnManager());
}

unique_ptr<planner::AbstractPlan> Optimizer::HandleDDLStatement(
    parser::SQLStatement *tree, bool &is_ddl_stmt) {
  unique_ptr<planner::AbstractPlan> ddl_plan = nullptr;
  is_ddl_stmt = true;
  auto stmt_type = tree->GetType();
  switch (stmt_type) {
    case StatementType::DROP: {
      LOG_TRACE("Adding Drop plan...");
      unique_ptr<planner::AbstractPlan> drop_plan(
          new planner::DropPlan((parser::DropStatement *)tree));
      ddl_plan = move(drop_plan);
      break;
    } 

    case StatementType::CREATE: {
      LOG_TRACE("Adding Create plan...");

      // This is adapted from the simple optimizer
      auto create_plan =
          new planner::CreatePlan((parser::CreateStatement *)tree);
      std::unique_ptr<planner::AbstractPlan> child_CreatePlan(create_plan);
      ddl_plan = move(child_CreatePlan);

      if (create_plan->GetCreateType() == peloton::CreateType::INDEX) {
        auto create_stmt = (parser::CreateStatement *)tree;
        auto target_table = catalog::Catalog::GetInstance()->GetTableWithName(
            create_stmt->GetDatabaseName(), create_stmt->GetTableName());
        std::vector<oid_t> column_ids;
        auto schema = target_table->GetSchema();
        for (auto column_name : create_plan->GetIndexAttributes()) {
          column_ids.push_back(schema->GetColumnID(column_name));
        }
        // Create a plan to retrieve data
        std::unique_ptr<planner::SeqScanPlan> child_SeqScanPlan(
            new planner::SeqScanPlan(target_table, nullptr, column_ids, false));

        child_SeqScanPlan->AddChild(std::move(ddl_plan));
        ddl_plan = std::move(child_SeqScanPlan);
        // Create a plan to add data to index
        std::unique_ptr<planner::AbstractPlan> child_PopulateIndexPlan(
            new planner::PopulateIndexPlan(target_table, column_ids));
        child_PopulateIndexPlan->AddChild(std::move(ddl_plan));
        ddl_plan = std::move(child_PopulateIndexPlan);
      }
      break;
    }
    case StatementType::TRANSACTION: {
      break;
    }
    case StatementType::ANALYZE: {
      LOG_TRACE("Adding Analyze plan...");
      unique_ptr<planner::AbstractPlan> analyze_plan(
        new planner::AnalyzePlan((parser::AnalyzeStatement *)tree));
      ddl_plan = move(analyze_plan);
      break;
    }
    case StatementType::COPY: {
      LOG_TRACE("Adding Copy plan...");
      parser::CopyStatement *copy_parse_tree =
          static_cast<parser::CopyStatement *>(tree);
      ddl_plan = util::CreateCopyPlan(copy_parse_tree);
      break;
    }
    default:
      is_ddl_stmt = false;
  }

  return ddl_plan;
}

shared_ptr<GroupExpression> Optimizer::InsertQueryTree(
    parser::SQLStatement *tree) {
  QueryToOperatorTransformer converter;
  shared_ptr<OperatorExpression> initial =
      converter.ConvertToOpExpression(tree);
  shared_ptr<GroupExpression> gexpr;
  RecordTransformedExpression(initial, gexpr);
  return gexpr;
}

PropertySet Optimizer::GetQueryRequiredProperties(parser::SQLStatement *tree) {
  QueryPropertyExtractor converter(column_manager_);
  return converter.GetProperties(tree);
}

unique_ptr<planner::AbstractPlan> Optimizer::OptimizerPlanToPlannerPlan(
    shared_ptr<OperatorExpression> plan, PropertySet &requirements,
    vector<PropertySet> &required_input_props,
    vector<unique_ptr<planner::AbstractPlan>> &children_plans,
    vector<ExprMap> &children_expr_map, ExprMap *output_expr_map) {
  OperatorToPlanTransformer transformer;
  return transformer.ConvertOpExpression(plan, &requirements,
                                         &required_input_props, children_plans,
                                         children_expr_map, output_expr_map);
}

unique_ptr<planner::AbstractPlan> Optimizer::ChooseBestPlan(
    GroupID id, PropertySet requirements, ExprMap *output_expr_map) {
  Group *group = memo_.GetGroupByID(id);
  shared_ptr<GroupExpression> gexpr = group->GetBestExpression(requirements);

  LOG_TRACE("Choosing best plan for group %d with op %s", gexpr->GetGroupID(),
            gexpr->Op().name().c_str());

  vector<GroupID> child_groups = gexpr->GetChildGroupIDs();
  vector<PropertySet> required_input_props =
      gexpr->GetInputProperties(requirements);
  PL_ASSERT(required_input_props.size() == child_groups.size());

  // Derive chidren plans first because they are useful in the derivation of
  // root plan. Also keep propagate expression to column offset mapping
  vector<unique_ptr<planner::AbstractPlan>> children_plans;
  vector<ExprMap> children_expr_map;
  for (size_t i = 0; i < child_groups.size(); ++i) {
    ExprMap child_expr_map;
    auto child_plan = ChooseBestPlan(child_groups[i], required_input_props[i],
                                     &child_expr_map);
    if (child_plan) {
      children_plans.push_back(move(child_plan));
      children_expr_map.push_back(move(child_expr_map));
    }
  }
  
  
  // Derive root plan
  shared_ptr<OperatorExpression> op =
      make_shared<OperatorExpression>(gexpr->Op());

  auto plan = OptimizerPlanToPlannerPlan(op, requirements, required_input_props,
                                         children_plans, children_expr_map,
                                         output_expr_map);

  LOG_TRACE("Finish Choosing best plan for group %d", id);
  return plan;
}

void Optimizer::OptimizeGroup(GroupID id, PropertySet requirements) {
  LOG_TRACE("Optimizing group %d with req %s", id,
            requirements.ToString().c_str());
  Group *group = memo_.GetGroupByID(id);

  // Whether required properties have already been optimized for the group
  if (group->GetBestExpression(requirements) != nullptr) return;

  const vector<shared_ptr<GroupExpression>> exprs = group->GetExpressions();
  for (size_t i = 0; i < exprs.size(); ++i) {
    if (exprs[i]->Op().IsPhysical()) OptimizeExpression(exprs[i], requirements);
  }
}

void Optimizer::OptimizeExpression(shared_ptr<GroupExpression> gexpr,
                                   PropertySet requirements) {
  LOG_TRACE("Optimizing expression of group %d with op %s", gexpr->GetGroupID(),
            gexpr->Op().name().c_str());

  // Only optimize and cost physical expression
  PL_ASSERT(gexpr->Op().IsPhysical());

  vector<pair<PropertySet, vector<PropertySet>>> output_input_property_pairs =
      DeriveChildProperties(gexpr, requirements);

  size_t num_property_pairs = output_input_property_pairs.size();

  auto child_group_ids = gexpr->GetChildGroupIDs();

  for (size_t pair_offset = 0; pair_offset < num_property_pairs;
       ++pair_offset) {
    auto output_properties = output_input_property_pairs[pair_offset].first;
    const auto &input_properties_list =
        output_input_property_pairs[pair_offset].second;

    vector<shared_ptr<Stats>> best_child_stats;
    vector<double> best_child_costs;
    for (size_t i = 0; i < child_group_ids.size(); ++i) {
      GroupID child_group_id = child_group_ids[i];
      const PropertySet &input_properties = input_properties_list[i];
      // Optimize child
      OptimizeGroup(child_group_id, input_properties);

      // Find best child expression
      shared_ptr<GroupExpression> best_expression =
          memo_.GetGroupByID(child_group_id)
              ->GetBestExpression(input_properties);
      // TODO(abpoms): we should allow for failure in the case where no
      // expression
      // can provide the required properties
      PL_ASSERT(best_expression != nullptr);
      best_child_stats.push_back(best_expression->GetStats(input_properties));
      best_child_costs.push_back(best_expression->GetCost(input_properties));
    }

    Group *group = this->memo_.GetGroupByID(gexpr->GetGroupID());
    // Add to group as potential best cost
    DeriveCostAndStats(gexpr, output_properties, input_properties_list,
                       best_child_stats, best_child_costs);
    group->SetExpressionCost(gexpr, gexpr->GetCost(output_properties),
                             output_properties);

    if (output_properties >= requirements) {
      DeriveCostAndStats(gexpr, requirements, input_properties_list,
                         best_child_stats, best_child_costs);
    }

    // It's possible that PropertySort needs some exprs that need to be
    // generated by PropertyColumns. We check it here
    shared_ptr<Property> new_cols_prop;
    if (requirements.GetPropertyOfType(PropertyType::SORT) != nullptr &&
        requirements.GetPropertyOfType(PropertyType::COLUMNS) != nullptr)
      new_cols_prop.reset(GenerateNewPropertyCols(requirements));

    // enforce missing properties
    auto &required_props = requirements.Properties();
    for (unsigned i = 0; i < required_props.size(); i++) {
      auto property = required_props[i];
      // When enforce PropertyColumns, use the new PropertyCols if necessary
      if (property->Type() == PropertyType::COLUMNS && new_cols_prop != nullptr)
        property = new_cols_prop;

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
  LOG_TRACE("Optimizing expression finishes");
}

Property *Optimizer::GenerateNewPropertyCols(PropertySet requirements) {
  auto cols_prop = requirements.GetPropertyOfType(PropertyType::COLUMNS)
                       ->As<PropertyColumns>();
  auto sort_prop =
      requirements.GetPropertyOfType(PropertyType::SORT)->As<PropertySort>();

  // Check if there is any missing columns from orderby need to be added
  ExprSet columns_set;
  bool orderby_complex_expr = false;
  bool has_star_expr = cols_prop->HasStarExpression();
  for (size_t i = 0; i < sort_prop->GetSortColumnSize(); i++) {
    auto expr = sort_prop->GetSortColumn(i);
    if (expr->GetExpressionType() != ExpressionType::VALUE_TUPLE) {
      orderby_complex_expr = true;
      columns_set.insert(expr);
    } else if (has_star_expr == false) {
      // Only add TupleValueExpr when star expression not present beneath
      columns_set.insert(expr);
    }
  }
  // Only Orderby base column. Star expression is enough
  if (has_star_expr && !orderby_complex_expr) return nullptr;

  for (size_t i = 0; i < cols_prop->GetSize(); i++)
    columns_set.insert(cols_prop->GetColumn(i));

  if (columns_set.size() > cols_prop->GetSize()) {
    // Some orderby exprs are not in original PropertyColumns. Return new one
    vector<shared_ptr<expression::AbstractExpression>> columns(
        columns_set.begin(), columns_set.end());
    return new PropertyColumns(move(columns));
  } else {
    // PropertyColumns already have all the orderby expr. Return the nullptr
    return nullptr;
  }
}

shared_ptr<GroupExpression> Optimizer::EnforceProperty(
    shared_ptr<GroupExpression> gexpr, PropertySet &output_properties,
    const shared_ptr<Property> property, PropertySet &requirements) {
  // new child input is the old output
  auto child_input_properties = vector<PropertySet>();
  child_input_properties.push_back(output_properties);

  auto child_stats = vector<shared_ptr<Stats>>();
  child_stats.push_back(gexpr->GetStats(output_properties));
  auto child_costs = vector<double>();
  child_costs.push_back(gexpr->GetCost(output_properties));

  PropertyEnforcer enforcer(column_manager_);
  auto enforced_gexpr =
      enforcer.EnforceProperty(gexpr, &output_properties, property);

  // the new enforced gexpr have the same GrouID as the parent expr
  // The enforced expression may already exist
  enforced_gexpr =
      memo_.InsertExpression(enforced_gexpr, gexpr->GetGroupID(), true);

  // For orderby, Restore the PropertyColumn back to the original one so that
  // orderby does not output the additional columns only used in order by
  if (property->Type() == PropertyType::SORT) {
    output_properties.RemoveProperty(PropertyType::COLUMNS);
    output_properties.AddProperty(
        requirements.GetPropertyOfType(PropertyType::COLUMNS));
  }
  // If the property with the same type exists, remove it first
  // For example, when enforcing PropertyColumns, there will already be a
  // PropertyColumn in the output_properties
  if (output_properties.GetPropertyOfType(property->Type()) != nullptr)
    output_properties.RemoveProperty(property->Type());
  // New output property would have the enforced Property
  output_properties.AddProperty(shared_ptr<Property>(property));

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

vector<pair<PropertySet, vector<PropertySet>>> Optimizer::DeriveChildProperties(
    shared_ptr<GroupExpression> gexpr, PropertySet requirements) {
  ChildPropertyGenerator converter(column_manager_);
  return converter.GetProperties(gexpr, requirements, &memo_);
}

void Optimizer::DeriveCostAndStats(
    shared_ptr<GroupExpression> gexpr, const PropertySet &output_properties,
    const vector<PropertySet> &input_properties_list,
    vector<shared_ptr<Stats>> child_stats, vector<double> child_costs) {
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

  for (shared_ptr<GroupExpression> gexpr :
       memo_.GetGroupByID(id)->GetExpressions()) {
    ExploreExpression(gexpr);
  }
  memo_.GetGroupByID(id)->SetExplorationFlag();
}

void Optimizer::ExploreExpression(shared_ptr<GroupExpression> gexpr) {
  LOG_TRACE("Exploring expression of group %d with op %s", gexpr->GetGroupID(),
            gexpr->Op().name().c_str());

  PL_ASSERT(gexpr->Op().IsLogical());

  // Explore logically equivalent plans by applying transformation rules
  for (const unique_ptr<Rule> &rule : logical_transformation_rules_) {
    // Apply all rules to operator which match. We apply all rules to one
    // operator before moving on to the next operator in the group because
    // then we avoid missing the application of a rule e.g. an application
    // of some rule creates a match for a previously applied rule, but it is
    // missed because the prev rule was already checked
    vector<shared_ptr<GroupExpression>> candidates =
        TransformExpression(gexpr, *(rule.get()));

    for (shared_ptr<GroupExpression> candidate : candidates) {
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

  for (shared_ptr<GroupExpression> gexpr :
       memo_.GetGroupByID(id)->GetExpressions()) {
    if (gexpr->Op().IsLogical()) ImplementExpression(gexpr);
  }
  memo_.GetGroupByID(id)->SetImplementationFlag();
}

void Optimizer::ImplementExpression(shared_ptr<GroupExpression> gexpr) {
  LOG_TRACE("Implementing expression of group %d with op %s",
            gexpr->GetGroupID(), gexpr->Op().name().c_str());

  // Explore implement physical expressions
  for (const unique_ptr<Rule> &rule : physical_implementation_rules_) {
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
vector<shared_ptr<GroupExpression>> Optimizer::TransformExpression(
    shared_ptr<GroupExpression> gexpr, const Rule &rule) {
  shared_ptr<Pattern> pattern = rule.GetMatchPattern();

  vector<shared_ptr<GroupExpression>> output_plans;
  ItemBindingIterator iterator(*this, gexpr, pattern);
  while (iterator.HasNext()) {
    shared_ptr<OperatorExpression> plan = iterator.Next();
    // Check rule condition function
    if (rule.Check(plan, &memo_)) {
      LOG_TRACE("Rule matched expression of group %d with op %s",
                gexpr->GetGroupID(), gexpr->Op().name().c_str());
      // Apply rule transformations
      // We need to be able to analyze the transformations performed by this
      // rule in order to perform deduplication and launch an exploration of
      // the newly applied rule
      vector<shared_ptr<OperatorExpression>> transformed_plans;
      rule.Transform(plan, transformed_plans);

      // Integrate transformed plans back into groups and explore/cost if new
      for (shared_ptr<OperatorExpression> plan : transformed_plans) {
        LOG_TRACE("Trying to integrate expression with op %s",
                  plan->Op().name().c_str());
        shared_ptr<GroupExpression> new_gexpr;
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

shared_ptr<GroupExpression> Optimizer::MakeGroupExpression(
    shared_ptr<OperatorExpression> expr) {
  vector<GroupID> child_groups;
  for (auto &child : expr->Children()) {
    auto gexpr = MakeGroupExpression(child);
    memo_.InsertExpression(gexpr, false);
    child_groups.push_back(gexpr->GetGroupID());
  }
  return make_shared<GroupExpression>(expr->Op(), child_groups);
}

bool Optimizer::RecordTransformedExpression(
    shared_ptr<OperatorExpression> expr, shared_ptr<GroupExpression> &gexpr) {
  return RecordTransformedExpression(expr, gexpr, UNDEFINED_GROUP);
}

bool Optimizer::RecordTransformedExpression(shared_ptr<OperatorExpression> expr,
                                            shared_ptr<GroupExpression> &gexpr,
                                            GroupID target_group) {
  gexpr = MakeGroupExpression(expr);
  return memo_.InsertExpression(gexpr, target_group, false) != gexpr;
}

}  // namespace optimizer
}  // namespace peloton
