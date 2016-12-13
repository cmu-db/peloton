//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// optimizer.h
//
// Identification: src/include/optimizer/optimizer.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/logger.h"
#include "optimizer/abstract_optimizer.h"
#include "optimizer/binding.h"
#include "optimizer/column_manager.h"
#include "optimizer/memo.h"
#include "optimizer/operator_node.h"
#include "optimizer/pattern.h"
#include "optimizer/property.h"
#include "optimizer/property_set.h"
#include "optimizer/query_operators.h"
#include "optimizer/rule.h"
#include "planner/abstract_plan.h"

#include <memory>

namespace peloton {

namespace parser {
class SQLStatementList;
class SQLStatement;
}
namespace optimizer {

//===--------------------------------------------------------------------===//
// Optimizer
//===--------------------------------------------------------------------===//
class Optimizer : public AbstractOptimizer {
  friend class BindingIterator;
  friend class GroupBindingIterator;

 public:
  Optimizer(const Optimizer &) = delete;
  Optimizer &operator=(const Optimizer &) = delete;
  Optimizer(Optimizer &&) = delete;
  Optimizer &operator=(Optimizer &&) = delete;

  Optimizer();

  static Optimizer &GetInstance();

  std::shared_ptr<planner::AbstractPlan> BuildPelotonPlanTree(
      const std::unique_ptr<parser::SQLStatementList> &parse_tree);

 private:
  /* TransformQueryTree - create an initial operator tree for the given query
   * to be used in performing optimization.
   *
   * tree: a peloton query tree representing a select query
   * return: the root group expression for the inserted query
   */
  std::shared_ptr<GroupExpression> InsertQueryTree(parser::SQLStatement *tree);

  /* GetQueryTreeRequiredProperties - get the required physical properties for
   * a peloton query tree.
   *
   * tree: a peloton query tree representing a select query
   * return: the set of required physical properties for the query
   */
  PropertySet GetQueryRequiredProperties(parser::SQLStatement *tree);

  /* OptimizerPlanToPlannerPlan - convert a tree of physical operators to
   *     a Peloton planner plan for execution.
   *
   * plan: an optimizer plan composed soley of physical operators
   * return: the corresponding planner plan
   */
  std::unique_ptr<planner::AbstractPlan> OptimizerPlanToPlannerPlan(
      std::shared_ptr<OperatorExpression> plan, PropertySet &requirements,
      std::vector<PropertySet> &required_input_props);

  /* ChooseBestPlan - retrieve the lowest cost tree of physical operators for
   *     the given properties
   *
   * id: the id of the group to produce the best physical
   * requirements: the set of properties the produced physical operator tree
   *     must satisfy
   * return: the lowest cost tree of physical plan nodes
   */
  std::unique_ptr<planner::AbstractPlan> ChooseBestPlan(
      GroupID id, PropertySet requirements);

  /* OptimizeGroup - explore the space of plans for the group to produce the
   *     most optimal physical operator tree and place it in the memo. After
   *     invoking this method, a call to ChooseBestPlan with the same
   *     PropertySet will retreive the optimal plan for the group.
   *
   * id: the group to optimize
   * requirements: the set of requirements the optimal physical operator tree
   *     must fulfill
   */
  void OptimizeGroup(GroupID id, PropertySet requirements);

  /* OptimizeExpression - produce all equivalent logical and physical
   *     operators for this expression by applying transformation rules that
   *     match the expression. If a new logical operator is produced as a result
   *     of a transformation rule, that expression will also be optimized with
   *     the same set of requirements. If a new physical operator is produced,
   *     then the new expression will be costed.
   *
   * gexpr: the group expression to optimize
   * requirements: the set of requirements the most optimal expression produced
   *     must fulfill
   */
  void OptimizeExpression(std::shared_ptr<GroupExpression> gexpr,
                          PropertySet requirements);

  /*
   * Get alternatives of the <output properties, input child property list> pair
   */
  std::vector<std::pair<PropertySet, std::vector<PropertySet>>>
  DeriveChildProperties(std::shared_ptr<GroupExpression> gexpr,
                        PropertySet requirements);

  /*
   * Derive the cost and stats for a group expression given the input/output
   * properties and the children's stats and costs
   */
  void DeriveCostAndStats(std::shared_ptr<GroupExpression> gexpr,
                          const PropertySet &output_properties,
                          const std::vector<PropertySet> &input_properties_list,
                          std::vector<std::shared_ptr<Stats>> child_stats,
                          std::vector<double> child_costs);

  /* EnforceProperty - Enforce a physical property to a gruop expression.
   * Typically this will lead to adding another physical operator on top of the
   * current group expression.
   *
   * gexpr: the group expression to enforce property on
   * property: the required property
   *
   * return: the new group expression that has the enforced property
   */
  std::shared_ptr<GroupExpression> EnforceProperty(
      std::shared_ptr<GroupExpression> gexpr, PropertySet &output_properties,
      const std::shared_ptr<Property> property);

  /* ExploreGroup - exploration equivalent of OptimizeGroup.
   *
   * id: the group to explore
   */
  void ExploreGroup(GroupID id);

  /* ExploreExpression - similar to OptimizeExpression except that it does not
   *     cost new physical operator expressions. The purpose of exploration is
   *     to produce logical expressions for child groups that can be used to
   *     match rules being applied on a parent group.
   *
   * gexpr: the group expression to apply rules to
   */
  void ExploreExpression(std::shared_ptr<GroupExpression> gexpr);

  /* ImplementGroup - Implement physical operators of a group
   *
   * id: the group to explore
   */
  void ImplementGroup(GroupID id);

  /* ImplementExpression - Implement physical operators of a group expression
   *
   * gexpr: the group expression to apply rules to
   */
  void ImplementExpression(std::shared_ptr<GroupExpression> gexpr);

  //////////////////////////////////////////////////////////////////////////////
  /// Rule application
  std::vector<std::shared_ptr<GroupExpression>> TransformExpression(
      std::shared_ptr<GroupExpression> gexpr, const Rule &rule);

  //////////////////////////////////////////////////////////////////////////////
  /// Memo insertion
  std::shared_ptr<GroupExpression> MakeGroupExpression(
      std::shared_ptr<OperatorExpression> expr);

  std::vector<GroupID> MemoTransformedChildren(
      std::shared_ptr<OperatorExpression> expr);

  GroupID MemoTransformedExpression(std::shared_ptr<OperatorExpression> expr);

  bool RecordTransformedExpression(std::shared_ptr<OperatorExpression> expr,
                                   std::shared_ptr<GroupExpression> &gexpr);

  bool RecordTransformedExpression(std::shared_ptr<OperatorExpression> expr,
                                   std::shared_ptr<GroupExpression> &gexpr,
                                   GroupID target_group);

  Memo memo_;
  ColumnManager column_manager_;

  // Rules to transform logical plan to equivalent logical plans
  std::vector<std::unique_ptr<Rule>> logical_transformation_rules_;

  // Rules to transform logical plan to physical implementation
  std::vector<std::unique_ptr<Rule>> physical_implementation_rules_;
};

} /* namespace optimizer */
} /* namespace peloton */
