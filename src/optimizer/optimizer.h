//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// optimizer.h
//
// Identification: src/backend/optimizer/optimizer.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/optimizer/memo.h"
#include "backend/optimizer/column_manager.h"
#include "backend/optimizer/query_operators.h"
#include "backend/optimizer/operator_node.h"
#include "backend/optimizer/binding.h"
#include "backend/optimizer/pattern.h"
#include "backend/optimizer/property.h"
#include "backend/optimizer/property_set.h"
#include "backend/optimizer/rule.h"
#include "backend/planner/abstract_plan.h"
#include "backend/common/logger.h"

#include <memory>

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Optimizer
//===--------------------------------------------------------------------===//
class Optimizer {
  friend class BindingIterator;
  friend class GroupBindingIterator;
 public:
  Optimizer(const Optimizer &) = delete;
  Optimizer &operator=(const Optimizer &) = delete;
  Optimizer(Optimizer &&) = delete;
  Optimizer &operator=(Optimizer &&) = delete;

  Optimizer();

  static Optimizer &GetInstance();

  std::shared_ptr<planner::AbstractPlan> GeneratePlan(
    std::shared_ptr<Select> select_tree);

 private:

  /* TransformQueryTree - create an initial operator tree for the given query
   * to be used in performing optimization.
   *
   * tree: a peloton query tree representing a select query
   * return: the root group expression for the inserted query
   */
  std::shared_ptr<GroupExpression> InsertQueryTree(
    std::shared_ptr<Select> tree);

  /* GetQueryTreeRequiredProperties - get the required physical properties for
   * a peloton query tree.
   *
   * tree: a peloton query tree representing a select query
   * return: the set of required physical properties for the query
   */
  PropertySet GetQueryTreeRequiredProperties(
    std::shared_ptr<Select> tree);

  /* OptimizerPlanToPlannerPlan - convert a tree of physical operators to
   *     a Peloton planner plan for execution.
   *
   * plan: an optimizer plan composed soley of physical operators
   * return: the corresponding planner plan
   */
  planner::AbstractPlan *OptimizerPlanToPlannerPlan(
    std::shared_ptr<OpExpression> plan);

  /* ChooseBestPlan - retrieve the lowest cost tree of physical operators for
   *     the given properties
   *
   * id: the id of the group to produce the best physical
   * requirements: the set of properties the produced physical operator tree
   *     must satisfy
   * return: the lowest cost tree of physical operators
   */
  std::shared_ptr<OpExpression> ChooseBestPlan(
    GroupID id,
    PropertySet requirements);

  /* OptimizeGroup - explore the space of plans for the group to produce the
   *     most optimal physical operator tree and place it in the memo. After
   *     invoking this method, a call to ChooseBestPlan with the same
   *     PropertySet will retreive the optimal plan for the group.
   *
   * id: the group to optimize
   * requirements: the set of requirements the optimal physical operator tree
   *     must fulfill
   */
  void OptimizeGroup(
    GroupID id,
    PropertySet requirements);

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
  void OptimizeExpression(
    std::shared_ptr<GroupExpression> gexpr,
    PropertySet requirements);

  /* CostExpression - determine the cost of the provided group expression by
   *     recursively optimizing and deriving statistics from child groups to be
   *     used in producing the cost and statics of the expression.
   *
   * gexpr: the group to cost
   */
  void CostExpression(
    std::shared_ptr<GroupExpression> gexpr);


  /* ExploreGroup - exploration equivalent of OptimizeGroup.
   *
   * gexpr: the group to explore
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

  //////////////////////////////////////////////////////////////////////////////
  /// Rule application
  std::vector<std::shared_ptr<GroupExpression>> TransformExpression(
    std::shared_ptr<GroupExpression> gexpr,
    const Rule &rule);

  //////////////////////////////////////////////////////////////////////////////
  /// Memo insertion
  std::shared_ptr<GroupExpression> MakeGroupExpression(
    std::shared_ptr<OpExpression> expr);

  std::vector<GroupID> MemoTransformedChildren(
    std::shared_ptr<OpExpression> expr);

  GroupID MemoTransformedExpression(
    std::shared_ptr<OpExpression> expr);

  bool RecordTransformedExpression(
    std::shared_ptr<OpExpression> expr,
    std::shared_ptr<GroupExpression> &gexpr);

  bool RecordTransformedExpression(
    std::shared_ptr<OpExpression> expr,
    std::shared_ptr<GroupExpression> &gexpr,
    GroupID target_group);

  Memo memo;
  ColumnManager column_manager;
  std::vector<std::unique_ptr<Rule>> rules;
};

} /* namespace optimizer */
} /* namespace peloton */
