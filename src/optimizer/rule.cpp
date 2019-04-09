//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rule.h
//
// Identification: src/optimizer/rule.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/rule_impls.h"
#include "optimizer/group_expression.h"
#include "optimizer/absexpr_expression.h"
#include "optimizer/rule_rewrite.h"

namespace peloton {
namespace optimizer {

template <class Node, class OperatorType, class OperatorExpr>
int Rule<Node,OperatorType,OperatorExpr>::Promise(
  GroupExpression<Node, OperatorType, OperatorExpr> *group_expr,
  OptimizeContext<Node, OperatorType, OperatorExpr> *context) const {

  (void)group_expr;
  (void)context;

  LOG_ERROR("Rule::Promise for rewrite engine not implemented!");
  PELOTON_ASSERT(0);
  return 0;
}

// Specialization due to OpType
template <>
int Rule<Operator,OpType,OperatorExpression>::Promise(
  GroupExpression<Operator,OpType,OperatorExpression> *group_expr,
  OptimizeContext<Operator,OpType,OperatorExpression> *context) const {

  (void)context;
  auto root_type = match_pattern->Type();
  // This rule is not applicable
  if (root_type != OpType::Leaf && root_type != group_expr->Op().GetType()) {
    return 0;
  }
  if (IsPhysical()) return PHYS_PROMISE;
  return LOG_PROMISE;
}

template <class Operator, class OperatorType, class OperatorExpr>
RuleSet<Operator,OperatorType,OperatorExpr>::RuleSet() {
  LOG_ERROR("Must invoke specialization of RuleSet constructor");
  PELOTON_ASSERT(0);
}

template <>
RuleSet<AbsExpr_Container,ExpressionType,AbsExpr_Expression>::RuleSet() {
  AddRewriteRule(RewriteRuleSetName::COMPARATOR_ELIMINATION,
                 new ComparatorElimination());
}

template <>
RuleSet<Operator,OpType, OperatorExpression>::RuleSet() {
  AddTransformationRule(new InnerJoinCommutativity());
  AddTransformationRule(new InnerJoinAssociativity());
  AddImplementationRule(new LogicalDeleteToPhysical());
  AddImplementationRule(new LogicalUpdateToPhysical());
  AddImplementationRule(new LogicalInsertToPhysical());
  AddImplementationRule(new LogicalInsertSelectToPhysical());
  AddImplementationRule(new LogicalGroupByToHashGroupBy());
  AddImplementationRule(new LogicalAggregateToPhysical());
  AddImplementationRule(new GetToDummyScan());
  AddImplementationRule(new GetToSeqScan());
  AddImplementationRule(new GetToIndexScan());
  AddImplementationRule(new LogicalExternalFileGetToPhysical());
  AddImplementationRule(new LogicalQueryDerivedGetToPhysical());
  AddImplementationRule(new InnerJoinToInnerNLJoin());
  AddImplementationRule(new InnerJoinToInnerHashJoin());
  AddImplementationRule(new ImplementDistinct());
  AddImplementationRule(new ImplementLimit());
  AddImplementationRule(new LogicalExportToPhysicalExport());

  AddRewriteRule(RewriteRuleSetName::PREDICATE_PUSH_DOWN,
                 new PushFilterThroughJoin());
  AddRewriteRule(RewriteRuleSetName::PREDICATE_PUSH_DOWN,
                 new PushFilterThroughAggregation());
  AddRewriteRule(RewriteRuleSetName::PREDICATE_PUSH_DOWN,
                 new CombineConsecutiveFilter());
  AddRewriteRule(RewriteRuleSetName::PREDICATE_PUSH_DOWN,
                 new EmbedFilterIntoGet());

  AddRewriteRule(RewriteRuleSetName::UNNEST_SUBQUERY,
                 new PullFilterThroughMarkJoin());
  AddRewriteRule(RewriteRuleSetName::UNNEST_SUBQUERY,
                 new MarkJoinToInnerJoin());
  AddRewriteRule(RewriteRuleSetName::UNNEST_SUBQUERY,
                 new PullFilterThroughAggregation());
}

// Explicitly instantiate
template class Rule<Operator,OpType,OperatorExpression>;
template class Rule<AbsExpr_Container,ExpressionType,AbsExpr_Expression>;

}  // namespace optimizer
}  // namespace peloton
