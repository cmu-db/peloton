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

namespace peloton {
namespace optimizer {

template <class Node, class OperatorType, class OperatorExpr>
int Rule<Node,OperatorType,OperatorExpr>::Promise(
  GroupExpression<Node, OperatorType, OperatorExpr> *group_expr,
  OptimizeContext<Node, OperatorType, OperatorExpr> *context) const {

  //(TODO): handle general/AbstractExpression case
  PELOTON_ASSERT(group_expr);
  PELOTON_ASSERT(context);
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
  //(TODO): handle general/AbstractExpression case
  PELOTON_ASSERT(0);
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

}  // namespace optimizer
}  // namespace peloton
