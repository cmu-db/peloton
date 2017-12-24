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

int Rule::Promise(GroupExpression *group_expr, OptimizeContext *context) const {
  (void)context;
  auto root_type = match_pattern->Type();
  // This rule is not applicable
  if (root_type != OpType::Leaf && root_type != group_expr->Op().type()) {
    return 0;
  }
  if (IsPhysical()) return PHYS_PROMISE;
  return LOG_PROMISE;
}

RuleSet::RuleSet() {
  AddTransformationRule(new InnerJoinCommutativity());
  AddImplementationRule(new LogicalDeleteToPhysical());
  AddImplementationRule(new LogicalUpdateToPhysical());
  AddImplementationRule(new LogicalInsertToPhysical());
  AddImplementationRule(new LogicalInsertSelectToPhysical());
  AddImplementationRule(new LogicalGroupByToHashGroupBy());
  AddImplementationRule(new LogicalAggregateToPhysical());
  AddImplementationRule(new GetToDummyScan());
  AddImplementationRule(new GetToSeqScan());
  AddImplementationRule(new GetToIndexScan());
  AddImplementationRule(new LogicalQueryDerivedGetToPhysical());
  AddImplementationRule(new InnerJoinToInnerNLJoin());
  AddImplementationRule(new InnerJoinToInnerHashJoin());
  AddImplementationRule(new ImplementDistinct());
  AddImplementationRule(new ImplementLimit());

  AddRewriteRule(RewriteRuleSetName::PREDICATE_PUSH_DOWN, new PushFilterThroughJoin());
  AddRewriteRule(RewriteRuleSetName::PREDICATE_PUSH_DOWN, new CombineConsecutiveFilter());
  AddRewriteRule(RewriteRuleSetName::PREDICATE_PUSH_DOWN, new EmbedFilterIntoGet());

  AddRewriteRule(RewriteRuleSetName::UNNEST_SUBQUERY, new PullFilterThroughMarkJoin());
  AddRewriteRule(RewriteRuleSetName::UNNEST_SUBQUERY, new MarkJoinInnerJoinToInnerJoin());
  AddRewriteRule(RewriteRuleSetName::UNNEST_SUBQUERY, new MarkJoinGetToInnerJoin());


}

}  // namespace optimizer
}  // namespace peloton
