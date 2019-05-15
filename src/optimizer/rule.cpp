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

int Rule::Promise(GroupExpression *group_expr, OptimizeContext *context) const {
  (void)context;
  auto root_type = match_pattern->GetOpType();
  auto root_type_exp = match_pattern->GetExpType();

  // This rule is not applicable
  if (root_type != OpType::Undefined &&
      root_type != OpType::Leaf &&
      root_type != group_expr->Node()->GetOpType()) {
    return 0;
  }

  if (root_type_exp != ExpressionType::INVALID &&
      root_type_exp != ExpressionType::GROUP_MARKER &&
      root_type_exp != group_expr->Node()->GetExpType()) {
    return 0;
  }

  if (IsPhysical()) return PHYS_PROMISE;
  return LOG_PROMISE;
}

RuleSet::RuleSet() {
  // Comparator Elimination related rules
  std::vector<std::pair<RuleType,ExpressionType>> comp_elim_pairs = {
    std::make_pair(RuleType::CONSTANT_COMPARE_EQUAL, ExpressionType::COMPARE_EQUAL),
    std::make_pair(RuleType::CONSTANT_COMPARE_NOTEQUAL, ExpressionType::COMPARE_NOTEQUAL),
    std::make_pair(RuleType::CONSTANT_COMPARE_LESSTHAN, ExpressionType::COMPARE_LESSTHAN),
    std::make_pair(RuleType::CONSTANT_COMPARE_GREATERTHAN, ExpressionType::COMPARE_GREATERTHAN),
    std::make_pair(RuleType::CONSTANT_COMPARE_LESSTHANOREQUALTO, ExpressionType::COMPARE_LESSTHANOREQUALTO),
    std::make_pair(RuleType::CONSTANT_COMPARE_GREATERTHANOREQUALTO, ExpressionType::COMPARE_GREATERTHANOREQUALTO)
  };

  for (auto &pair : comp_elim_pairs) {
    AddRewriteRule(
      RewriteRuleSetName::GENERIC_RULES,
      new ComparatorElimination(pair.first, pair.second)
    );
  }

  // Equivalent Transform related rules (flip AND, OR, EQUAL)
  std::vector<std::pair<RuleType,ExpressionType>> equiv_pairs = {
    std::make_pair(RuleType::EQUIV_AND, ExpressionType::CONJUNCTION_AND),
    std::make_pair(RuleType::EQUIV_OR, ExpressionType::CONJUNCTION_OR),
    std::make_pair(RuleType::EQUIV_COMPARE_EQUAL, ExpressionType::COMPARE_EQUAL)
  };
  for (auto &pair : equiv_pairs) {
    AddRewriteRule(
      RewriteRuleSetName::EQUIVALENT_TRANSFORM,
      new EquivalentTransform(pair.first, pair.second)
    );
  }

  // Additional rules
  AddRewriteRule(RewriteRuleSetName::GENERIC_RULES, new TVEqualityWithTwoCVTransform());
  AddRewriteRule(RewriteRuleSetName::GENERIC_RULES, new TransitiveClosureConstantTransform());

  AddRewriteRule(RewriteRuleSetName::GENERIC_RULES, new AndShortCircuit());
  AddRewriteRule(RewriteRuleSetName::GENERIC_RULES, new OrShortCircuit());

  AddRewriteRule(RewriteRuleSetName::GENERIC_RULES, new NullLookupOnNotNullColumn());
  AddRewriteRule(RewriteRuleSetName::GENERIC_RULES, new NotNullLookupOnNotNullColumn());

  AddRewriteRule(RewriteRuleSetName::GENERIC_RULES, new TVEqualityWithTwoCVTransform());
  AddRewriteRule(RewriteRuleSetName::GENERIC_RULES, new TransitiveClosureConstantTransform());

  // Define transformation/implementation rules
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

  // Query optimizer related rewrite rules
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
