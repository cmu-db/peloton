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
  if (root_type != OpType::Leaf && root_type != group_expr->Op().type())
    return 0;
  if (IsPhysical()) return PHYS_PROMISE;
  return LOG_PROMISE;
}

RuleSet::RuleSet() {
  transformation_rules_.emplace_back(new InnerJoinCommutativity());
  implementation_rules_.emplace_back(new LogicalDeleteToPhysical());
  implementation_rules_.emplace_back(new LogicalUpdateToPhysical());
  implementation_rules_.emplace_back(new LogicalInsertToPhysical());
  implementation_rules_.emplace_back(new LogicalInsertSelectToPhysical());
  implementation_rules_.emplace_back(new LogicalGroupByToHashGroupBy());
  implementation_rules_.emplace_back(new LogicalAggregateToPhysical());
  implementation_rules_.emplace_back(new GetToDummyScan());
  implementation_rules_.emplace_back(new GetToSeqScan());
  implementation_rules_.emplace_back(new GetToIndexScan());
  implementation_rules_.emplace_back(new LogicalQueryDerivedGetToPhysical());
  implementation_rules_.emplace_back(new InnerJoinToInnerNLJoin());
  implementation_rules_.emplace_back(new InnerJoinToInnerHashJoin());
  implementation_rules_.emplace_back(new ImplementDistinct());
  implementation_rules_.emplace_back(new ImplementLimit());

  rewrite_rules_.emplace_back(new PushFilterThroughJoin());
  rewrite_rules_.emplace_back(new CombineConsecutiveFilter());
  rewrite_rules_.emplace_back(new EmbedFilterIntoGet());
}

}  // namespace optimizer
}  // namespace peloton
