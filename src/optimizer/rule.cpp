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
  rules_.emplace_back(new InnerJoinCommutativity());
  rules_.emplace_back(new LogicalDeleteToPhysical());
  rules_.emplace_back(new LogicalUpdateToPhysical());
  rules_.emplace_back(new LogicalInsertToPhysical());
  rules_.emplace_back(new LogicalInsertSelectToPhysical());
  rules_.emplace_back(new LogicalGroupByToHashGroupBy());
  rules_.emplace_back(new LogicalAggregateToPhysical());
  rules_.emplace_back(new GetToDummyScan());
  rules_.emplace_back(new GetToSeqScan());
  rules_.emplace_back(new GetToIndexScan());
  rules_.emplace_back(new LogicalQueryDerivedGetToPhysical());
  rules_.emplace_back(new InnerJoinToInnerNLJoin());
  rules_.emplace_back(new InnerJoinToInnerHashJoin());
}

}  // namespace optimizer
}  // namespace peloton