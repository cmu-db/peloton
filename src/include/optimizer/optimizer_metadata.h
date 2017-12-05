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
#include "optimizer/memo.h"
#include "optimizer/rule.h"
namespace peloton {
namespace optimizer {

class OptimizerMetadata {
 public:
  Memo memo;
  RuleSet rule_set;
  OptimizerTaskPool task_pool;

  void AddRule(Rule *rule) { rule_set.AddRule(rule); }

  std::shared_ptr<GroupExpression> MakeGroupExpression(
      std::shared_ptr<OperatorExpression> expr) {
    std::vector<GroupID> child_groups;
    for (auto &child : expr->Children()) {
      auto gexpr = MakeGroupExpression(child);
      memo.InsertExpression(gexpr, false);
      child_groups.push_back(gexpr->GetGroupID());
    }
    return std::make_shared<GroupExpression>(expr->Op(),
                                             std::move(child_groups));
  }

  bool RecordTransformedExpression(std::shared_ptr<OperatorExpression> expr,
                                   std::shared_ptr<GroupExpression> &gexpr) {
    return RecordTransformedExpression(expr, gexpr, UNDEFINED_GROUP);
  }

  bool RecordTransformedExpression(std::shared_ptr<OperatorExpression> expr,
                                   std::shared_ptr<GroupExpression> &gexpr,
                                   GroupID target_group) {
    gexpr = MakeGroupExpression(expr);
    if (memo.InsertExpression(gexpr, target_group, false) != gexpr) {
      gexpr->ResetRuleMask(rule_set.size());
      return true;
    }
    return false;
  }
};

}  // namespace optimizer
}  // namespace peloton
