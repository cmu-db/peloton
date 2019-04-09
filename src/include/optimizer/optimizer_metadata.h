//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// optimizer_metadata.h
//
// Identification: src/include/optimizer/optimizer_metadata.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/timer.h"
#include "optimizer/cost_model/default_cost_model.h"
#include "optimizer/memo.h"
#include "optimizer/group_expression.h"
#include "optimizer/rule.h"
#include "settings/settings_manager.h"

namespace peloton {
namespace catalog {
class Catalog;
class CatalogCache;
}
namespace optimizer {

template <class Node, class OperatorType, class OperatorExpr>
class OptimizerTaskPool;

template <class Node, class OperatorType, class OperatorExpr>
class RuleSet;

template <class Node, class OperatorType, class OperatorExpr>
class OptimizerMetadata {
 public:

  OptimizerMetadata(std::unique_ptr<AbstractCostModel> cost_model)
      : cost_model(std::move(cost_model)), timeout_limit(settings::SettingsManager::GetInt(
      settings::SettingId::task_execution_timeout)),
        timer(Timer<std::milli>()) {}

  Memo<Node, OperatorType, OperatorExpr> memo;
  RuleSet<Node, OperatorType, OperatorExpr> rule_set;
  OptimizerTaskPool<Node, OperatorType, OperatorExpr> *task_pool;
  std::unique_ptr<AbstractCostModel> cost_model;
  catalog::CatalogCache *catalog_cache;
  unsigned int timeout_limit;
  Timer<std::milli> timer;
  concurrency::TransactionContext* txn;

  void SetTaskPool(OptimizerTaskPool<Node,OperatorType,OperatorExpr> *task_pool) {
    this->task_pool = task_pool;
  }

  std::shared_ptr<GroupExpression<Node,OperatorType,OperatorExpr>> MakeGroupExpression(
      std::shared_ptr<OperatorExpr> expr) {
    std::vector<GroupID> child_groups;
    for (auto &child : expr->Children()) {
      auto gexpr = MakeGroupExpression(child);
      memo.InsertExpression(gexpr, false);
      child_groups.push_back(gexpr->GetGroupID());
    }
    return std::make_shared<GroupExpression<Node,OperatorType,OperatorExpr>>(expr->Op(),
                                                                             std::move(child_groups));
  }

  bool RecordTransformedExpression(std::shared_ptr<OperatorExpr> expr,
                                   std::shared_ptr<GroupExpression<Node,OperatorType,OperatorExpr>> &gexpr) {
    return RecordTransformedExpression(expr, gexpr, UNDEFINED_GROUP);
  }

  bool RecordTransformedExpression(std::shared_ptr<OperatorExpr> expr,
                                   std::shared_ptr<GroupExpression<Node,OperatorType,OperatorExpr>> &gexpr,
                                   GroupID target_group) {
    gexpr = MakeGroupExpression(expr);
    return (memo.InsertExpression(gexpr, target_group, false) == gexpr.get());
  }

  // TODO(boweic): check if we really need to use shared_ptr
  void ReplaceRewritedExpression(std::shared_ptr<OperatorExpr> expr,
                                 GroupID target_group) {
    memo.EraseExpression(target_group);
    memo.InsertExpression(MakeGroupExpression(expr), target_group, false);
  }
};

}  // namespace optimizer
}  // namespace peloton
