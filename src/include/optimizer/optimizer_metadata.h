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

#include "common/timer.h"
#include "optimizer/memo.h"
#include "optimizer/group_expression.h"
#include "optimizer/rule.h"
#include "settings/settings_manager.h"

namespace peloton {
namespace catalog {
class Catalog;
}
namespace optimizer {

class OptimizerTaskPool;
class RuleSet;

class OptimizerMetadata {
 public:
  OptimizerMetadata()
      : timeout_limit(settings::SettingsManager::GetInt(
            settings::SettingId::task_execution_timeout)),
        timer(Timer<std::milli>()) {}

  Memo memo;
  RuleSet rule_set;
  OptimizerTaskPool *task_pool;
  catalog::CatalogCache *catalog_cache;
  unsigned int timeout_limit;
  Timer<std::milli> timer;
  concurrency::TransactionContext* txn;

  void SetTaskPool(OptimizerTaskPool *task_pool) {
    this->task_pool = task_pool;
  }

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
    return (memo.InsertExpression(gexpr, target_group, false) == gexpr.get());
  }

  // TODO(boweic): check if we really need to use shared_ptr
  void ReplaceRewritedExpression(std::shared_ptr<OperatorExpression> expr,
                                 GroupID target_group) {
    memo.EraseExpression(target_group);
    memo.InsertExpression(MakeGroupExpression(expr), target_group, false);
  }
};

}  // namespace optimizer
}  // namespace peloton
