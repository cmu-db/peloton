//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rule.h
//
// Identification: src/include/optimizer/optimizer_task.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "optimizer/metadata.h"

namespace peloton {
namespace optimizer {

class OptimizeContext;

enum class OptimizerTaskType {
  OPTIMIZE_GROUP,
  OPTIMIZE_EXPR,
  EXPLORE_GROUP,
  EXPLORE_EXPR,
  APPLY_RULE,
  OPTIMIZE_INPUTS
};

class OptimizerTask {
 public:
  OptimizerTask(std::shared_ptr<OptimizeContext> context, OptimizerTaskType type):
      context_(context), type_(type){}

  virtual void execute() = 0;
  
  void PushTask(OptimizerTask* task);

  inline Memo& GetMemo() const;

  inline RuleSet& GetRuleSet() const;

 protected:
  OptimizerTaskType type_;
  std::shared_ptr<OptimizeContext> context_;
};

class OptimizeGroup : public OptimizerTask {
 public:
  OptimizeGroup(Group* group, std::shared_ptr<OptimizeContext> context)
      : OptimizerTask(context, OptimizerTaskType::OPTIMIZE_GROUP),
        group_(group) {}
  virtual void execute() override;

 private:
  Group* group_;
};

class OptimizeExpression : public OptimizerTask {
 public:
  OptimizeExpression(GroupExpression* group_expr,
                     std::shared_ptr<OptimizeContext> context)
      : OptimizerTask(context, OptimizerTaskType::OPTIMIZE_EXPR),
        group_expr_(group_expr) {}
  virtual void execute() override;

 private:
  GroupExpression* group_expr_;
};

class ExploreGroup : public OptimizerTask {
 public:
  ExploreGroup(Group* group, std::shared_ptr<OptimizeContext> context)
      : OptimizerTask(context, OptimizerTaskType::EXPLORE_GROUP), group_(group) {}
  virtual void execute() override;

 private:
  Group* group_;
};

class ExploreExpression : public OptimizerTask {
 public:
  ExploreExpression(GroupExpression* group_expr,
                    std::shared_ptr<OptimizeContext> context)
      : OptimizerTask(context, OptimizerTaskType::EXPLORE_EXPR),
        group_expr_(group_expr) {}
  virtual void execute() override;

 private:
  GroupExpression* group_expr_;
};

class ApplyRule : public OptimizerTask {
 public:
  ApplyRule(GroupExpression *group_expr,
            Rule *rule,
            std::shared_ptr<OptimizeContext> context)
      : OptimizerTask(context, OptimizerTaskType::APPLY_RULE),
        group_expr_(group_expr), rule_(rule) {}
  virtual void execute() override;

 private:
  GroupExpression *group_expr_;
  Rule *rule_;
};

class OptimizeInputs : public OptimizerTask {
 public:
  OptimizeInputs(GroupExpression *group_expr,
                 std::shared_ptr<OptimizeContext> context)
      : OptimizerTask(context, OptimizerTaskType::OPTIMIZE_INPUTS),
        group_expr_(group_expr) {
    child_costs_.resize(group_expr->GetChildGroupIDs().size(), 0);
  }
  virtual void execute() override;

 private:
  GroupExpression *group_expr_;
  std::vector<double> child_costs_;
  double local_cost_ = 0;
  int current_child_no_ = 0;
};

} // namespace optimizer
} // namespace peloton