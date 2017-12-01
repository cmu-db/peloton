//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rule.h
//
// Identification: src/optimizer/optimizer_task.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/optimizer_task.h"
#include "optimizer/optimize_context.h"
#include "optimizer/optimizer_metadata.h"

namespace peloton {
namespace optimizer {
//===--------------------------------------------------------------------===//
// Base class
//===--------------------------------------------------------------------===//
void OptimizerTask::PushTask(OptimizerTask *task) {
  context_->task_pool->Push(task);
}

//===--------------------------------------------------------------------===//
// OptimizeGroup
//===--------------------------------------------------------------------===//
void OptimizeGroup::execute() {
  if (group_->GetCostLB() > context_->cost_upper_boun ||  // Cost LB > Cost UB
      group_->GetBestExpression(*(context_->required_prop.get())) != nullptr)  // Has optimized given the context
    return;

  // Push explore task first for logical expressions
  for(auto& logical_expr : group_->GetLogicalExpressions())
    PushTask(new OptimizeExpression(logical_expr.get(), context_));

  for(auto& phyical_expr : group_->GetPhysicalExpressions())
    PushTask(new OptimizeInputs(phyical_expr.get(), context_));
}


//===--------------------------------------------------------------------===//
// OptimizeExpression
//===--------------------------------------------------------------------===//
void OptimizeExpression::execute() {

  for (auto& rule : metadata_->rule_set.GetRules()) {
    if (group_expr_->HasRuleExplored(rule.get()))
      continue;


  }
}

} // namespace optimizer
} // namespace peloton