//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_optimizer.h
//
// Identification: src/include/optimizer/abstract_optmizer.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/memo.h"
#include "optimizer/column_manager.h"
#include "optimizer/query_operators.h"
#include "optimizer/operator_node.h"
#include "optimizer/binding.h"
#include "optimizer/pattern.h"
#include "optimizer/property.h"
#include "optimizer/property_set.h"
#include "optimizer/rule.h"
#include "planner/abstract_plan.h"
#include "common/logger.h"

#include <memory>

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Abstract Optimizer
//===--------------------------------------------------------------------===//
class AbstractOptimizer {
 public:
  AbstractOptimizer(const AbstractOptimizer &) = delete;
  AbstractOptimizer &operator=(const AbstractOptimizer &) = delete;
  AbstractOptimizer(AbstractOptimizer &&) = delete;
  AbstractOptimizer &operator=(AbstractOptimizer &&) = delete;

  AbstractOptimizer();
  virtual ~AbstractOptimizer();

  static AbstractOptimizer &GetInstance();

  virtual std::shared_ptr<planner::AbstractPlan> GeneratePlan(
      std::shared_ptr<std::string> plan_tree);

  virtual bool ShouldOptimize(std::string parse);

  virtual std::shared_ptr<std::string> TransformParseTreeToOptimizerPlanTree(
      std::string parse);
};

} // namespace optimizer
} // namespace peloton
