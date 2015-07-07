/*-------------------------------------------------------------------------
 *
 * plan_executor.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/bridge/plan_executor.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/common/types.h"
#include "backend/executor/abstract_executor.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Plan Executor
//===--------------------------------------------------------------------===//

class PlanExecutor {

 public:
  PlanExecutor(const PlanExecutor &) = delete;
  PlanExecutor& operator=(const PlanExecutor &) = delete;
  PlanExecutor(PlanExecutor &&) = delete;
  PlanExecutor& operator=(PlanExecutor &&) = delete;

  PlanExecutor(){};

  static void PrintPlan(const planner::AbstractPlanNode *plan, std::string prefix = "");

  static bool ExecutePlan(planner::AbstractPlanNode *plan);

 private:

};

} // namespace bridge
} // namespace peloton
