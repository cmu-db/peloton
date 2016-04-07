//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// append_plan.h
//
// Identification: src/backend/planner/append_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "abstract_plan.h"
#include "backend/common/types.h"

namespace peloton {
namespace planner {

/**
 * @brief Plan node for append.
 */
class AppendPlan : public AbstractPlan {
 public:
  AppendPlan(const AppendPlan &) = delete;
  AppendPlan &operator=(const AppendPlan &) = delete;
  AppendPlan(const AppendPlan &&) = delete;
  AppendPlan &operator=(const AppendPlan &&) = delete;

  AppendPlan() {}

  inline PlanNodeType GetPlanNodeType() const { return PLAN_NODE_TYPE_APPEND; }

  const std::string GetInfo() const { return "Append"; }

  AbstractPlan *Copy() const { return new AppendPlan(); }

private:
  // nothing
};
}
}
