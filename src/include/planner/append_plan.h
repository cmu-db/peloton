//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// append_plan.h
//
// Identification: src/include/planner/append_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "abstract_plan.h"
#include "common/types.h"

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

  void SetParameterValues(UNUSED_ATTRIBUTE std::vector<Value>* values) { };

  const std::string GetInfo() const { return "Append"; }

  std::unique_ptr<AbstractPlan> Copy() const {
    return std::unique_ptr<AbstractPlan>(new AppendPlan());
  }

 private:
  // nothing
};
}
}
