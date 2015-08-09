//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// append_node.h
//
// Identification: src/backend/planner/append_node.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
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

  inline std::string GetInfo() const { return "Append"; }

 private:
  // nothing
};
}
}
