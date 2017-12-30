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
#include "common/internal_types.h"

namespace peloton {
namespace planner {

/**
 * @brief Plan node for append.
 */
class AppendPlan : public AbstractPlan {
 public:
  AppendPlan() {}

  inline PlanNodeType GetPlanNodeType() const { return PlanNodeType::APPEND; }

  const std::string GetInfo() const { return "Append"; }

  std::unique_ptr<AbstractPlan> Copy() const {
    return std::unique_ptr<AbstractPlan>(new AppendPlan());
  }

 private:
  DISALLOW_COPY_AND_MOVE(AppendPlan);
};

}  // namespace planner
}  // namespace peloton