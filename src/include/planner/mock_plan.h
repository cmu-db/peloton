//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// mock_plan.h
//
// Identification: src/include/planner/mock_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types.h"
#include "planner/abstract_plan.h"
#include "gmock/gmock.h"

namespace peloton {

namespace test {
//===--------------------------------------------------------------------===//
// Abstract Plan
//===--------------------------------------------------------------------===//

class MockPlan : public planner::AbstractPlan {
 public:
  MockPlan() : planner::AbstractPlan() {}

  //===--------------------------------------------------------------------===//
  // Accessors
  //===--------------------------------------------------------------------===//

  inline PlanNodeType GetPlanNodeType() const { return PLAN_NODE_TYPE_MOCK; }

  //===--------------------------------------------------------------------===//
  // Utilities
  //===--------------------------------------------------------------------===//

  inline const std::string GetInfo() const { return "Mock"; }

  std::unique_ptr<planner::AbstractPlan> Copy() const {
    return std::unique_ptr<AbstractPlan>(new MockPlan());
  }
};

}  // namespace test
}  // namespace peloton
