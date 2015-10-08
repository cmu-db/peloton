//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// mock_plan.h
//
// Identification: src/backend/planner/mock_plan.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once


#include "backend/common/types.h"
#include "backend/planner/abstract_plan.h"
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

  inline PlanNodeType GetPlanNodeType() const {
    return PLAN_NODE_TYPE_MOCK;
  }

  //===--------------------------------------------------------------------===//
  // Utilities
  //===--------------------------------------------------------------------===//

  inline std::string GetInfo() const {
    return "Mock";
  }



};

}  // namespace test
}  // namespace peloton
