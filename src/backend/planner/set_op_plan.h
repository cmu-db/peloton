//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// set_op_plan.h
//
// Identification: src/backend/planner/set_op_plan.h
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
 * @brief Plan node for set operation:
 * INTERSECT/INTERSECT ALL/EXPECT/EXCEPT ALL
 *
 * @warning UNION (ALL) is handled differently.
 * IMPORTANT: Both children must have the same physical schema.
 */
class SetOpPlan : public AbstractPlan {
 public:
  SetOpPlan(const SetOpPlan &) = delete;
  SetOpPlan &operator=(const SetOpPlan &) = delete;
  SetOpPlan(const SetOpPlan &&) = delete;
  SetOpPlan &operator=(const SetOpPlan &&) = delete;

  SetOpPlan(SetOpType set_op) : set_op_(set_op) {}

  SetOpType GetSetOp() const { return set_op_; }

  inline PlanNodeType GetPlanNodeType() const { return PLAN_NODE_TYPE_SETOP; }

  const std::string GetInfo() const { return "SetOp"; }

  std::unique_ptr<AbstractPlan> Copy() const {
    return std::unique_ptr<AbstractPlan>(new SetOpPlan(set_op_));
  }

  // Every class should implement SerializeTo method before using it.
  // The implementation in seq_scan_plan can be referenced
  bool SerializeTo(SerializeOutput &output) const {
	  ALWAYS_ASSERT(&output != nullptr);
	  throw SerializationException("This class should implement SerializeTo method");}

 private:
  /** @brief Set Operation of this node */
  SetOpType set_op_;
};
}
}
