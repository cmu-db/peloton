//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// set_op_node.h
//
// Identification: src/backend/planner/hash_plan.h
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
 * @brief
 *
 */
class HashPlan : public AbstractPlan {
 public:
  HashPlan(const HashPlan &) = delete;
  HashPlan &operator=(const HashPlan &) = delete;
  HashPlan(const HashPlan &&) = delete;
  HashPlan &operator=(const HashPlan &&) = delete;

  HashPlan() {}

  inline PlanNodeType GetPlanNodeType() const { return PLAN_NODE_TYPE_HASH; }

  inline std::string GetInfo() const { return "Hash"; }

 private:
};
}
}
