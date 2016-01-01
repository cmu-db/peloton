//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// hash_join_node.h
//
// Identification: src/backend/planner/hash_join_node.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "backend/common/types.h"
#include "backend/expression/abstract_expression.h"
#include "backend/planner/abstract_join_plan.h"
#include "backend/planner/project_info.h"

namespace peloton {
namespace planner {

class HashJoinPlan : public AbstractJoinPlan {
 public:

  HashJoinPlan(const HashJoinPlan &) = delete;
  HashJoinPlan &operator=(const HashJoinPlan &) = delete;
  HashJoinPlan(HashJoinPlan &&) = delete;
  HashJoinPlan &operator=(HashJoinPlan &&) = delete;

  HashJoinPlan(const expression::AbstractExpression *predicate,
                const ProjectInfo *proj_info)
  : AbstractJoinPlan(JOIN_TYPE_INVALID, predicate, proj_info) {}

  inline PlanNodeType GetPlanNodeType() const {
    return PLAN_NODE_TYPE_HASHJOIN;
  }


  inline std::string GetInfo() const { return "HashJoin"; }

 private:
};

}  // namespace planner
}  // namespace peloton
