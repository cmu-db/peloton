//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// nested_loop_join_node.h
//
// Identification: src/backend/planner/nested_loop_join_node.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_join_plan.h"
#include "backend/common/types.h"
#include "backend/expression/abstract_expression.h"
#include "backend/planner/project_info.h"
#include "nodes/plannodes.h"

namespace peloton {
namespace planner {

class NestedLoopJoinPlan : public AbstractJoinPlan {
 public:
  NestedLoopJoinPlan(const NestedLoopJoinPlan &) = delete;
  NestedLoopJoinPlan &operator=(const NestedLoopJoinPlan &) = delete;
  NestedLoopJoinPlan(NestedLoopJoinPlan &&) = delete;
  NestedLoopJoinPlan &operator=(NestedLoopJoinPlan &&) = delete;

  NestedLoopJoinPlan(PelotonJoinType join_type,
                     const expression::AbstractExpression *predicate,
                     const ProjectInfo *proj_info)
      : AbstractJoinPlan(join_type, predicate, proj_info) {
    nl_ = nullptr;
  }

  NestedLoopJoinPlan(PelotonJoinType join_type,
                     const expression::AbstractExpression *predicate,
                     const ProjectInfo *proj_info, NestLoop *nl)
      : AbstractJoinPlan(join_type, predicate, proj_info) {
      nl_ = nl;
  } // added to set member nl_

  inline PlanNodeType GetPlanNodeType() const {
    return PLAN_NODE_TYPE_NESTLOOP;
  }

  const std::string GetInfo() const { return "NestedLoopJoin"; }

  inline NestLoop* GetNestLoop() const { return nl_; } // added to support IN+subquery

 private:
  NestLoop *nl_; // added to support IN+subquery
};

}  // namespace planner
}  // namespace peloton
