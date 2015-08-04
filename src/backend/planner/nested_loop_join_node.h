/**
 * @brief Header for nested loop join plan node.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "backend/common/types.h"
#include "backend/expression/abstract_expression.h"
#include "backend/planner/project_info.h"
#include "backend/planner/abstract_join_node.h"

namespace peloton {
namespace planner {

class NestedLoopJoinNode : public AbstractJoinPlanNode {
 public:
  NestedLoopJoinNode(const NestedLoopJoinNode &) = delete;
  NestedLoopJoinNode &operator=(const NestedLoopJoinNode &) = delete;
  NestedLoopJoinNode(NestedLoopJoinNode &&) = delete;
  NestedLoopJoinNode &operator=(NestedLoopJoinNode &&) = delete;

  NestedLoopJoinNode(expression::AbstractExpression *predicate,
                     const ProjectInfo *proj_info)
      : AbstractJoinPlanNode(JOIN_TYPE_INVALID, predicate, proj_info) {  // FIXME
    // Nothing to see here...
  }

  inline PlanNodeType GetPlanNodeType() const {
    return PLAN_NODE_TYPE_NESTLOOP;
  }

  inline std::string GetInfo() const { return "NestedLoopJoin"; }

 private:
  // There is nothing special that we need here
};

}  // namespace planner
}  // namespace peloton
