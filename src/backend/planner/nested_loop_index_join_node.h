//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// nested_loop_index_join_node.h
//
// Identification: src/backend/planner/nested_loop_index_join_node.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_join_node.h"
#include "backend/common/types.h"
#include "backend/expression/abstract_expression.h"

namespace peloton {
namespace planner {

class NestedLoopIndexJoinNode : public AbstractJoinPlanNode {
 public:
  NestedLoopIndexJoinNode(const NestedLoopIndexJoinNode &) = delete;
  NestedLoopIndexJoinNode &operator=(const NestedLoopIndexJoinNode &) = delete;
  NestedLoopIndexJoinNode(NestedLoopIndexJoinNode &&) = delete;
  NestedLoopIndexJoinNode &operator=(NestedLoopIndexJoinNode &&) = delete;

  NestedLoopIndexJoinNode(const expression::AbstractExpression *predicate,
                          const ProjectInfo *proj_info)
      : AbstractJoinPlanNode(JOIN_TYPE_INVALID, predicate,
                             proj_info) {  // FIXME
    // Nothing to see here...
  }

  inline PlanNodeType GetPlanNodeType() const {
    return PLAN_NODE_TYPE_NESTLOOPINDEX;
  }

  inline std::string GetInfo() const { return "NestedLoopIndexJoin"; }

 private:
  // There is nothing special that we need here
};

}  // namespace planner
}  // namespace peloton
