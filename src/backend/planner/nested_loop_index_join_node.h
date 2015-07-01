/**
 * @brief Header for nested loop index join plan node.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "backend/common/types.h"
#include "backend/expression/abstract_expression.h"
#include "backend/planner/abstract_join_plan_node.h"

namespace peloton {
namespace planner {

class NestedLoopIndexJoinNode : public AbstractJoinPlanNode {
public:
    NestedLoopIndexJoinNode(const NestedLoopIndexJoinNode &) = delete;
    NestedLoopIndexJoinNode& operator=(const NestedLoopIndexJoinNode &) = delete;
    NestedLoopIndexJoinNode(NestedLoopIndexJoinNode &&) = delete;
    NestedLoopIndexJoinNode& operator=(NestedLoopIndexJoinNode &&) = delete;

    NestedLoopIndexJoinNode(expression::AbstractExpression *predicate) :
        AbstractJoinPlanNode(JOIN_TYPE_INVALID, predicate) { // FIXME
            // Nothing to see here...
    }

    inline PlanNodeType GetPlanNodeType() const {
        return PLAN_NODE_TYPE_NESTLOOPINDEX;
    }

    inline std::string GetInfo() const {
        return "NestedLoopIndexJoin";
    }

private:

    // There is nothing special that we need here

};

} // namespace planner
} // namespace peloton
