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
#include "backend/planner/abstract_plan_node.h"

namespace peloton {

namespace storage {
class Tuple;
}

namespace planner {

class NestedLoopJoinNode : public AbstractPlanNode {
 public:
  NestedLoopJoinNode(const NestedLoopJoinNode &) = delete;
  NestedLoopJoinNode& operator=(const NestedLoopJoinNode &) = delete;
  NestedLoopJoinNode(NestedLoopJoinNode &&) = delete;
  NestedLoopJoinNode& operator=(NestedLoopJoinNode &&) = delete;

  NestedLoopJoinNode(expression::AbstractExpression *predicate)
  : predicate_(predicate) {
  }

  const expression::AbstractExpression *GetPredicate() const {
    return predicate_.get();
  }

  inline PlanNodeType GetPlanNodeType() const {
    return PLAN_NODE_TYPE_NESTLOOP;
  }

  inline std::string GetInfo() const {
    return "NestedLoopJoin";
  }

 private:

  /** @brief Join predicate. */
  const std::unique_ptr<expression::AbstractExpression> predicate_;

};

} // namespace planner
} // namespace peloton
