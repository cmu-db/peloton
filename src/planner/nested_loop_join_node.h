/**
 * @brief Header for nested loop join plan node.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/types.h"
#include "expression/abstract_expression.h"
#include "planner/abstract_plan_node.h"

namespace nstore {

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
    return PLAN_NODE_TYPE_INDEXSCAN;
  }

  inline std::string GetInfo() const {
    return "IndexScan";
  }

 private:

  /** @brief Join predicate. */
  const std::unique_ptr<expression::AbstractExpression> predicate_;

};

} // namespace planner
} // namespace nstore
