/*-------------------------------------------------------------------------
 *
 * nested_loop_join.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/executor/nested_loop_join_executor.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "executor/nested_loop_join_executor.h"

#include <vector>

#include "common/types.h"
#include "common/logger.h"
#include "executor/logical_tile_factory.h"
#include "expression/abstract_expression.h"
#include "expression/container_tuple.h"

namespace nstore {
namespace executor {

/**
 * @brief Constructor for nested loop join executor.
 * @param node Nested loop join node corresponding to this executor.
 */
NestedLoopJoinExecutor::NestedLoopJoinExecutor(planner::AbstractPlanNode *node)
: AbstractExecutor(node, nullptr) {
}

/**
 * @brief Do some basic checks and create the schema for the output logical tiles.
 * @return true on success, false otherwise.
 */
bool NestedLoopJoinExecutor::DInit() {
  assert(children_.size() == 2);

  // Grab data from plan node.
  const planner::NestedLoopJoinNode &node = GetNode<planner::NestedLoopJoinNode>();

  // NOTE: predicate can be null for cartesian product
  predicate_ = node.GetPredicate();

  return true;
}

/**
 * @brief Creates logical tiles from the two input logical tiles after applying join predicate.
 * @return true on success, false otherwise.
 */
bool NestedLoopJoinExecutor::DExecute() {

  LOG_TRACE("Nested Loop Join executor :: 2 children \n");

  if (!children_[0]->Execute()) {
    return false;
  }

  std::unique_ptr<LogicalTile> tile(children_[0]->GetOutput());

  if (predicate_ != nullptr) {
    // Invalidate tuples that don't satisfy the predicate.
    for (oid_t tuple_id : *tile) {
      expression::ContainerTuple<LogicalTile> tuple(tile.get(), tuple_id);
      if (predicate_->Evaluate(&tuple, nullptr).IsFalse()) {
        tile->InvalidateTuple(tuple_id);
      }
    }
  }

  SetOutput(tile.release());

  return true;
}

} // namespace executor
} // namespace nstore


