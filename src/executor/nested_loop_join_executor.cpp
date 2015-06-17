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

  shorter_table_itr = START_OID;
  result_itr = START_OID;

  return true;
}

/**
 * @brief Creates logical tiles from the two input logical tiles after applying join predicate.
 * @return true on success, false otherwise.
 */
bool NestedLoopJoinExecutor::DExecute() {

  LOG_TRACE("Nested Loop Join executor :: 2 children \n");

  // Already performed the join ?
  if(done) {
    if(result_itr == result.size()) {
      return false;
    }
    else {
      // Return appropriate tile and go to next tile
      SetOutput(result[result_itr]);
      result_itr++;
      return true;
    }
  }

  // Try to get next tile from LEFT child
  if (children_[0]->Execute() == false) {
    return false;
  }

  // Try to get next tile from RIGHT child
  if (children_[1]->Execute() == false) {

    // Check if reinit helps
    children_[1]->Init();

    if (children_[1]->Execute() == false) {
      return false;
    }
  }

  std::unique_ptr<LogicalTile> tile0(children_[0]->GetOutput());
  std::unique_ptr<LogicalTile> tile1(children_[1]->GetOutput());

  // Check the logical tiles.
  assert(tile0.get() != nullptr);
  assert(tile1.get() != nullptr);

  if (predicate_ != nullptr) {

    /*
    // Invalidate tuples that don't satisfy the predicate.
    for (oid_t tuple_id : *tile) {
      expression::ContainerTuple<LogicalTile> tuple(tile.get(), tuple_id);
      if (predicate_->Evaluate(&tuple, nullptr).IsFalse()) {
        tile->InvalidateTuple(tuple_id);
      }
    }
    */

  }


  done = true;

  LOG_TRACE("Result tiles : %lu \n", result.size());

  SetOutput(result[result_itr]);
  result_itr++;

  return true;
}

} // namespace executor
} // namespace nstore


