//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// hash_join_executor.cpp
//
// Identification: src/backend/executor/hash_join_executor.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <vector>

#include "backend/common/types.h"
#include "backend/common/logger.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/executor/hash_join_executor.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/container_tuple.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor for hash join executor.
 * @param node Hash join node corresponding to this executor.
 */
HashJoinExecutor::HashJoinExecutor(const planner::AbstractPlan *node,
                                   ExecutorContext *executor_context)
    : AbstractJoinExecutor(node, executor_context) {}

bool HashJoinExecutor::DInit() {
  assert(children_.size() == 2);

  auto status = AbstractJoinExecutor::DInit();
  if (status == false) return status;

  assert(children_[1]->GetRawNode()->GetPlanNodeType() == PLAN_NODE_TYPE_HASH);

  hash_executor_ = reinterpret_cast<HashExecutor *>(children_[1]);

  return true;
}

/**
 * @brief Creates logical tiles from the two input logical tiles after applying
 * join predicate.
 * @return true on success, false otherwise.
 */
bool HashJoinExecutor::DExecute() {
  // Loop until we have non-empty result join logical tile or exit

  // Build outer join output when done

  //===--------------------------------------------------------------------===//
  // Pick right and left tiles
  //===--------------------------------------------------------------------===//

  // Get all the logical tiles from RIGHT child

  // Get next logical tile from LEFT child

  //===--------------------------------------------------------------------===//
  // Build Join Tile
  //===--------------------------------------------------------------------===//

  // Build output join logical tile

  // Build position lists

  // Get the hash table from the hash executor

  // Go over the left logical tile
  // For each tuple, find matching tuples in the hash table built on top of the
  // right table
  // Go over the matching right tuples

  // Check if we have any join tuples

  return false;
}

}  // namespace executor
}  // namespace peloton
