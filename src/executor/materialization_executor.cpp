/**
 * @brief Executor for materialization node.
 *
 * TODO Deal with case where we want to materialize fields that are not
 * contained in the logical tile, but correspond to positions that are in
 * the tile.
 *
 * Copyright(c) 2015, CMU
 */

#include "executor/materialization_executor.h"

#include <cassert>

#include "planner/materialization_node.h"

namespace nstore {
namespace executor {

/**
 * @brief Constructor for the materialization executor.
 * @param node Materialization node corresponding to this executor.
 */
MaterializationExecutor::MaterializationExecutor(
    const planner::AbstractPlanNode *node)
  : AbstractExecutor(node) {
}

/**
 * @brief Nothing to init at the moment.
 *
 * @return True on success, false otherwise.
 */
bool MaterializationExecutor::SubInit() {
  assert(children_.size() == 1);
  return true;
}

/**
 * @brief Creates materialized physical tile from logical tile and wraps it
 *        in a new logical tile.
 * 
 * @return Pointer to logical tile containing newly materialized physical tile.
 */
LogicalTile *MaterializationExecutor::SubGetNextTile() {
  assert(children_.size() == 1);
  planner::MaterializationNode &node = GetNode<planner::MaterializationNode>();
  //TODO Implement.
  return nullptr;
}

/** @brief Nothing to clean up at the moment. */
void MaterializationExecutor::SubCleanUp() {}

} // namespace executor
} // namespace nstore
