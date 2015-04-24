/**
 * @brief Executor for Concat node.
 *
 * Copyright(c) 2015, CMU
 */

#include "executor/concat_executor.h"
#include "executor/logical_tile.h"

namespace nstore {
namespace executor {

/** @brief TODO */
ConcatExecutor::ConcatExecutor(const planner::AbstractPlanNode *node)
  : AbstractExecutor(node) {
  (void) node;
}

/** @brief TODO */
bool ConcatExecutor::SubInit() {
  //TODO
  return false;
}

/** @brief TODO */
LogicalTile *ConcatExecutor::SubGetNextTile() {
  //TODO
  return nullptr;
}

} // namespace executor
} // namespace nstore
