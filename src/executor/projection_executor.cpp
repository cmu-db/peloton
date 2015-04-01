/**
 * @brief Executor for projection node.
 *
 * Note that this executor does not materialize any changes.
 * TODO For now, we don't allow changing the ordering of columns. That will be
 * deferred to the materialization node. The planner has to be aware of this.
 *
 * Copyright(c) 2015, CMU
 */

#include "executor/projection_executor.h"

#include <cassert>

#include <unordered_set>

#include "executor/logical_schema.h"
#include "executor/logical_tile.h"
#include "planner/projection_node.h"

namespace nstore {
namespace executor {

/**
 * @brief Constructor for the projection executor.
 * @param node Projection node corresponding to this executor.
 */
ProjectionExecutor::ProjectionExecutor(const planner::ProjectionNode *node)
  : node_(node) {}

/**
 * @brief Nothing to init at the moment.
 *
 * @return True on success, false otherwise.
 */
bool ProjectionExecutor::SubInit() {
  assert(children_.size() == 1);
  return true;
}

/**
 * @brief Goes through each column and invalidates it if not present
 *        in output set.
 *
 * @return Pointer to logical tile after projection.
 */
LogicalTile *ProjectionExecutor::SubGetNextTile() {
  assert(children_.size() == 1);
  LogicalTile *tile = children_[0]->GetNextTile();
  LogicalSchema *schema = tile->schema();

  const std::unordered_set<id_t> &output_column_ids =
    node_->output_column_ids();

  // Loop through all columns and invalidate the ones that are not in the
  // output set.
  for (id_t id = 0; id < schema->NumCols(); id++) {
    if (schema->IsValid(id) && output_column_ids.count(id) == 0) {
      schema->InvalidateColumn(id);
    }
  }
  return tile;
}

/** @brief Nothing to clean up at the moment. */
void ProjectionExecutor::SubCleanUp() {}

} // namespace executor
} // namespace nstore
