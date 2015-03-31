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
#include "planner/projection_node.h"

namespace nstore {
namespace executor {

//===--------------------------------------------------------------------===//
// Projection Executor
//===--------------------------------------------------------------------===//

/** @brief Constructor for the projection executor. */
ProjectionExecutor::ProjectionExecutor(
    std::unique_ptr<planner::AbstractPlanNode> abstract_node,
    std::vector<AbstractExecutor *>& children)
  : AbstractExecutor(std::move(abstract_node), children) {
  assert(children.size() == 1);
}

/**
 * @brief Convert vector of output column ids provided by plan node into
 *        a set.
 *
 * TODO We might want the node to store the ids as a set instead of
 * having to convert from a vector here. Depends on how we decide to
 * deal with changing of ordering of columns (see header comment).
 * Or perhaps we can get planner to give us ids we *dont* want to output.
 *
 * @return True on success, false otherwise.
 */
bool ProjectionExecutor::SubInit() {
  planner::ProjectionNode *node =
      dynamic_cast<planner::ProjectionNode *>(abstract_node_.get());
  assert(node);

  const std::vector<id_t>& ids = node->output_column_ids();
  assert(ids.size() > 0);

  output_column_ids_.insert(ids.begin(), ids.end());
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

  // Loop through all columns and invalidate the ones that are not in the
  // output set.
  for (id_t id = 0; id < schema->NumCols(); id++) {
    if (schema->IsValid(id) && output_column_ids_.count(id) == 0) {
      schema->InvalidateColumn(id);
    }
  }
  return tile;
}

/** @brief Nothing to clean up. */
void ProjectionExecutor::SubCleanUp() {
}

} // namespace executor
} // namespace nstore
