/**
 * @brief Executor for Concat node.
 *
 * Copyright(c) 2015, CMU
 */

#include "executor/concat_executor.h"

#include <memory>

#include "catalog/manager.h"
#include "executor/logical_tile.h"
#include "planner/concat_node.h"

namespace nstore {

namespace storage {
class Tile;
}

namespace executor {

/**
 * @brief Constructor for concat executor.
 * @param node Concat node corresponding to this executor.
 */
ConcatExecutor::ConcatExecutor(planner::AbstractPlanNode *node)
: AbstractExecutor(node) {
}

/**
 * @brief Nothing to init at the moment.
 * @return true on success, false otherwise.
 */
bool ConcatExecutor::SubInit() {
  assert(children_.size() == 1);
  return true;
}

/**
 * @brief Adds a column to the logical tile, using the position lists.
 * @return true on success, false otherwise.
 */
bool ConcatExecutor::SubExecute() {
  assert(children_.size() == 1);

  // Retrieve next tile.
  const bool success = children_[0]->Execute();
  if (!success) {
    return false;
  }

  // Grab data from plan node.
  std::unique_ptr<LogicalTile> source_tile(children_[0]->GetOutput());
  planner::ConcatNode &node = GetNode<planner::ConcatNode>();
  const std::vector<planner::ConcatNode::ColumnPointer> &new_columns =
      node.new_columns();

  // Add new columns.
  const bool own_base_tile = false;
  for (unsigned int i = 0; i < new_columns.size(); i++) {
    const planner::ConcatNode::ColumnPointer &col = new_columns[i];
    auto &locator = catalog::Manager::GetInstance().locator;
    auto it = locator.find(col.base_tile_oid);
    assert(it != locator.end());
    source_tile->AddColumn(
        static_cast<storage::Tile *>(it->second),
        own_base_tile,
        col.origin_column_id,
        col.position_list_idx);
  }

  SetOutput(source_tile.release());
  return true;
}

} // namespace executor
} // namespace nstore
