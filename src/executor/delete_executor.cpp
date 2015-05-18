/*-------------------------------------------------------------------------
 *
 * delete_executor.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/executor/delete_executor.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "executor/delete_executor.h"

#include "catalog/manager.h"
#include "common/context.h"
#include "common/logger.h"
#include "executor/logical_tile.h"
#include "planner/delete_node.h"

namespace nstore {
namespace executor {

/**
 * @brief Constructor for delete executor.
 * @param node Delete node corresponding to this executor.
 */
DeleteExecutor::DeleteExecutor(planner::AbstractPlanNode *node,
                               Context *context)
: AbstractExecutor(node, context){
}

/**
 * @brief Nothing to init at the moment.
 * @return true on success, false otherwise.
 */
bool DeleteExecutor::DInit() {
  assert(children_.size() <= 1);
  return true;
}

/**
 * @brief Delete the table tuples using the position list in the logical tile.
 *
 * If truncate is on, then it will truncate the table itself.
 * @return true on success, false otherwise.
 */
bool DeleteExecutor::DExecute() {
  assert(children_.size() == 1);
  assert(context_);

  const planner::DeleteNode &node = GetNode<planner::DeleteNode>();

  // Delete tuples in logical tile
  LOG_TRACE("Delete executor :: 1 child \n");

  // Retrieve next tile.
  const bool success = children_[0]->Execute();
  if (!success) {
    return false;
  }
  std::unique_ptr<LogicalTile> source_tile(children_[0]->GetOutput());

  storage::Tile *tile = source_tile->GetBaseTile(0);
  storage::TileGroup *tile_group = tile->GetTileGroup();
  auto tile_group_id = tile_group->GetTileGroupId();
  auto txn_id = context_->GetTransactionId();

  LOG_TRACE("Source tile : %p Tuples : %lu \n", source_tile.get(), source_tile->NumTuples());

  // Delete each tuple
  for (oid_t tuple_id : *source_tile) {
    LOG_TRACE("Tuple id : %lu \n", tuple_id);

    // try to delete the tuple
    // this might fail due to a concurrent operation that has latched the tuple
    bool status = tile_group->DeleteTuple(txn_id, tuple_id);
    if(status == false) {
      context_->Abort();
      return false;
    }
    context_->RecordDelete(ItemPointer(tile_group_id, tuple_id));
  }

  return true;
}

} // namespace executor
} // namespace nstore
