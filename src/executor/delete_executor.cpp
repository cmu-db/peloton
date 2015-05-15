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
#include "executor/logical_tile.h"
#include "planner/delete_node.h"

#include "common/logger.h"

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
  assert(children_.size() <= 1);
  assert(context_);

  const planner::DeleteNode &node = GetNode<planner::DeleteNode>();

  // Delete tuples in logical tile
  if(children_.size() == 1) {
    LOG_TRACE("Delete executor :: 1 child \n");

    // Retrieve next tile.
    const bool success = children_[0]->Execute();
    if (!success) {
      return false;
    }
    std::unique_ptr<LogicalTile> source_tile(children_[0]->GetOutput());

    storage::Tile *base_tile = source_tile->GetBaseTile(0);
    storage::TileGroup *base_tile_group = base_tile->GetTileGroup();
    auto txn_id = context_->GetTransactionId();

    LOG_TRACE("Source tile : %p Tuples : %lu \n", source_tile.get(), source_tile->NumTuples());

    // Delete each tuple
    for (id_t tuple_id : *source_tile) {
      LOG_TRACE("Tuple id : %lu \n", tuple_id);

      // try to delete the tuple
      // this might fail due to a concurrent operation that has latched the tuple
      bool status = base_tile_group->DeleteTuple(txn_id, tuple_id);
      if(status == false) {
        context_->Undo();
        return false;
      }

    }

  }
  // Truncate table
  else if(children_.size() == 0){

    if(node.GetTruncate()){
      // drop the entire table and associated structures like indices
      delete node.GetTable();
    }

  }

  return true;
}

} // namespace executor
} // namespace nstore
