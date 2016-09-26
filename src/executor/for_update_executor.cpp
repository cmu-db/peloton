//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// for_update_executor.cpp
//
// Identification: src/executor/forupdate_executor.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "executor/for_update_executor.h"
#include "planner/update_plan.h"
#include "common/logger.h"
#include "catalog/manager.h"
#include "executor/logical_tile.h"
#include "executor/executor_context.h"
#include "expression/container_tuple.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager_factory.h"
#include "storage/data_table.h"
#include "storage/tile_group_header.h"
#include "storage/tile.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor for update for executor.
 * @param node Update node corresponding to this executor.
 */
ForUpdateExecutor::ForUpdateExecutor(const planner::AbstractPlan *node,
                               ExecutorContext *executor_context)
: AbstractExecutor(node, executor_context) {}

/**
 * @brief Nothing to init at the moment.
 * @return true on success, false otherwise.
 */
bool ForUpdateExecutor::DInit() {
  PL_ASSERT(children_.size() == 1);
  PL_ASSERT(target_table_ == nullptr);
  PL_ASSERT(project_info_ == nullptr);

  // Grab settings from node
  const planner::UpdatePlan &node = GetPlanNode<planner::UpdatePlan>();
  target_table_ = node.GetTable();
  project_info_ = node.GetProjectInfo();

  PL_ASSERT(target_table_);
  PL_ASSERT(project_info_);

  return true;
}

/**
 * @brief locks a set of tuples in tile
 * @return true on success, false otherwise.
 */
bool ForUpdateExecutor::DExecute(LogicalTile *source_tile) {
  PL_ASSERT(children_.size() == 1);
  PL_ASSERT(executor_context_);

  // We are scanning over a logical tile.
  LOG_TRACE("Update executor :: 1 child ");

  if (!children_[0]->Execute()) {
    return false;
  }

  auto &pos_lists = source_tile.get()->GetPositionLists();
  storage::Tile *tile = source_tile->GetBaseTile(0);
  storage::TileGroup *tile_group = tile->GetTileGroup();
  storage::TileGroupHeader *tile_group_header = tile_group->GetHeader();
  auto tile_group_id = tile_group->GetTileGroupId();

  auto &transaction_manager =
      concurrency::TransactionManagerFactory::GetInstance();

  auto current_txn = executor_context_->GetTransaction();


  // Update tuples in a given table
  for (oid_t visible_tuple_id : *source_tile) {
    oid_t physical_tuple_id = pos_lists[0][visible_tuple_id];

    ItemPointer old_location(tile_group_id, physical_tuple_id);

    LOG_TRACE("Visible Tuple id : %u, Physical Tuple id : %u ",
              visible_tuple_id, physical_tuple_id);

    bool is_owner = 
        transaction_manager.IsOwner(current_txn, tile_group_header, physical_tuple_id);
    if (is_owner == true) {
    	// tuple is already locked and we are currently the owner
    	return true;
    } else {

      bool is_ownable = 
          transaction_manager.IsOwnable(current_txn, tile_group_header, physical_tuple_id);

      if (is_ownable == true) {
        // if the tuple is not owned by any transaction and is visible to current transaction.

        bool acquire_ownership_success = 
            transaction_manager.AcquireOwnership(current_txn, tile_group_header, physical_tuple_id);

        if (acquire_ownership_success == false) {
          LOG_TRACE("Fail to insert new tuple. Set txn failure.");
          transaction_manager.SetTransactionResult(current_txn, Result::RESULT_FAILURE);
          return false;
        }
      } else {
      
        // transaction should be aborted as we cannot update the latest version.
        LOG_TRACE("Fail to update tuple. Set txn failure.");
        transaction_manager.SetTransactionResult(current_txn, Result::RESULT_FAILURE);
        return false;
      }
    }
  }
  return true;
}

}  // namespace executor
}  // namespace peloton
