//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// delete_executor.cpp
//
// Identification: src/executor/delete_executor.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "executor/delete_executor.h"
#include "executor/executor_context.h"

#include "common/value.h"
#include "planner/delete_plan.h"
#include "catalog/manager.h"
#include "common/container_tuple.h"
#include "common/logger.h"
#include "executor/logical_tile.h"
#include "storage/data_table.h"
#include "storage/tile.h"
#include "storage/tile_group.h"
#include "storage/tile_group_header.h"
#include "storage/tuple.h"
#include "concurrency/transaction_manager_factory.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor for delete executor.
 * @param node Delete node corresponding to this executor.
 */
DeleteExecutor::DeleteExecutor(const planner::AbstractPlan *node,
                               ExecutorContext *executor_context)
: AbstractExecutor(node, executor_context) {}

/**
 * @brief Nothing to init at the moment.
 * @return true on success, false otherwise.
 */
bool DeleteExecutor::DInit() {
  PL_ASSERT(children_.size() == 1);
  PL_ASSERT(executor_context_);

  PL_ASSERT(target_table_ == nullptr);

  // Delete tuples in logical tile
  LOG_TRACE("Delete executor :: 1 child ");

  // Grab data from plan node.
  const planner::DeletePlan &node = GetPlanNode<planner::DeletePlan>();
  target_table_ = node.GetTable();
  PL_ASSERT(target_table_);

  return true;
}

/**
 * @brief Delete the table tuples using the position list in the logical tile.
 *
 * If truncate is on, then it will truncate the table itself.
 * @return true on success, false otherwise.
 */
bool DeleteExecutor::DExecute() {
  PL_ASSERT(target_table_);
  // Retrieve next tile.
  if (!children_[0]->Execute()) {
    return false;
  }

  std::unique_ptr<LogicalTile> source_tile(children_[0]->GetOutput());

  auto &pos_lists = source_tile.get()->GetPositionLists();
  storage::Tile *tile = source_tile->GetBaseTile(0);
  storage::TileGroup *tile_group = tile->GetTileGroup();
  storage::TileGroupHeader *tile_group_header = tile_group->GetHeader();
  auto tile_group_id = tile_group->GetTileGroupId();

  auto &transaction_manager =
      concurrency::TransactionManagerFactory::GetInstance();

  auto current_txn = executor_context_->GetTransaction();

  LOG_TRACE("Source tile : %p Tuples : %lu ", source_tile.get(),
            source_tile->GetTupleCount());

  LOG_TRACE("Source tile info: %s", source_tile->GetInfo().c_str());

  LOG_TRACE("Transaction ID: %lu",
            executor_context_->GetTransaction()->GetTransactionId());

  // Delete each tuple
  for (oid_t visible_tuple_id : *source_tile) {
    oid_t physical_tuple_id = pos_lists[0][visible_tuple_id];

    ItemPointer old_location(tile_group_id, physical_tuple_id);

    LOG_TRACE("Visible Tuple id : %u, Physical Tuple id : %u ",
              visible_tuple_id, physical_tuple_id);

    bool is_owner = 
        transaction_manager.IsOwner(current_txn, tile_group_header, physical_tuple_id);
    bool is_written =
      transaction_manager.IsWritten(current_txn, tile_group_header, physical_tuple_id);
    PL_ASSERT((is_owner == false && is_written == true) == false);

    if (is_owner == true && is_written == true) {
      // if the thread is the owner of the tuple, then directly update in place.
      LOG_TRACE("Thread is owner of the tuple");
      transaction_manager.PerformDelete(current_txn, old_location);

    } else {

      bool is_ownable = is_owner ||
          transaction_manager.IsOwnable(current_txn, tile_group_header, physical_tuple_id);

      if (is_ownable == true) {
        // if the tuple is not owned by any transaction and is visible to current
        // transaction.
      	LOG_TRACE("Thread is not the owner of the tuple, but still visible");

        bool acquire_ownership_success = is_owner ||
            transaction_manager.AcquireOwnership(current_txn, tile_group_header, physical_tuple_id);


        if (acquire_ownership_success == false) {
          transaction_manager.SetTransactionResult(current_txn, Result::RESULT_FAILURE);
          return false;
        }
        // if it is the latest version and not locked by other threads, then
        // insert an empty version.
        ItemPointer new_location = target_table_->InsertEmptyVersion();

        // PerformUpdate() will not be executed if the insertion failed.
        // There is a write lock acquired, but since it is not in the write set,
        // because we haven't yet put them into the write set.
        // the acquired lock can't be released when the txn is aborted.
        // the YieldOwnership() function helps us release the acquired write lock.
        if (new_location.IsNull() == true) {
          LOG_TRACE("Fail to insert new tuple. Set txn failure.");
          if (is_owner == false) {
            // If the ownership is acquire inside this update executor, we release it here
            transaction_manager.YieldOwnership(current_txn, tile_group_id, physical_tuple_id);
          }
          transaction_manager.SetTransactionResult(current_txn, Result::RESULT_FAILURE);
          return false;
        }
        transaction_manager.PerformDelete(current_txn, old_location, new_location);

        executor_context_->num_processed += 1;  // deleted one

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
