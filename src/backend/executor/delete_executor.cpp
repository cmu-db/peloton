//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// delete_executor.cpp
//
// Identification: src/backend/executor/delete_executor.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/executor/delete_executor.h"
#include "backend/executor/executor_context.h"

#include "backend/common/value.h"
#include "backend/planner/delete_plan.h"
#include "backend/catalog/manager.h"
#include "backend/expression/container_tuple.h"
#include "backend/common/logger.h"
#include "backend/executor/logical_tile.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tile_group_header.h"
#include "backend/storage/tuple.h"
#include "backend/concurrency/transaction_manager_factory.h"

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
  assert(children_.size() == 1);
  assert(executor_context_);

  assert(target_table_ == nullptr);

  // Delete tuples in logical tile
  LOG_TRACE("Delete executor :: 1 child ");

  // Grab data from plan node.
  const planner::DeletePlan &node = GetPlanNode<planner::DeletePlan>();
  target_table_ = node.GetTable();
  assert(target_table_);

  return true;
}

/**
 * @brief Delete the table tuples using the position list in the logical tile.
 *
 * If truncate is on, then it will truncate the table itself.
 * @return true on success, false otherwise.
 */
bool DeleteExecutor::DExecute() {
  assert(target_table_);

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

  LOG_TRACE("Source tile : %p Tuples : %lu ", source_tile.get(),
            source_tile->GetTupleCount());

  LOG_TRACE("Transaction ID: %lu",
            executor_context_->GetTransaction()->GetTransactionId());

  // Delete each tuple
  for (oid_t visible_tuple_id : *source_tile) {
    oid_t physical_tuple_id = pos_lists[0][visible_tuple_id];

    LOG_TRACE("Visible Tuple id : %lu, Physical Tuple id : %lu ",
              visible_tuple_id, physical_tuple_id);

    if (transaction_manager.IsOwner(tile_group_header, physical_tuple_id) ==
        true) {
      // if the thread is the owner of the tuple, then directly update in place.

      transaction_manager.PerformDelete(tile_group_id, physical_tuple_id);

    } else if (transaction_manager.IsOwnable(tile_group_header,
                                             physical_tuple_id) == true) {
      // if the tuple is not owned by any transaction and is visible to current
      // transaction.

      if (transaction_manager.AcquireOwnership(tile_group_header, tile_group_id,
                                               physical_tuple_id) == false) {
        transaction_manager.SetTransactionResult(RESULT_FAILURE);
        return false;
      }
      // if it is the latest version and not locked by other threads, then
      // insert a new version.
      storage::Tuple *new_tuple =
          new storage::Tuple(target_table_->GetSchema(), true);

      // Make a copy of the original tuple and allocate a new tuple
      expression::ContainerTuple<storage::TileGroup> old_tuple(
          tile_group, physical_tuple_id);

      // finally insert updated tuple into the table
      ItemPointer location = target_table_->InsertEmptyVersion(new_tuple);

      if (location.block == INVALID_OID) {
        delete new_tuple;
        new_tuple = nullptr;
        LOG_TRACE("Fail to insert new tuple. Set txn failure.");
        transaction_manager.SetTransactionResult(Result::RESULT_FAILURE);
        return false;
      }

      auto res = transaction_manager.PerformDelete(tile_group_id,
                                                   physical_tuple_id, location);
      if (!res) {
        transaction_manager.SetTransactionResult(RESULT_FAILURE);
        return res;
      }

      executor_context_->num_processed += 1;  // deleted one

      delete new_tuple;
      new_tuple = nullptr;
    } else {
      // transaction should be aborted as we cannot update the latest version.
      LOG_TRACE("Fail to update tuple. Set txn failure.");
      transaction_manager.SetTransactionResult(Result::RESULT_FAILURE);
      return false;
    }
  }

  return true;
}

}  // namespace executor
}  // namespace peloton
