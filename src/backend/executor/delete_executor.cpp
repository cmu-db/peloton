//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// delete_executor.cpp
//
// Identification: src/backend/executor/delete_executor.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/executor/delete_executor.h"
#include "backend/executor/executor_context.h"

#include "backend/common/value.h"
#include "backend/logging/log_manager.h"
#include "backend/logging/records/tuple_record.h"
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
  LOG_INFO("Delete executor :: 1 child ");

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
  const bool success = children_[0]->Execute();
  if (!success) {
    return false;
  }

  std::unique_ptr<LogicalTile> source_tile(children_[0]->GetOutput());

  storage::Tile *tile = source_tile->GetBaseTile(0);
  storage::TileGroup *tile_group = tile->GetTileGroup();
  storage::TileGroupHeader *tile_group_header = tile_group->GetHeader();

  auto &pos_lists = source_tile.get()->GetPositionLists();
  auto tile_group_id = tile_group->GetTileGroupId();
  auto transaction_ = executor_context_->GetTransaction();

  LOG_INFO("Source tile : %p Tuples : %lu ", source_tile.get(),
           source_tile->GetTupleCount());

  LOG_INFO("Transaction ID: %lu", transaction_->GetTransactionId());

  // Delete each tuple
  for (oid_t visible_tuple_id : *source_tile) {
    oid_t physical_tuple_id = pos_lists[0][visible_tuple_id];

    LOG_INFO("Visible Tuple id : %lu, Physical Tuple id : %lu ",
             visible_tuple_id, physical_tuple_id);

    txn_id_t tid = transaction_->GetTransactionId();
    if (tile_group_header->GetTransactionId(physical_tuple_id) == tid) {
      // if the thread is the owner of the tuple, then directly update in place.
      tile_group_header->SetEndCommitId(physical_tuple_id, INVALID_CID);

      // TODO: Logging
      // {
      //   auto &log_manager = logging::LogManager::GetInstance();

      //   if (log_manager.IsInLoggingMode()) {
      //     auto logger = log_manager.GetBackendLogger();
      //     auto record = logger->GetTupleRecord(
      //       LOGRECORD_TYPE_TUPLE_DELETE, transaction_->GetTransactionId(),
      //       target_table_->GetOid(), INVALID_ITEMPOINTER, delete_location);

      //     logger->Log(record);
      //   }
      // }

    } else if (tile_group_header->GetTransactionId(physical_tuple_id) ==
                   INITIAL_TXN_ID &&
               tile_group_header->GetEndCommitId(physical_tuple_id) ==
                   MAX_CID) {
      // if the tuple is not owned by any transaction and is visible to current
      // transdaction.

      if (tile_group_header->LockTupleSlot(physical_tuple_id, tid) == false) {
        LOG_INFO("Fail to insert new tuple. Set txn failure.");
        transaction_->SetResult(Result::RESULT_FAILURE);
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
      ItemPointer location =
          target_table_->InsertVersion(new_tuple, false);

      tile_group_header->SetNextItemPointer(physical_tuple_id, location);
      auto new_tile_group_header =
          target_table_->GetTileGroupById(location.block)->GetHeader();
      new_tile_group_header->SetEndCommitId(location.offset, INVALID_CID);

      if (location.block == INVALID_OID) {
        delete new_tuple;
        LOG_INFO("Fail to insert new tuple. Set txn failure.");
        transaction_->SetResult(Result::RESULT_FAILURE);
        return false;
      }

      executor_context_->num_processed += 1;  // deleted one

      ItemPointer delete_location(tile_group_id, physical_tuple_id);
      transaction_->RecordDelete(tile_group_id, physical_tuple_id);

      // Logging
      // {
      //   auto &log_manager = logging::LogManager::GetInstance();

      //   if (log_manager.IsInLoggingMode()) {
      //     auto logger = log_manager.GetBackendLogger();
      //     auto record = logger->GetTupleRecord(
      //         LOGRECORD_TYPE_TUPLE_DELETE, transaction_->GetTransactionId(),
      //         target_table_->GetOid(), INVALID_ITEMPOINTER, delete_location);

      //     logger->Log(record);
      //   }
      // }
      delete new_tuple;

    } else {
      // transaction should be aborted as we cannot update the latest version.
      LOG_INFO("Fail to update tuple. Set txn failure.");
      transaction_->SetResult(Result::RESULT_FAILURE);
      return false;
    }
  }

  return true;
}

}  // namespace executor
}  // namespace peloton
