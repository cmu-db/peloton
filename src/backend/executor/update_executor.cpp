//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// update_executor.cpp
//
// Identification: src/backend/executor/update_executor.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/executor/update_executor.h"

#include "backend/logging/log_manager.h"
#include "backend/logging/records/tuple_record.h"
#include "backend/planner/update_plan.h"
#include "backend/common/logger.h"
#include "backend/catalog/manager.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/executor_context.h"
#include "backend/expression/container_tuple.h"
#include "backend/concurrency/transaction.h"
#include "backend/concurrency/transaction_manager.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile_group_header.h"
#include "backend/storage/tile.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor for update executor.
 * @param node Update node corresponding to this executor.
 */
UpdateExecutor::UpdateExecutor(const planner::AbstractPlan *node,
                               ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {}

/**
 * @brief Nothing to init at the moment.
 * @return true on success, false otherwise.
 */
bool UpdateExecutor::DInit() {
  assert(children_.size() == 1);
  assert(target_table_ == nullptr);
  assert(project_info_ == nullptr);

  // Grab settings from node
  const planner::UpdatePlan &node = GetPlanNode<planner::UpdatePlan>();
  target_table_ = node.GetTable();
  project_info_ = node.GetProjectInfo();

  assert(target_table_);
  assert(project_info_);

  return true;
}

/**
 * @brief updates a set of columns
 * @return true on success, false otherwise.
 */
bool UpdateExecutor::DExecute() {
  assert(children_.size() == 1);
  assert(executor_context_);

  // We are scanning over a logical tile.
  LOG_INFO("Update executor :: 1 child ");

  if (!children_[0]->Execute()) {
    return false;
  }

  std::unique_ptr<LogicalTile> source_tile(children_[0]->GetOutput());

  auto &pos_lists = source_tile.get()->GetPositionLists();
  storage::Tile *tile = source_tile->GetBaseTile(0);
  storage::TileGroup *tile_group = tile->GetTileGroup();
  storage::TileGroupHeader *tile_group_header = tile_group->GetHeader();
  auto transaction_ = executor_context_->GetTransaction();
  auto tile_group_id = tile_group->GetTileGroupId();

  // Update tuples in given table
  for (oid_t visible_tuple_id : *source_tile) {
    oid_t physical_tuple_id = pos_lists[0][visible_tuple_id];
    LOG_INFO("Visible Tuple id : %lu, Physical Tuple id : %lu ",
             visible_tuple_id, physical_tuple_id);

    txn_id_t tid = transaction_->GetTransactionId();
    //storage::Tuple *new_tuple = nullptr;
    if (tile_group_header->GetTransactionId(physical_tuple_id) == tid){
      // if the thread is the owner of the tuple, then directly update in place.
      storage::Tuple *new_tuple = new storage::Tuple(target_table_->GetSchema(), true);

      // Make a copy of the original tuple and allocate a new tuple
      expression::ContainerTuple<storage::TileGroup> old_tuple(tile_group,
                                                               physical_tuple_id);
      // Execute the projections
      project_info_->Evaluate(new_tuple, &old_tuple, nullptr, executor_context_);
      // TODO: update in place.

      delete new_tuple;

    } else if (tile_group_header->GetTransactionId(physical_tuple_id) == INITIAL_TXN_ID &&
            tile_group_header->GetEndCommitId(physical_tuple_id) == MAX_CID) {

      if (tile_group_header->LatchTupleSlot(physical_tuple_id, tid) == false){
        LOG_INFO("Fail to insert new tuple. Set txn failure.");
        transaction_->SetResult(Result::RESULT_FAILURE);
        return false;
      }

      // if it is the latest version and not locked by other threads, then insert a new version.
      storage::Tuple *new_tuple = new storage::Tuple(target_table_->GetSchema(), true);

      // Make a copy of the original tuple and allocate a new tuple
      expression::ContainerTuple<storage::TileGroup> old_tuple(tile_group,
                                                               physical_tuple_id);
      // Execute the projections
      project_info_->Evaluate(new_tuple, &old_tuple, nullptr, executor_context_);

      // finally insert updated tuple into the table
      ItemPointer location = target_table_->InsertVersion(transaction_, new_tuple);
      tile_group_header->SetPrevItemPointer(physical_tuple_id, location);

      if (location.block == INVALID_OID) {
        delete new_tuple;
        LOG_INFO("Fail to insert new tuple. Set txn failure.");
        transaction_->SetResult(Result::RESULT_FAILURE);
        return false;
      }

      executor_context_->num_processed += 1;  // updated one

      transaction_->RecordWrite(ItemPointer(tile_group_id, physical_tuple_id));
      delete new_tuple;

    } else{
      // transaction should be aborted as we cannot update the latest version.
      LOG_INFO("Fail to update tuple. Set txn failure.");
      transaction_->SetResult(Result::RESULT_FAILURE);
      return false;
    }

    // (A) Try to delete the tuple first
    //auto delete_location = ItemPointer(tile_group_id, physical_tuple_id);
    //bool status = target_table_->DeleteTuple(transaction_, delete_location);
//    if (status == false) {
//      LOG_INFO("Fail to delete old tuple. Set txn failure.");
//      transaction_->SetResult(Result::RESULT_FAILURE);
//      return false;
//    }
//    transaction_->RecordDelete(delete_location);

    //storage::Tuple *new_tuple =
        //new storage::Tuple(target_table_->GetSchema(), true);

    // Execute the projections
    //project_info_->Evaluate(new_tuple, &old_tuple, nullptr, executor_context_);

    // finally insert updated tuple into the table
//    ItemPointer location = target_table_->InsertTuple(transaction_, new_tuple);
//    if (location.block == INVALID_OID) {
//      delete new_tuple;
//      LOG_INFO("Fail to insert new tuple. Set txn failure.");
//      transaction_->SetResult(Result::RESULT_FAILURE);
//      return false;
//    }


    // Logging
//    {
//      auto &log_manager = logging::LogManager::GetInstance();
//
//      if (log_manager.IsInLoggingMode()) {
//        auto logger = log_manager.GetBackendLogger();
//        auto record = logger->GetTupleRecord(
//            LOGRECORD_TYPE_TUPLE_UPDATE, transaction_->GetTransactionId(),
//            target_table_->GetOid(), location, delete_location, new_tuple);
//
//        logger->Log(record);
//      }
//    }

    //delete new_tuple;
  }
  // By default, update should return nothing?
  // SetOutput(source_tile.release());
  return true;
}

}  // namespace executor
}  // namespace peloton
