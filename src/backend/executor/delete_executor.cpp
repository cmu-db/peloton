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

#include "../logging/log_manager.h"
#include "../logging/records/tuple_record.h"
#include "../planner/delete_plan.h"
#include "backend/catalog/manager.h"
#include "backend/common/logger.h"
#include "backend/executor/logical_tile.h"


namespace peloton {
namespace executor {

/**
 * @brief Constructor for delete executor.
 * @param node Delete node corresponding to this executor.
 */
DeleteExecutor::DeleteExecutor(planner::AbstractPlan *node,
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
  LOG_INFO("Delete executor :: 1 child \n");

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

  auto &pos_lists = source_tile.get()->GetPositionLists();
  auto tile_group_id = tile_group->GetTileGroupId();
  auto transaction_ = executor_context_->GetTransaction();

  LOG_INFO("Source tile : %p Tuples : %lu \n", source_tile.get(),
            source_tile->GetTupleCount());

  LOG_INFO("Transaction ID: %lu\n", transaction_->GetTransactionId());

  // Delete each tuple
  for (oid_t visible_tuple_id : *source_tile) {
    oid_t physical_tuple_id = pos_lists[0][visible_tuple_id];

    LOG_INFO("Visible Tuple id : %lu, Physical Tuple id : %lu \n",
              visible_tuple_id, physical_tuple_id);

    peloton::ItemPointer delete_location(tile_group_id, physical_tuple_id);

    // Logging 
    {
      auto& logManager = logging::LogManager::GetInstance();

      if(logManager.IsInLoggingMode()){
        auto logger = logManager.GetBackendLogger();
        auto record = logger->GetTupleRecord(LOGRECORD_TYPE_TUPLE_DELETE,
                                             transaction_->GetTransactionId(), 
                                             target_table_->GetOid(),
                                             INVALID_ITEMPOINTER,
                                             delete_location);

        logger->Log(record);
      }
    }

    // try to delete the tuple
    // this might fail due to a concurrent operation that has latched the tuple
    bool status = target_table_->DeleteTuple(transaction_, delete_location);

    if (status == false) {
      LOG_INFO("Fail to delete. Set txn failure");
      transaction_->SetResult(peloton::Result::RESULT_FAILURE);
      return false;
    }
    executor_context_->num_processed += 1; // deleted one
    transaction_->RecordDelete(delete_location);
  }

  return true;
}

}  // namespace executor
}  // namespace peloton
