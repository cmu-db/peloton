/*-------------------------------------------------------------------------
 *
 * update_executor.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 *-------------------------------------------------------------------------
 */

#include "backend/executor/update_executor.h"

#include "backend/common/logger.h"
#include "backend/catalog/manager.h"
#include "backend/executor/logical_tile.h"
#include "backend/planner/update_node.h"
#include "backend/concurrency/transaction.h"
#include "backend/concurrency/transaction_manager.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor for update executor.
 * @param node Update node corresponding to this executor.
 */
UpdateExecutor::UpdateExecutor(planner::AbstractPlanNode *node,
                               ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {
}

/**
 * @brief Nothing to init at the moment.
 * @return true on success, false otherwise.
 */
bool UpdateExecutor::DInit() {
    assert(children_.size() == 1);
    assert(target_table_ == nullptr);

    // Grab settings from node
    const planner::UpdateNode &node = GetPlanNode<planner::UpdateNode>();
    target_table_ = node.GetTable();
    updated_col_exprs_ = node.GetUpdatedColumnExprs();

    assert(target_table_);

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
    LOG_TRACE("Update executor :: 1 child \n");

    if (!children_[0]->Execute()) {
        return false;
    }

    std::unique_ptr<LogicalTile> source_tile(children_[0]->GetOutput());

    auto& pos_lists = source_tile.get()->GetPositionLists();
    storage::Tile *tile = source_tile->GetBaseTile(0);
    storage::TileGroup *tile_group = tile->GetTileGroup();
    auto transaction_ = executor_context_->GetTransaction();

    auto tile_group_id = tile_group->GetTileGroupId();
    auto txn_id = transaction_->GetTransactionId();
    auto& manager = catalog::Manager::GetInstance();

    // Update tuples in given table
    for (oid_t visible_tuple_id : *source_tile) {

      oid_t physical_tuple_id = pos_lists[0][visible_tuple_id];
      LOG_TRACE("Visible Tuple id : %d, Physical Tuple id : %d \n", visible_tuple_id, physical_tuple_id);

      // (A) try to delete the tuple first
      // this might fail due to a concurrent operation that has latched the tuple
      auto delete_location = ItemPointer(tile_group_id, physical_tuple_id);
      bool status = target_table_->DeleteTuple(txn_id, delete_location);
      if(status == false) {
          auto& txn_manager = concurrency::TransactionManager::GetInstance();
          txn_manager.AbortTransaction(transaction_);
          transaction_->SetStatus(Result::RESULT_FAILURE);
          return false;
      }
      transaction_->RecordDelete(delete_location);

      // (B.1) now, make a copy of the original tuple
      storage::Tuple *tuple = tile_group->SelectTuple(physical_tuple_id);

      // (B.2) and update with expressions
      // WARNING: updated values should be calculated based on OLD attribute values
      // So we need a buffer to hold the new values and update the original tuple in a second step.
      std::vector<peloton::Value> updated_expr_values;
      for(auto entry : updated_col_exprs_) {
        updated_expr_values.push_back(entry.second->Evaluate(tuple, nullptr, executor_context_));
      }

      for(size_t i = 0; i < updated_col_exprs_.size(); i++){
        tuple->SetValue(updated_col_exprs_[i].first,
                        updated_expr_values[i]);
      }

      // and finally insert into the table in update mode
      bool update_mode = true;
      ItemPointer location = target_table_->InsertTuple(txn_id, tuple, update_mode);
      if(location.block == INVALID_OID) {
          auto& txn_manager = concurrency::TransactionManager::GetInstance();
          txn_manager.AbortTransaction(transaction_);

          tuple->FreeUninlinedData();
          delete tuple;
          return false;
      }
      transaction_->RecordInsert(location);
      tuple->FreeUninlinedData();
      delete tuple;

      // (C) set back pointer in tile group header of updated tuple
      auto updated_tile_group = static_cast<storage::TileGroup*>(manager.locator[location.block]);
      auto updated_tile_group_header = updated_tile_group->GetHeader();
      updated_tile_group_header->SetPrevItemPointer(location.offset, ItemPointer(tile_group_id, physical_tuple_id));
    }

    // By default, update should return nothing?
    // SetOutput(source_tile.release());
    return true;
}

} // namespace executor
} // namespace peloton
