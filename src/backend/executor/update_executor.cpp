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

namespace nstore {
namespace executor {

/**
 * @brief Constructor for update executor.
 * @param node Update node corresponding to this executor.
 */
UpdateExecutor::UpdateExecutor(planner::AbstractPlanNode *node,
                               concurrency::Transaction *context)
    : AbstractExecutor(node, context) {
}

/**
 * @brief Nothing to init at the moment.
 * @return true on success, false otherwise.
 */
bool UpdateExecutor::DInit() {
    assert(children_.size() <= 1);
    return true;
}

/**
 * @brief updates a set of columns
 * @return true on success, false otherwise.
 */
bool UpdateExecutor::DExecute() {
    assert(children_.size() == 1);
    assert(transaction_);

    // We are scanning over a logical tile.
    LOG_TRACE("Update executor :: 1 child \n");

    if (!children_[0]->Execute()) {
        return false;
    }

    std::unique_ptr<LogicalTile> source_tile(children_[0]->GetOutput());

    storage::Tile *tile = source_tile->GetBaseTile(0);
    storage::TileGroup *tile_group = tile->GetTileGroup();

    auto tile_group_id = tile_group->GetTileGroupId();
    auto txn_id = transaction_->GetTransactionId();
    auto& manager = catalog::Manager::GetInstance();

    const planner::UpdateNode &node = GetNode<planner::UpdateNode>();
    storage::DataTable *target_table = node.GetTable();
    auto updated_cols = node.GetUpdatedColumns();
    auto updated_col_vals = node.GetUpdatedColumnValues();

    // Update tuples in given table
    for (oid_t tuple_id : *source_tile) {

        // (A) try to delete the tuple first
        // this might fail due to a concurrent operation that has latched the tuple
        bool status = tile_group->DeleteTuple(txn_id, tuple_id);
        if(status == false) {
            auto& txn_manager = concurrency::TransactionManager::GetInstance();
            txn_manager.AbortTransaction(transaction_);
            return false;
        }
        transaction_->RecordDelete(ItemPointer(tile_group_id, tuple_id));

        // (B) now, make a copy of the original tuple
        storage::Tuple *tuple = tile_group->SelectTuple(tuple_id);

        // and update the relevant values
        oid_t col_itr = 0;
        for(auto col : updated_cols) {
            Value val = updated_col_vals[col_itr++];
            tuple->SetValue(col, val);
        }

        // and finally insert into the table in update mode
        bool update_mode = true;
        ItemPointer location = target_table->InsertTuple(txn_id, tuple, update_mode);
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
        updated_tile_group_header->SetPrevItemPointer(location.offset, ItemPointer(tile_group_id, tuple_id));
    }

    SetOutput(source_tile.release());
    return true;
}

} // namespace executor
} // namespace nstore
