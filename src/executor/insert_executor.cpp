/*-------------------------------------------------------------------------
 *
 * insert_executor.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/executor/insert_executor.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "executor/insert_executor.h"

#include "catalog/manager.h"
#include "executor/logical_tile.h"
#include "planner/insert_node.h"

namespace nstore {
namespace executor {

/**
 * @brief Constructor for insert executor.
 * @param node Insert node corresponding to this executor.
 */
InsertExecutor::InsertExecutor(planner::AbstractPlanNode *node,
                               Transaction *transaction)
    : AbstractExecutor(node, transaction) {
}

/**
 * @brief Nothing to init at the moment.
 * @return true on success, false otherwise.
 */
bool InsertExecutor::DInit() {
    assert(children_.size() <= 1);
    return true;
}

/**
 * @brief Adds a column to the logical tile, using the position lists.
 * @return true on success, false otherwise.
 */
bool InsertExecutor::DExecute() {
    assert(children_.size() == 0);
    assert(transaction_);

    const planner::InsertNode &node = GetNode<planner::InsertNode>();
    storage::DataTable *target_table = node.GetTable();
    auto tuples = node.GetTuples();

    // Insert given tuples into table

    for (auto tuple : tuples) {
        ItemPointer location = target_table->InsertTuple(transaction_->GetTransactionId(), tuple);
        if (location.block == INVALID_OID) {
            auto& txn_manager = TransactionManager::GetInstance();
            txn_manager.AbortTransaction(transaction_);
            return false;
        }
        transaction_->RecordInsert(location);
    }

    return true;
}

} // namespace executor
} // namespace nstore
