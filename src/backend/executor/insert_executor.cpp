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

#include "backend/executor/insert_executor.h"

#include "backend/catalog/manager.h"
#include "backend/common/logger.h"
#include "backend/executor/logical_tile.h"
#include "backend/planner/insert_node.h"
#include "backend/storage/tile_iterator.h"

namespace nstore {
namespace executor {

/**
 * @brief Constructor for insert executor.
 * @param node Insert node corresponding to this executor.
 */
InsertExecutor::InsertExecutor(planner::AbstractPlanNode *node,
                               concurrency::Transaction *transaction)
: AbstractExecutor(node, transaction) {
}

/**
 * @brief Nothing to init at the moment.
 * @return true on success, false otherwise.
 */
bool InsertExecutor::DInit() {
  assert(children_.size() == 0 || children_.size() == 1);
  assert(transaction_);

  return true;
}

/**
 * @brief Adds a column to the logical tile, using the position lists.
 * @return true on success, false otherwise.
 */
bool InsertExecutor::DExecute() {

  const planner::InsertNode &node = GetNode<planner::InsertNode>();
  storage::DataTable *target_table = node.GetTable();

  // Inserting a logical tile.
  if (children_.size() == 1) {
    LOG_TRACE("Insert executor :: 1 child \n");

    if (!children_[0]->Execute()) {
      return false;
    }

    std::unique_ptr<LogicalTile> logical_tile(children_[0]->GetOutput());
    assert(logical_tile.get() != nullptr);

    // Get the underlying physical tile from the logical wrapper tile
    assert(logical_tile.get()->IsWrapper() == true);
    storage::Tile *physical_tile = logical_tile.get()->GetWrappedTile();

    // Next, check logical tile schema against table schema
    catalog::Schema *schema = target_table->GetSchema();
    const catalog::Schema *tile_schema = physical_tile->GetSchema();

    if(*schema != *tile_schema) {
      LOG_ERROR("Insert executor :: table schema and logical tile schema don't match \n");
      return false;
    }

    storage::TileIterator tile_iterator = physical_tile->GetIterator();
    storage::Tuple tuple(physical_tile->GetSchema());

    // Finally, Insert given tuples into table
    while (tile_iterator.Next(tuple)) {
      ItemPointer location = target_table->InsertTuple(transaction_->GetTransactionId(), &tuple);
      if (location.block == INVALID_OID) {
        auto& txn_manager = concurrency::TransactionManager::GetInstance();
        txn_manager.AbortTransaction(transaction_);
        return false;
      }
      transaction_->RecordInsert(location);
    }

    return true;
  }
  // Inserting a collection of tuples from plan node
  else if (children_.size() == 0) {

    LOG_TRACE("Insert executor :: 0 child \n");

    // Insert given tuples into table
    auto tuples = node.GetTuples();

    for (auto tuple : tuples) {
      ItemPointer location = target_table->InsertTuple(transaction_->GetTransactionId(), tuple);
      if (location.block == INVALID_OID) {
        auto& txn_manager = concurrency::TransactionManager::GetInstance();
        txn_manager.AbortTransaction(transaction_);
        return false;
      }
      transaction_->RecordInsert(location);
    }

    return true;
  }

  return true;
}

} // namespace executor
} // namespace nstore
