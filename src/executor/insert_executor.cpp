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
#include "common/logger.h"
#include "executor/logical_tile.h"
#include "planner/insert_node.h"
#include "storage/tile_iterator.h"

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
  assert(children_.size() == 0 || children_.size() == 1);
  assert(transaction_);

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

    // Check logical tile schema against table schema
    catalog::Schema *schema = target_table->GetSchema();
    std::unique_ptr<catalog::Schema> tile_schema(logical_tile.get()->GetSchema());

    if(*schema != *(tile_schema.get())) {
      LOG_ERROR("Insert executor :: table schema and logical tile schema don't match \n");
      return false;
    }

    // Get the underlying physical tile
    assert(logical_tile.get()->IsWrapper() == true);
    assert(logical_tile.get()->GetWrappedTileCount() == 1);

    storage::Tile *physical_tile = logical_tile.get()->GetWrappedTile(0);
    storage::TileIterator tile_iterator = physical_tile->GetIterator();

    storage::Tuple tuple(physical_tile->GetSchema());

    // Insert given tuples into table
    while (tile_iterator.Next(tuple)) {
      // TODO: Insert tuple
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
        auto& txn_manager = TransactionManager::GetInstance();
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
