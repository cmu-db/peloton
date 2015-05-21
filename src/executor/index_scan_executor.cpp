/**
 * @brief Executor for index scan node.
 *
 * Copyright(c) 2015, CMU
 */

#include "executor/index_scan_executor.h"

#include <memory>
#include <utility>
#include <vector>

#include "common/types.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "expression/abstract_expression.h"
#include "expression/container_tuple.h"
#include "planner/index_scan_node.h"
#include "storage/table.h"
#include "storage/tile_group.h"

#include "common/logger.h"

namespace nstore {
namespace executor {

/**
 * @brief Constructor for indexscan executor.
 * @param node Indexscan node corresponding to this executor.
 */
IndexScanExecutor::IndexScanExecutor(planner::AbstractPlanNode *node, Transaction *transaction)
: AbstractExecutor(node, transaction) {
}

/**
 * @brief Nothing to init at the moment.
 * @return true on success, false otherwise.
 */
bool IndexScanExecutor::DInit() {
  assert(children_.size() == 0);
  return true;
}

/**
 * @brief Creates logical tile(s) after scanning index.
 * @return true on success, false otherwise.
 */
bool IndexScanExecutor::DExecute() {
  assert(children_.size() == 0);

  // Grab data from plan node.
  const planner::IndexScanNode &node = GetNode<planner::IndexScanNode>();
  const index::Index *index = node.GetIndex();
  const std::vector<oid_t> &column_ids = node.GetColumnIds();

  LOG_TRACE("Index Scan executor :: 0 child \n");

  // We are scanning a table (this is a leaf node in the plan tree).
  assert(index != nullptr);
  assert(column_ids.size() > 0);

  /*
  // Retrieve next tile group.
  if (current_tile_group_id_ == table->GetTileGroupCount()) {
    return false;
  }

  storage::TileGroup *tile_group = table->GetTileGroup(current_tile_group_id_++);
  storage::TileGroupHeader *tile_group_header = tile_group->GetHeader();
  txn_id_t txn_id = transaction_->GetTransactionId();
  cid_t commit_id = transaction_->GetLastCommitId();
  oid_t active_tuple_count = tile_group->GetNextTupleSlot();

  //tile_group_header->PrintVisibility(txn_id, commit_id);

  // Construct position list by looping through tile group
  // and applying the predicate.
  std::vector<oid_t> position_list;
  for (oid_t tuple_id = 0; tuple_id < active_tuple_count ; tuple_id++) {
    if(tile_group_header->IsVisible(tuple_id, txn_id, commit_id) == false){
      continue;
    }

    expression::ContainerTuple<storage::TileGroup> tuple(tile_group, tuple_id);
    if (predicate == nullptr || predicate->Evaluate(&tuple, nullptr).IsTrue()) {
      position_list.push_back(tuple_id);
    }
  }
  */

  // Construct logical tile.
  std::unique_ptr<LogicalTile> logical_tile(LogicalTileFactory::GetTile());
  /*
  const bool own_base_tile = false;
  const int position_list_idx = 0;
  logical_tile->AddPositionList(std::move(position_list));

  for (oid_t origin_column_id : column_ids) {
    oid_t base_tile_offset, tile_column_id;

    tile_group->LocateTileAndColumn(
        origin_column_id,
        base_tile_offset,
        tile_column_id);

    logical_tile->AddColumn(
        tile_group->GetTile(base_tile_offset),
        own_base_tile,
        tile_column_id,
        position_list_idx);
  }
  */

  SetOutput(logical_tile.release());
  return true;
}

} // namespace executor
} // namespace nstore
