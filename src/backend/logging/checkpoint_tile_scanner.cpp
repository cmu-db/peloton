/*-------------------------------------------------------------------------
 *
 * checkpoint_tile_scanner.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/checkpoint_tile_scanner.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/logging/checkpoint_tile_scanner.h"
#include "backend/storage/tile_group_header.h"
#include "backend/executor/logical_tile_factory.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Checkpoint Tile Scanner
//===--------------------------------------------------------------------===//
std::unique_ptr<executor::LogicalTile> CheckpointTileScanner::Scan(
    std::shared_ptr<storage::TileGroup> tile_group,
    const std::vector<oid_t> &column_ids, cid_t start_cid) {
  // Retrieve next tile group.
  auto tile_group_header = tile_group->GetHeader();

  oid_t active_tuple_count = tile_group->GetNextTupleSlot();

  // Construct position list by looping through tile group
  // and applying the predicate.
  std::vector<oid_t> position_list;
  for (oid_t tuple_id = 0; tuple_id < active_tuple_count; tuple_id++) {
    // check transaction visibility
    if (IsVisible(tile_group_header, tuple_id, start_cid)) {
      position_list.push_back(tuple_id);
    }
  }

  // Construct logical tile.
  std::unique_ptr<executor::LogicalTile> logical_tile(
      executor::LogicalTileFactory::GetTile());
  logical_tile->AddColumns(tile_group, column_ids);
  logical_tile->AddPositionList(std::move(position_list));

  return std::move(logical_tile);
}

// Visibility check
bool CheckpointTileScanner::IsVisible(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id, cid_t start_cid) {
  txn_id_t tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  cid_t tuple_begin_cid = tile_group_header->GetBeginCommitId(tuple_id);
  cid_t tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);
  if (tuple_txn_id == INVALID_TXN_ID) {
    // the tuple is not available.
    return false;
  }

  bool activated = (start_cid >= tuple_begin_cid);
  bool invalidated = (start_cid >= tuple_end_cid);
  if (tuple_txn_id != INITIAL_TXN_ID) {
    // if the tuple is owned by other transactions.
    if (tuple_begin_cid == MAX_CID) {
      // never read an uncommitted version.
      return false;
    } else {
      // the older version may be visible.
      if (activated && !invalidated) {
        return true;
      } else {
        return false;
      }
    }
  } else {
    // if the tuple is not owned by any transaction.
    if (activated && !invalidated) {
      return true;
    } else {
      return false;
    }
  }
}

}  // namespace logging
}  // namespace peloton
