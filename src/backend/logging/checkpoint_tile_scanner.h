/*-------------------------------------------------------------------------
 *
 * checkpoint_tile_scanner.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/checkpoint_tile_scanner.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include <cassert>
#include <string>
#include <memory>

#include "backend/storage/tile.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tuple.h"
#include "backend/executor/logical_tile.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Checkpoint
//===--------------------------------------------------------------------===//
class CheckpointTileScanner {
 public:
  CheckpointTileScanner() {}

  ~CheckpointTileScanner() {}

  //Scan a tile group r.w.t start_cid
  std::unique_ptr<executor::LogicalTile> Scan(
      std::shared_ptr<storage::TileGroup>,
      const std::vector<oid_t> &column_ids, cid_t start_cid);

  // check whether a tuple is visible to current checkpoint.
  bool IsVisible(const storage::TileGroupHeader *const tile_group_header,
                 const oid_t &tuple_id, cid_t start_cid);
};

}  // namespace logging
}  // namespace peloton
