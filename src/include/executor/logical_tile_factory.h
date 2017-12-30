//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// logical_tile_factory.h
//
// Identification: src/include/executor/logical_tile_factory.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "common/internal_types.h"

namespace peloton {

namespace storage {
class Tile;
class TileGroup;
class AbstractTable;
}

//===--------------------------------------------------------------------===//
// Logical Tile Factory
//===--------------------------------------------------------------------===//

namespace executor {
class LogicalTile;

class LogicalTileFactory {
 public:
  static LogicalTile *GetTile();

  static LogicalTile *WrapTiles(
      const std::vector<std::shared_ptr<storage::Tile>> &base_tile_refs);

  static LogicalTile *WrapTileGroup(
      const std::shared_ptr<storage::TileGroup> &tile_group);
};

}  // namespace executor
}  // namespace peloton
