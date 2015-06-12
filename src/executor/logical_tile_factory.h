/**
 * @brief Header for logical tile factory.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include <vector>

#include "common/types.h"

namespace nstore {

namespace storage {
class Tile;
class TileGroup;
class Table;
}

namespace executor {
class LogicalTile;

class LogicalTileFactory {
 public:
  static LogicalTile *GetTile();

  static LogicalTile *WrapBaseTiles(
      const std::vector<storage::Tile *> &base_tile,
      bool own_base_tiles);

  static LogicalTile *WrapTileGroup(storage::TileGroup *tile_group);

  static std::vector<LogicalTile *> WrapTupleLocations(const storage::Table *table,
                                                       const std::vector<ItemPointer> tuple_locations,
                                                       const std::vector<oid_t> column_ids);

};

} // namespace executor
} // namespace nstore
