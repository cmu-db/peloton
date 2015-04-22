/**
 * @brief Header for logical tile factory.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

namespace nstore {

namespace storage {
class Tile;
}

namespace executor {
class LogicalTile;

class LogicalTileFactory {
 public:
  static LogicalTile *GetTile();

  static LogicalTile *WrapBaseTile(
      storage::Tile *base_tile,
      bool own_base_tile);
};

} // namespace executor
} // namespace nstore
